package common

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/datastruct"
	"github.com/slack-go/slack"
)

var (
	slackChannel  = os.Getenv("SLACK_CHANNEL_ID") // Slack Channel Id
	slackAPITOKEN = os.Getenv("SLACK_API_TOKEN")  // Slack API token
	api           = slack.New(slackAPITOKEN)      // Slack api client
)

// formatWalletAddress
// Takes wallet address and reformat it to new shape
func formatWalletAddress(wallet string) string {

	var start = ""
	var end = ""
	if wallet != "" {
		start = wallet[:2]
		end = wallet[len(wallet)-6:]
		return fmt.Sprintf("%s...%s", start, end)
	}
	return ""
}

// formatTable
// Takes tableData and build the table that will send to slack
func formatTable(tableData [][]string) string {
	var returnString strings.Builder

	// Calculate column widths
	columnWidths := make([]int, len(tableData[0]))
	for _, row := range tableData {
		for colIdx, cell := range row {
			if len(cell) > columnWidths[colIdx] {
				columnWidths[colIdx] = len(cell)
			}
		}
	}

	// Construct the table
	for i, row := range tableData {
		// Construct header line
		if i == 1 {
			returnString.WriteString("|")
			for _, width := range columnWidths {
				returnString.WriteString(strings.Repeat("-", width+2) + "|")
			}
			returnString.WriteString("\n")
		}

		// Construct data rows
		returnString.WriteString("|")
		for colIdx, cell := range row {
			padding := columnWidths[colIdx] - len(cell)
			returnString.WriteString(" " + cell + strings.Repeat(" ", padding) + " |")
		}
		returnString.WriteString("\n")
	}

	return returnString.String()
}

// SendSlack
// Takes Transaction data and color
// It will send the Transaction to slack channel
func SendSlack(record datastruct.EthereumTransactionRow, color string) {
	var (
		transactionURL    = ""
		blockchainScanner = ""
	)
	switch record.Id {
	case "ethereum":
		transactionURL = datastruct.EthereumTXURL
		blockchainScanner = datastruct.EthereumScan
	}

	tableData := [][]string{
		{"From", "To", "USDValue", "FromWallet", "ToWallet"},
		{formatWalletAddress(record.FromAddress), formatWalletAddress(record.ToAddress), fmt.Sprintf("%.2f", record.ValueUsd), formatWalletAddress(record.FromAddress), formatWalletAddress(record.ToAddress)},
	}

	table := formatTable(tableData)

	attachment := slack.Attachment{

		Color: color,
		Title: "Price Movement",
		Fields: []slack.AttachmentField{
			{
				Title: "Transaction Hash Id",
				Value: record.TransactionHash,
			},
			{
				Title: "Overview",
				Value: fmt.Sprintf("```%s```", table),
			},
		},
		Actions: []slack.AttachmentAction{
			{
				Name: "View Transaction",
				Text: fmt.Sprintf("view on %s", blockchainScanner),
				Type: "button",
				URL:  fmt.Sprintf("%s%s", transactionURL, record.TransactionHash),
			},
		},
	}
	channelID, timestamp, err := api.PostMessage(slackChannel, slack.MsgOptionAttachments(attachment))
	if err != nil {
		log.Error(fmt.Sprintf("ERROR posting to slack: %s", err))
		time.Sleep(time.Second)
	} else {
		log.Info(fmt.Sprintf("Message successfully sent to channel %s at %s \n", channelID, timestamp))
	}
}

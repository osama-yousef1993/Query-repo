package store

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
)

type Chain struct {
	ID          string `json:"id"`
	Description string `json:"description"`
}

type Data struct {
	ID          string   `json:"id"`
	Description string   `json:"description"`
	Urls        []string `json:"urls"`
	Questions   []Question
}

type Question struct {
	Question string `json:"question"`
	Answer   string `json:"answer"`
}

func readFile(filename string) ([]Chain, error) {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var data []Chain
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func extractUrls(description string) []string {
	var urls []string
	re := regexp.MustCompile(`href="(https://[^"]+)"`)
	matches := re.FindAllStringSubmatch(description, -1)
	for _, match := range matches {
		if len(match) > 1 {
			urls = append(urls, match[1])
		}
	}
	return urls
}

func processSubList(sub []string, pattern *regexp.Regexp) []Question {
	var subList []Question
	for _, ques := range sub {
		fullText := strings.SplitN(ques, "</h3>", 2)
		question := fullText[0]
		answer := fullText[1]
		answer = pattern.ReplaceAllString(answer, "")
		subList = append(subList, Question{
			Question: question,
			Answer:   strings.TrimSpace(answer),
		})
	}
	return subList
}

func buildNftsQuestions(data []Chain) []Data {
	var faq []Data
	var idWithNoDescription []string
	var idWithDescription []string

	h3Pattern := regexp.MustCompile(`[\r\n]|<p dir="ltr">|</p>`)
	h3AltPattern := regexp.MustCompile(`[\r\n]|<p>|</p>`)

	for _, chain := range data {
		description := chain.Description
		if strings.Contains(description, `<h3 dir="ltr">`) {
			idWithDescription = append(idWithDescription, chain.ID)
			sub := strings.Split(description, `<h3 dir="ltr">`)
			subList := processSubList(sub[1:], h3Pattern)
			faq = append(faq, Data{
				ID:          chain.ID,
				Description: chain.Description,
				Urls:        extractUrls(chain.Description),
				Questions:   subList,
			})
		} else if strings.Contains(description, "<h3>") {
			idWithDescription = append(idWithDescription, chain.ID)
			sub := strings.Split(description, "<h3>")
			subList := processSubList(sub[1:], h3AltPattern)
			faq = append(faq, Data{
				ID:          chain.ID,
				Description: chain.Description,
				Urls:        extractUrls(chain.Description),
				Questions:   subList,
			})
		} else {
			idWithNoDescription = append(idWithNoDescription, chain.ID)
			faq = append(faq, Data{
				ID:          chain.ID,
				Description: chain.Description,
				Urls:        extractUrls(chain.Description),
				Questions:   []Question{},
			})
		}
	}
	file, _ := json.MarshalIndent(faq, "",  " ")
	_ = os.WriteFile("FAQ.json", file, 0644)
	return faq
}

func MainProcess() []Data {
	data, _ := readFile("nfts.json")
	res := buildNftsQuestions(data)

	return res
}

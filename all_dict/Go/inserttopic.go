v1.HandleFunc("/topics", InsertTopics).Methods(http.MethodGet, http.MethodOptions)

func InsertTopics(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)

	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "InsertTopics")
	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "InsertTopics"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Insert Topics Data")

	store.SaveNewsTopic(ctx)

	log.EndTimeL(labels, "InsertTopics ", startTime, nil)
	span.SetStatus(codes.Ok, "InsertTopics")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}


type Item struct {
	Categories                 string `json:"Categories"`
	SuggestedMetaTitleTemplate string `json:"Suggested Meta Title Template"`
	SuggestedMetaDescription   string `json:"Suggested Meta Description"`
	SuggestedOnPageSummaryDesc string `json:"Suggested On-Page Summary Description"`
	SuggestedH1                string `json:"Suggested H1"`
	News                       string `json:"news/"`
	SuggestedURL               string `json:"Suggested URL "`
}

func SaveNewsTopic(ctx context.Context) {

	fs := GetFirestoreClient()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	jsonData := `
	[
    {
        "Categories": "A16Z",
        "Suggested Meta Title Template": "Latest A16Z News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest A16Z news. Dive into all the updates and expert insights to stay informed on any changes in the cryptocurrency market. ",
        "Suggested On-Page Summary Description": "Uncover the latest A16Z news. Dive into the newest trend updates, analysis, news, and crypto market updates below. ",
        "Suggested H1": "A16Z News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Algorand",
        "Suggested Meta Title Template": "Latest Algorand (ALGO) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Algorand news, analysis, and trends to stay ahead of the game. Find the latest ALGO articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Algorand with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Algorand News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "ApeCoin",
        "Suggested Meta Title Template": "Latest ApeCoin (APE) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest ApeCoin news, analysis, and trends to stay ahead of the game. Find the latest APE articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of ApeCoin with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "ApeCoin News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Aptos",
        "Suggested Meta Title Template": "Latest Aptos (APT) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Aptos news, analysis, and trends to stay ahead of the game. Find the latest APT articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Aptos with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Aptos News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Artificial Intelligence",
        "Suggested Meta Title Template": "Latest Artificial Intelligence News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest AI news. Dive into all the updates and expert insights to stay informed on any changes in the world of artificial intelligence. ",
        "Suggested On-Page Summary Description": "Keep up with the latest on Artificial Intelligence news. Dive into the latest updates to stay informed, maximize opportunities, and drive success in the new AI landscape.",
        "Suggested H1": "Artificial Intelligence News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Avalanche",
        "Suggested Meta Title Template": "Latest Avalanche (AVAX) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Avalanche news, analysis, and trends to stay ahead of the game. Find the latest AVAX articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Avalanche with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Avalanche News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "BNB",
        "Suggested Meta Title Template": "Latest BNB News Crypto | Forbes Digital Assets",
        "Suggested Meta Description": "Stay ahead in the crypto world with cutting-edge BNB crypto news. Come and discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested On-Page Summary Description": "Stay ahead in the crypto world with cutting-edge BNB crypto news. Look below to discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested H1": "BNB Crypto News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Binance",
        "Suggested Meta Title Template": "Latest Binance News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our Binance news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Stay informed on Binance. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Binance News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Binance USD",
        "Suggested Meta Title Template": "Latest Binance USD News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Binance USD news, analysis, and trends to stay ahead of the game. Find the latest BNB articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Binance USD with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Binance USD News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Bitcoin",
        "Suggested Meta Title Template": "Latest Bitcoin (BTC) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Bitcoin news, analysis, and trends to stay ahead of the game. Find the latest BTC articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Bitcoin with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Bitcoin News",
        "news/": "news/",
        "Suggested URL ": "/digital-assets/assets/bitcoin-btc/news/"
    },
    {
        "Categories": "BlockFi",
        "Suggested Meta Title Template": "Latest BlockFi News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our BlockFi news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Keep up with the newest on BlockFi. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "BlockFi News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Blockstream",
        "Suggested Meta Title Template": "Latest Blockstream News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed with the latest Blockstream news. Discover real-time updates, news, analysis, and insights on our Blockstream news page.",
        "Suggested On-Page Summary Description": "Discover the latest on Blockstream. Keep up with the most recent news, innovations, and other updates to stay informed for crypto success.",
        "Suggested H1": "Blockstream News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Cardano",
        "Suggested Meta Title Template": "Latest Cardano USD (ADA) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Cardano USD news, analysis, and trends to stay ahead of the game. Find the latest ADA articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Cardano with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Cardano News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Chainlink",
        "Suggested Meta Title Template": "Latest Chainlink (LINK) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Chainlink news, analysis, and trends to stay ahead of the game. Find the latest LINK articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Chainlink with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Chainlink News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Circle",
        "Suggested Meta Title Template": "Latest Circle News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our Circle news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Keep up with the newest on Circle. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Circle News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Coinbase",
        "Suggested Meta Title Template": "Latest Coinbase News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our Coinbase news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Keep up with the newest on Coinbase. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Coinbase News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Cosmos",
        "Suggested Meta Title Template": "Latest Cosmos (ATOM) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Cosmos news, analysis, and trends to stay ahead of the game. Find the latest ATOM articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Cosmos with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Cosmos News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Dai",
        "Suggested Meta Title Template": "Latest Dai (DAI) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Dai news, analysis, and trends to stay ahead of the game. Find the latest DAI articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Dai with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Dai News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Digital Currency Group",
        "Suggested Meta Title Template": "Latest Digital Currency Group News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our Digital Currency Group news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Keep up with the newest on Digital Currency Group. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Digital Currency Group News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Dogecoin",
        "Suggested Meta Title Template": "Latest Dogecoin USD (DOGE) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Dogecoin news, analysis, and trends to stay ahead of the game. Find the latest DOGE articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Dogecoin with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Dogecoin News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Dragonfly",
        "Suggested Meta Title Template": "Latest Dragonfly News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest Dragonfly news. Dive into all the updates and expert insights to stay informed on any changes in the cryptocurrency market. ",
        "Suggested On-Page Summary Description": "Uncover the latest Dragonfly news. Dive into the newest trend updates, analysis, news, and crypto market updates below. ",
        "Suggested H1": "Dragonfly News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Ethereum",
        "Suggested Meta Title Template": "Latest Ethereum (ETH) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Ethereum news, analysis, and trends to stay ahead of the game. Find the latest ETH articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Ethereum with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Ethereum News",
        "news/": "news/",
        "Suggested URL ": "/digital-assets/assets/ethereum-eth/news/"
    },
    {
        "Categories": "FTX",
        "Suggested Meta Title Template": "Latest FTX News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our FTX news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Keep up with the newest on FTX. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "FTX News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Filecoin",
        "Suggested Meta Title Template": "Latest Filecoin (FIL) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Filecoin news, analysis, and trends to stay ahead of the game. Find the latest FIL articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Filecoin with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Filecoin News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Gaming",
        "Suggested Meta Title Template": "Latest Crypto Gaming News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest crypto gaming news. Dive into all the updates and expert insights to stay informed on any changes in the world of crypto gaming. ",
        "Suggested On-Page Summary Description": "Unlock the world of Crypto Gaming. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Gaming News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Gemini",
        "Suggested Meta Title Template": "Latest Gemini News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our Gemini news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Keep up with the newest on Gemini. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Gemini News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Goldman Sachs",
        "Suggested Meta Title Template": "Latest Goldman Sachs Crypto News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay ahead in the crypto world with cutting-edge Goldman Sachs crypto news. Discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested On-Page Summary Description": "Stay ahead in the crypto world with cutting-edge Goldman Sachs crypto news. Look below to discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested H1": "Goldman Sachs Crypto News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "HSBC",
        "Suggested Meta Title Template": "Latest HSBC Crypto News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay ahead in the crypto world with cutting-edge HSBC crypto news. Come and discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested On-Page Summary Description": "Stay ahead in the crypto world with cutting-edge HSBC crypto news. Look below to discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested H1": "HSBC Crypto News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Hedera",
        "Suggested Meta Title Template": "Latest Hedera (HBAR) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Hedera news, analysis, and trends to stay ahead of the game. Find the latest HBAR articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Hedera with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Hedera News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "IBM",
        "Suggested Meta Title Template": "Latest IBM Crypto News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay ahead in the crypto world with cutting-edge IBM crypto news. Come and discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested On-Page Summary Description": "Stay ahead in the crypto world with cutting-edge IBM crypto news. Look below to discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested H1": "IBM Crypto News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "IMF",
        "Suggested Meta Title Template": "Latest IMF News Crypto | Forbes Digital Assets",
        "Suggested Meta Description": "Stay ahead in the crypto world with cutting-edge IMF crypto news. Come and discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested On-Page Summary Description": "Stay ahead in the crypto world with cutting-edge IMF crypto news. Look below to discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested H1": "IMF Crypto News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "JPMorgan",
        "Suggested Meta Title Template": "Latest JPMorgan Crypto News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay ahead in the crypto world with cutting-edge JPMorgan crypto news. Come and discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested On-Page Summary Description": "Stay ahead in the crypto world with cutting-edge JPMorgan crypto news. Look below to discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested H1": "JPMorgan Crypto News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Kraken",
        "Suggested Meta Title Template": "Latest Kraken News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our Kraken news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Keep up with the newest on Kraken. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Kraken News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Litecoin",
        "Suggested Meta Title Template": "Latest Litecoin (LTC) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Litecoin news, analysis, and trends to stay ahead of the game. Find the latest LTC articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Litecoin with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Litecoin News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Marathon",
        "Suggested Meta Title Template": "Latest Marathon News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed with the latest Marathon news. Discover real-time updates, news, analysis, and insights on our Marathon news page.",
        "Suggested On-Page Summary Description": "Discover the latest on Marathon. Keep up with the most recent news, innovations, and other updates to stay informed for crypto success.",
        "Suggested H1": "Marathon News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Microsoft",
        "Suggested Meta Title Template": "Latest Microsoft Crypto News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay ahead in the crypto world with cutting-edge Microsoft crypto news. Come and discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested On-Page Summary Description": "Stay ahead in the crypto world with cutting-edge Microsoft crypto news. Look below to discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested H1": "Microsoft Crypto News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Miners",
        "Suggested Meta Title Template": "Latest Crypto Mining News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest Crypto Miner news. Dive into all the updates and expert insights to stay informed on any changes in the world of crypto mining. ",
        "Suggested On-Page Summary Description": "Unlock the world of Crypto Mining. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Miners News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Multicoin",
        "Suggested Meta Title Template": "Latest Multicoin News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest Multicoin news. Dive into all the updates and expert insights to stay informed on any changes in the cryptocurrency market. ",
        "Suggested On-Page Summary Description": "Uncover the latest Multicoin news. Dive into the newest trend updates, analysis, news, and crypto market updates below. ",
        "Suggested H1": "Multicoin News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "NFT",
        "Suggested Meta Title Template": "Latest NFT News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest NFT news. Dive into all the updates and expert insights to stay informed on any changes in the world of NFTs. ",
        "Suggested On-Page Summary Description": "Unleash the power of NFTs. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the NFT world.",
        "Suggested H1": "NFT News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "OKB",
        "Suggested Meta Title Template": "Latest OKB News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest OKB news, analysis, and trends to stay ahead of the game. Find the latest OKB articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of OKB with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "OKB News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Optimism",
        "Suggested Meta Title Template": "Latest Optimism (OP) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Optimism news, analysis, and trends to stay ahead of the game. Find the latest OP articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Optimism with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Optimism News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Pantera",
        "Suggested Meta Title Template": "Latest Pantera News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest Pantera news. Dive into all the updates and expert insights to stay informed on any changes in the cryptocurrency market. ",
        "Suggested On-Page Summary Description": "Uncover the latest Pantera news. Dive into the newest trend updates, analysis, news, and crypto market updates below. ",
        "Suggested H1": "Pantera News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "PayPal",
        "Suggested Meta Title Template": "Latest PayPal News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our PayPal news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Keep up with the newest on PayPal. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "PayPal News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Polkadot",
        "Suggested Meta Title Template": "Latest Polkadot (DOT) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Polkadot news, analysis, and trends to stay ahead of the game. Find the latest DOT articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Polkadot with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Polkadot News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Polychain",
        "Suggested Meta Title Template": "Latest Polychain News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest Polychain news. Dive into all the updates and expert insights to stay informed on any changes in the cryptocurrency market. ",
        "Suggested On-Page Summary Description": "Uncover the latest Polychain news. Dive into the newest trend updates, analysis, news, and crypto market updates below. ",
        "Suggested H1": "Polychain News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Polygon",
        "Suggested Meta Title Template": "Latest Polygon (MATIC) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Polygon news, analysis, and trends to stay ahead of the game. Find the latest MATIC articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Polygon with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Polygon News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Regulation",
        "Suggested Meta Title Template": "Latest Crypto Regulation News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest cryptocurrency regulation news. Dive into all the updates and expert insights to stay informed on any changes in crypto regulation. ",
        "Suggested On-Page Summary Description": "Keep up with the latest crypto regulation. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Regulation News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Riot",
        "Suggested Meta Title Template": "Latest Riot News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed with the latest Riot news. Discover real-time updates, news, analysis, and insights on our Riot news page.",
        "Suggested On-Page Summary Description": "Discover the latest on Riot. Keep up with the most recent news, innovations, and other updates to stay informed for crypto success.",
        "Suggested H1": "Riot News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Robinhood",
        "Suggested Meta Title Template": "Latest Robinhood News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our Robinhood news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Keep up with the newest on Robinhood. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Robinhood News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Santander",
        "Suggested Meta Title Template": "Latest Santander Crypto News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay ahead in the crypto world with cutting-edge Santander crypto news. Come and discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested On-Page Summary Description": "Stay ahead in the crypto world with cutting-edge Santander crypto news. Look below to discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested H1": "Santander Crypto News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Sequoia",
        "Suggested Meta Title Template": "Latest Sequoia News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest Sequoia news. Dive into all the updates and expert insights to stay informed on any changes in the cryptocurrency market. ",
        "Suggested On-Page Summary Description": "Uncover the latest Sequoia news. Dive into the newest trend updates, analysis, news, and crypto market updates below. ",
        "Suggested H1": "Sequoia News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Seven Seven Six",
        "Suggested Meta Title Template": "Latest Seven Seven Six News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest Seven Seven Six news. Dive into all the updates and expert insights to stay informed on any changes in the cryptocurrency market. ",
        "Suggested On-Page Summary Description": "Uncover the latest Seven Seven Six news. Dive into the newest trend updates, analysis, news, and crypto market updates below. ",
        "Suggested H1": "Seven Seven Six News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Shiba Inu",
        "Suggested Meta Title Template": "Latest Shiba Inu (SHIB) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Shiba Inu news, analysis, and trends to stay ahead of the game. Find the latest SHIB articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Shiba Inu with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Shiba Inu News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Solana",
        "Suggested Meta Title Template": "Latest Solana (SOL) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Solana news, analysis, and trends to stay ahead of the game. Find the latest SOL articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Solana with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Solana News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Stablecoins",
        "Suggested Meta Title Template": "Latest Stablecoin News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay ahead in the crypto world with cutting-edge Stablecoin news. Come and discover game-changing insights for your cryptocurrency investing strategies.",
        "Suggested On-Page Summary Description": "Unleash the power of Stablecoins. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Stablecoins News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Stronghold Digital",
        "Suggested Meta Title Template": "Latest Stronghold Digital News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed with the latest Stronghold Digital news. Discover real-time updates, news, analysis, and insights on our Stronghold Digital news page.",
        "Suggested On-Page Summary Description": "Discover the latest on Stronghold Digital. Keep up with the most recent news, innovations, and other updates to stay informed for crypto success.",
        "Suggested H1": "Stronghold Digital News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "TRON",
        "Suggested Meta Title Template": "Latest TRON (TRX) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest TRON news, analysis, and trends to stay ahead of the game. Find the latest TRX articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of TRON with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "TRON News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Tether",
        "Suggested Meta Title Template": "Latest Tether News | Forbes Digital Assets",
        "Suggested Meta Description": "Stay informed on crypto news. Our Tether news portal brings you the latest updates, analysis, and opinions for the savviest crypto investors.",
        "Suggested On-Page Summary Description": "Keep up with the newest on Tether. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the crypto world.",
        "Suggested H1": "Tether News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Tezos",
        "Suggested Meta Title Template": "Latest Tezos (XTZ) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Tezos news, analysis, and trends to stay ahead of the game. Find the latest XTZ articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Tezos with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Tezos News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "TrueUSD",
        "Suggested Meta Title Template": "Latest True USD (TUSD) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest TrueUSD news, analysis, and trends to stay ahead of the game. Find the latest TUSD articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of TrueUSD with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "TrueUSD News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "USDC",
        "Suggested Meta Title Template": "Latest USCoin (USDC) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest USCoin news, analysis, and trends to stay ahead of the game. Find the latest USDC articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of USCoin with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "USDC News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "USDT",
        "Suggested Meta Title Template": "Latest Tether USD (USDT) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Tether USD news, analysis, and trends to stay ahead of the game. Find the latest USDT articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Tether USD with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "USDT News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Union Square Ventures",
        "Suggested Meta Title Template": "Latest Union Square News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest Union Square news. Dive into all the updates and expert insights to stay informed on any changes in the cryptocurrency market. ",
        "Suggested On-Page Summary Description": "Uncover the latest Union Square news. Dive into the newest trend updates, analysis, news, and crypto market updates below. ",
        "Suggested H1": "Union Square News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Uniswap",
        "Suggested Meta Title Template": "Latest Uniswap (UNI) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Uniswap news, analysis, and trends to stay ahead of the game. Find the latest UNI articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Uniswap with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "Uniswap News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "VeChain",
        "Suggested Meta Title Template": "Latest VeChain (VET) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest VeChain news, analysis, and trends to stay ahead of the game. Find the latest VET articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of VeChain with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "VeChain News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "Web3",
        "Suggested Meta Title Template": "Latest Web3 News | Forbes Digital Assets",
        "Suggested Meta Description": "Keep up with the latest Web3 news. Dive into all the updates and expert insights to stay informed on any advances in the world of Web3. ",
        "Suggested On-Page Summary Description": "Uncover everything Web3. Dive into the latest news and updates to stay informed, maximize opportunities, and drive success in the world of Web3.",
        "Suggested H1": "Web3 News",
        "news/": "news/",
        "Suggested URL ": "news/"
    },
    {
        "Categories": "XRP",
        "Suggested Meta Title Template": "Latest Ripple USD (XRP) News | Forbes Digital Assets",
        "Suggested Meta Description": "Discover the latest Ripple USD news, analysis, and trends to stay ahead of the game. Find the latest XRP articles and important investing insights today.",
        "Suggested On-Page Summary Description": "Unlock the world of Ripple with our latest news hub. Dive into the latest trends, crypto market updates, and analysis and news articles below.  ",
        "Suggested H1": "XRP News",
        "news/": "news/",
        "Suggested URL ": "news/"
    }
]`

	var items []Item

	// Unmarshal the JSON data into the slice
	err := json.Unmarshal([]byte(jsonData), &items)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return
	}
	for index, item := range items {
		fs.Collection(collectionName).Doc(item.Categories).Set(ctx, map[string]interface{}{
			"topicOrder":           index + 1,
			"topicPageDescription": item.SuggestedOnPageSummaryDesc,
		}, firestore.MergeAll)
	}

}






// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// firestore.go
type FundamentalsTopic struct {
	Symbol string `json:"symbol" firestore:"symbol" postgres:"symbol"`
	Name   string `json:"name" firestore:"name" postgres:"name"`
	Slug   string `json:"slug" firestore:"slug" postgres:"slug"`
}
// Add topics with all its data to FS
func SaveNewsTopics(ctx0 context.Context, topics []services.Topic) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveNewsTopics")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	for _, topic := range topics {
		slug := topic.Slug
		isAssets := false
		fund, err := CheckTopicAssets(ctx, topic.TopicName)
		if err != nil {
			isAssets = false
		}
		if fund.Symbol != "" {
			isAssets = true
			slug = fund.Slug
		}
		fs.Collection(collectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"topicName":          topic.TopicName,
			"bertieTag":          topic.BertieTag,
			"topicUrl":           topic.TopicURl,
			"topicOrder":         topic.TopicOrder,
			"description":        topic.Description,
			"isTrending":         topic.IsTrending,
			"titleTemplate":      topic.TitleTemplate,
			"slug":               slug,
			"topicPageDescription": topic.TopicPageDescription,
			"newsHeader":         topic.NewsHeader,
			"isAssets":           isAssets,
		}, firestore.MergeAll)
		for _, article := range topic.Articles {
			doc := make(map[string]interface{})
			doc["id"] = article.Id
			doc["title"] = article.Title
			doc["image"] = article.Image
			doc["articleURL"] = article.ArticleURL
			doc["author"] = article.Author
			doc["type"] = article.Type
			doc["authorType"] = article.AuthorType
			doc["authorLink"] = article.AuthorLink
			doc["description"] = article.Description
			doc["publishDate"] = article.PublishDate
			doc["disabled"] = article.Disabled
			doc["seniorContributor"] = article.SeniorContributor
			doc["bylineFormat"] = article.BylineFormat
			doc["bertieTag"] = article.BertieTag
			doc["order"] = article.Order
			doc["isFeaturedArticle"] = article.IsFeaturedArticle
			doc["lastUpdated"] = article.LastUpdated
			if article.DocId != "" {
				fs.Collection(collectionName).Doc(topic.TopicName).Collection("articles").Doc(article.DocId).Set(ctx, doc, firestore.MergeAll)
			} else {
				fs.Collection(collectionName).Doc(topic.TopicName).Collection("articles").NewDoc().Set(ctx, doc, firestore.MergeAll)
			}
		}
	}
	span.SetStatus(otelCodes.Ok, "Success")

}


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// post.go

func CheckTopicAssets(ctxO context.Context, name string) (*FundamentalsData, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "CheckTopicAssets", trace.WithAttributes(attribute.String("name", name)))
	defer span.End()

	startTime := StartTime("Check Topic Assets Query")

	pg := PGConnect()
	query := `
	SELECT 
		symbol,
		name,
		slug,
	FROM 
		public.fundamentalslatest
	where 
		name = '` + name + `'
		 `
	var fundamentals FundamentalsData

	queryResult, err := pg.QueryContext(ctx, query)
	span.AddEvent("Query Executed")

	if err != nil {

		ConsumeTime("Check Topic Assets Query", startTime, err)
		span.SetStatus(codes.Error, "unable to get data for name from PG")
		return nil, err

	}

	for queryResult.Next() {
		err := queryResult.Scan(&fundamentals.Symbol, &fundamentals.Name, &fundamentals.Slug)
		if err != nil {
			ConsumeTime("Check Topic Assets Query", startTime, err)
			return nil, err
		}
	}
	ConsumeTime("Check Topic Assets Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")

	return &fundamentals, nil
}


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// post.go
func CheckTopicAssets(ctxO context.Context, name string) (*FundamentalsData, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "CheckTopicAssets", trace.WithAttributes(attribute.String("name", name)))
	defer span.End()

	startTime := StartTime("Check Topic Assets Query")

	pg := PGConnect()
	query := `
	SELECT 
		symbol,
		name,
		slug,
	FROM 
		public.fundamentalslatest
	where 
		name = '` + name + `'
		 `
	var fundamentals FundamentalsData

	queryResult, err := pg.QueryContext(ctx, query)
	span.AddEvent("Query Executed")

	if err != nil {

		ConsumeTime("Check Topic Assets Query", startTime, err)
		span.SetStatus(codes.Error, "unable to get data for name from PG")
		return nil, err

	}

	for queryResult.Next() {
		err := queryResult.Scan(&fundamentals.Symbol, &fundamentals.Name, &fundamentals.Slug)
		if err != nil {
			ConsumeTime("Check Topic Assets Query", startTime, err)
			return nil, err
		}
	}
	ConsumeTime("Check Topic Assets Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")

	return &fundamentals, nil
}

func CheckAllTopicAssets(ctxO context.Context) (map[string][]FundamentalsTopic, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "CheckTopicAssets")
	defer span.End()

	startTime := StartTime("Check Topic Assets Query")

	pg := PGConnect()

	topics := []string{"a16z", "algorand", "apecoin", "aptos", "artificial intelligence", "avalanche", "bnb", "binance",
		"binance usd", "bitcoin", "blockfi", "blockstream", "cardano", "chainlink", "circle", "coinbase",
		"cosmos", "cosmos hub", "dai", "digital currency group", "dogecoin", "dragonfly", "ethereum", "ftx",
		"filecoin", "gaming", "gemini", "goldman sachs", "hsbc", "hedera", "ibm", "imf", "jpmorgan", "kraken",
		"litecoin", "marathon", "microsoft", "miners", "multicoin", "nft", "okb", "optimism", "pantera", "paypal",
		"polkadot", "polychain", "polygon", "regulation", "riot", "robinhood", "santander", "sequoia", "seven seven six",
		"shiba inu", "solana", "stablecoins", "stronghold digital", "tron", "tether", "tezos", "trueusd", "usdc", "usdt",
		"union square ventures", "uniswap", "vechain", "web3", "xrp"}

	fundamentals := make(map[string][]FundamentalsTopic)
	for _, topic := range topics {

		query := `
			SELECT 
				symbol,
				name,
				slug
			FROM 
				public.fundamentalslatest
			where 
				name = '` + strings.Title(topic) + `'
				or 
				symbol like '` + strings.ToLower(topic) + `%'
				 `

		queryResult, err := pg.QueryContext(ctx, query)
		span.AddEvent("Query Executed")

		if err != nil {

			ConsumeTime("Check Topic Assets Query", startTime, err)
			span.SetStatus(codes.Error, "unable to get data for name from PG")
			return nil, err

		}

		for queryResult.Next() {
			var fundamental FundamentalsTopic
			err := queryResult.Scan(&fundamental.Symbol, &fundamental.Name, &fundamental.Slug)
			if err != nil {
				ConsumeTime("Check Topic Assets Query", startTime, err)
				return nil, err
			}
			fundamentals[topic] = append(fundamentals[topic], fundamental)
		}

	}
	ConsumeTime("Check Topic Assets Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")

	file, _ := json.MarshalIndent(fundamentals, " ", "")
	_ = os.WriteFile("fundamentalsTopic.json", file, 0644)

	return fundamentals, nil
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// main.go 3092
_, _ = store.CheckAllTopicAssets(ctx)
//topic
package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/go-tools/log"
	otelCodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type Topic struct {
	TopicName            string             `json:"topicName" firestore:"topicName"`                       // Topic Name
	BertieTag            string             `json:"bertieTag" firestore:"bertieTag"`                       // Bertie Tag we will use it to fetch all articles related to the topic
	Description          string             `json:"description" firestore:"description"`                   // topic Description
	IsTrending           bool               `json:"isTrending" firestore:"isTrending"`                     // Trending Tag for topic
	Slug                 string             `json:"slug" firestore:"slug"`                                 // topic Slug
	TopicURl             string             `json:"topicUrl" firestore:"topicUrl"`                         // topic url
	TopicOrder           int                `json:"topicOrder" firestore:"topicOrder"`                     // topic order we will use it for updating the trending topic for 24 hour
	TitleTemplate        string             `json:"titleTemplate" firestore:"titleTemplate"`               // topic title
	TopicPageDescription string             `json:"topicPageDescription" firestore:"topicPageDescription"` // topic summary description
	NewsHeader           string             `json:"newsHeader" firestore:"newsHeader"`                     // topic header
	Articles             []EducationArticle `json:"articles" firestore:"articles"`                         // topic articles
}

// get topics from config Rowy table
// get all Articles from BQ depends on Bertie Tag
// Map Articles to each Topic
// return all Topics and it's Articles to by saved on FS Table
func BuildTopics(ctx context.Context) ([]Topic, error) {
	fs := GetFirestoreClient()
	ctxO, span := tracer.Start(ctx, "BuildTopics")
	defer span.End()
	topicCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	// Get All Topics from FS
	dbSnap := fs.Collection(topicCollection).Documents(ctxO)
	span.AddEvent("Start Get Topics Data from FS")

	var topics []Topic

	var bertieTag []string
	for {
		var topic Topic
		var articles []EducationArticle
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error Getting Topics Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Topics Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		// Get All Articles the related to each Topic from FS
		db := fs.Collection(topicCollection).Doc(doc.Ref.ID).Collection("articles").Documents(ctxO)

		for {
			var article EducationArticle
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&article); err != nil {
				log.Error("Error Getting Article Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}
			article.DocId = doc.Ref.ID
			if article.UpdatedAt != nil {
				article.LastUpdated = article.UpdatedAt["timestamp"].(time.Time)
			}

			articles = append(articles, article)
		}

		bertieTag = append(bertieTag, topic.BertieTag)

		topic.Articles = articles
		topics = append(topics, topic)

	}

	// get All new Articles from BQ using Bertie tag for Topics
	articles, err := GetEducationContentFromBertie(bertieTag, ctxO, "mv_content_latest")

	if err != nil {
		log.Error("Error Getting Articles from Bertie BQ: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Getting Articles from Bertie BQ: %s", err))
		return nil, err
	}
	// Map the new Articles to Topics
	newsTopics, err := MapArticlesToTopic(ctxO, topics, articles)
	if err != nil {
		log.Error("Error Map Articles to Sections: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Map Articles to Sections: %s", err))
		return nil, err
	}
	return newsTopics, nil

}

// map Articles to each Topic by Bertie Tag
func MapArticlesToTopic(ctxO context.Context, topics []Topic, articles []EducationArticle) ([]Topic, error) {
	_, span := tracer.Start(ctxO, "MapArticlesToTopic")
	defer span.End()

	span.AddEvent("Start Map Articles to each topic")
	var newsTopics []Topic

	for _, topic := range topics {
		var topicArticles []EducationArticle
		for _, article := range articles {
			if topic.BertieTag == article.BertieTag {
				for _, sectionArticle := range topic.Articles {
					// if article exist in topic map the new value article to it
					if sectionArticle.Title == article.Title {
						article.DocId = sectionArticle.DocId
						article.Order = sectionArticle.Order
						article.LastUpdated = sectionArticle.LastUpdated
						article.IsFeaturedArticle = sectionArticle.IsFeaturedArticle
						goto ADDArticles
					}
				}
			ADDArticles:
				topicArticles = append(topicArticles, article)
			}
		}
		SortArticles(topicArticles, true)
		topic.Articles = topicArticles
		newsTopics = append(newsTopics, topic)
	}
	span.SetStatus(otelCodes.Ok, "Success")
	return newsTopics, nil
}

// get Topic with it's connected articles from FS using slug
func GetNewsTopic(ctx0 context.Context, slug string) (*Topic, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNewsTopics")
	defer span.End()

	sectionCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	// get topic data using slug
	dbSnap := fs.Collection(sectionCollection).Where("slug", "==", slug).Documents(ctx)

	span.AddEvent("Start Get News Topics Data from FS")

	var topic Topic
	for {
		var articles []EducationArticle
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error Getting News Topics Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting News Topics Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		// get topic articles
		db := fs.Collection(sectionCollection).Doc(topic.TopicName).Collection("articles").OrderBy("order", firestore.Asc).Documents(ctx)

		for {
			var article EducationArticle
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&article); err != nil {
				log.Error("Error Getting Article Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}

			articles = append(articles, article)
		}
		topic.Articles = articles

	}

	span.AddEvent("Modify Articles to be only 8 Articles for each Topic")
	span.SetStatus(otelCodes.Ok, "Success")
	return &topic, nil
}

// build Topics data from BQ and config table in Rowy
func GetNewsTopicData(ctx0 context.Context, slug string) ([]byte, error) {
	ctx, span := tracer.Start(ctx0, "GetNewsTopicsData")
	defer span.End()
	span.AddEvent("Start Get News Topics Data")
	// get the topic with all it's articles using slug
	topic, err := GetNewsTopic(ctx, slug)

	if err != nil {
		log.Error("Error Getting News Topics from FS:  %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting News Topics from FS: %s", err))
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	result, err := json.Marshal(topic)
	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return result, nil

}

// Update trending topic for the day
func UpdateTrendingTopics(ctx0 context.Context) ([]Topic, []Topic) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "UpdateTrendingTopics")
	defer span.End()
	span.AddEvent("Start Update Trending Topics")

	newsCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	var (
		trendingTopics    []Topic
		notTrendingTopics []Topic
		dbSnap            *firestore.DocumentIterator
	)
	// get all topic the trending and not trending ones
	dbSnap = fs.Collection(newsCollection).Documents(ctx)

	span.AddEvent("Start Get Topics Data from FS")
	for {
		var topic Topic
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error Getting Topics Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Topics Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
		}

		if topic.IsTrending {
			trendingTopics = append(trendingTopics, topic)
		} else {
			notTrendingTopics = append(notTrendingTopics, topic)
		}
	}
	lastTopic := trendingTopics[len(trendingTopics)-1]
	order := lastTopic.TopicOrder
	// build the new trending topics
	topicResult := BuildTrendingTopicArray(ctx0, trendingTopics, notTrendingTopics, order)
	span.SetStatus(otelCodes.Ok, "Success")
	return topicResult, trendingTopics
}

// build new trending topics
func BuildTrendingTopicArray(ctx context.Context, trendingTopics []Topic, notTrendingTopics []Topic, topicIndex int) []Topic {
	_, span := tracer.Start(ctx, "BuildTrendingTopicArray")
	defer span.End()
	span.AddEvent("Start Build Trending Topic Array")

	var topicResult []Topic
	trendingTopicCount := 20
	trendingTopicsLen := len(trendingTopics)
	notTrendingTopicsLen := len(notTrendingTopics)
	totalIndex := (trendingTopicsLen + notTrendingTopicsLen)
	res := totalIndex - topicIndex

	// if the result for topic equals to 20 then return the topic with in the range
	// if it's not equals to 20 we need to get the last part from topics and append the rest of them to reach 20 topics
	if res >= 20 {
		topicResult = append(topicResult, notTrendingTopics[topicIndex-trendingTopicCount:topicIndex]...)
	} else {
		topicResult = append(topicResult, notTrendingTopics[topicIndex-trendingTopicCount:totalIndex-trendingTopicCount]...)
		if len(topicResult) < trendingTopicCount {
			t := trendingTopicCount - len(topicResult)
			topicResult = append(topicResult, notTrendingTopics[0:t]...)
		}
	}
	// second way 
	// if res >= 20 {
	// 	topicResult = append(topicResult, notTrendingTopics[topicIndex-trendingTopicCount:topicIndex]...)
	// } else {
	// 	initIndex := topicIndex - trendingTopicCount
	// 	if initIndex > notTrendingTopicsLen {
	// 		topicResult = append(topicResult, notTrendingTopics[0:totalIndex-trendingTopicCount]...)
	// 	} else {
	// 		topicResult = append(topicResult, notTrendingTopics[topicIndex-trendingTopicCount:totalIndex-trendingTopicCount]...)
	// 	}
	// 	if len(topicResult) < trendingTopicCount {
	// 		t := trendingTopicCount - len(topicResult)
	// 		topicResult = append(topicResult, trendingTopics[0:t]...)
	// 	}
	// }
	span.SetStatus(otelCodes.Ok, "Success")
	return topicResult
}

// firestore
type FundamentalsTopic struct {
	Symbol string `json:"symbol" firestore:"symbol" postgres:"symbol"`
	Name   string `json:"name" firestore:"name" postgres:"name"`
	Slug   string `json:"slug" firestore:"slug" postgres:"slug"`
}

func SaveNewsTopics(ctx0 context.Context, topics []services.Topic) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveNewsTopics")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	for _, topic := range topics {
		slug := topic.Slug
		isAssets := false
		fund, err := CheckTopicAssets(ctx, topic.TopicName)
		if err != nil {
			isAssets = false
		}
		if fund.Symbol != "" {
			isAssets = true
			slug = fund.Slug
		}
		fs.Collection(collectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"topicName":            topic.TopicName,
			"bertieTag":            topic.BertieTag,
			"topicUrl":             topic.TopicURl,
			"topicOrder":           topic.TopicOrder,
			"description":          topic.Description,
			"isTrending":           topic.IsTrending,
			"titleTemplate":        topic.TitleTemplate,
			"slug":                 slug,
			"topicPageDescription": topic.TopicPageDescription,
			"newsHeader":           topic.NewsHeader,
			"isAssets":             isAssets,
		}, firestore.MergeAll)
		for _, article := range topic.Articles {
			doc := make(map[string]interface{})
			doc["id"] = article.Id
			doc["title"] = article.Title
			doc["image"] = article.Image
			doc["articleURL"] = article.ArticleURL
			doc["author"] = article.Author
			doc["type"] = article.Type
			doc["authorType"] = article.AuthorType
			doc["authorLink"] = article.AuthorLink
			doc["description"] = article.Description
			doc["publishDate"] = article.PublishDate
			doc["disabled"] = article.Disabled
			doc["seniorContributor"] = article.SeniorContributor
			doc["bylineFormat"] = article.BylineFormat
			doc["bertieTag"] = article.BertieTag
			doc["order"] = article.Order
			doc["isFeaturedArticle"] = article.IsFeaturedArticle
			doc["lastUpdated"] = article.LastUpdated
			if article.DocId != "" {
				fs.Collection(collectionName).Doc(topic.TopicName).Collection("articles").Doc(article.DocId).Set(ctx, doc, firestore.MergeAll)
			} else {
				fs.Collection(collectionName).Doc(topic.TopicName).Collection("articles").NewDoc().Set(ctx, doc, firestore.MergeAll)
			}
		}
	}
	span.SetStatus(otelCodes.Ok, "Success")

}
// postgresql 
func CheckTopicAssets(ctxO context.Context, name string) (*FundamentalsData, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "CheckTopicAssets", trace.WithAttributes(attribute.String("name", name)))
	defer span.End()

	startTime := StartTime("Check Topic Assets Query")

	pg := PGConnect()
	query := `
	SELECT 
		symbol,
		name,
		slug,
	FROM 
		public.fundamentalslatest
	where 
		name = '` + name + `'
		 `
	var fundamentals FundamentalsData

	queryResult, err := pg.QueryContext(ctx, query)
	span.AddEvent("Query Executed")

	if err != nil {

		ConsumeTime("Check Topic Assets Query", startTime, err)
		span.SetStatus(codes.Error, "unable to get data for name from PG")
		return nil, err

	}

	for queryResult.Next() {
		err := queryResult.Scan(&fundamentals.Symbol, &fundamentals.Name, &fundamentals.Slug)
		if err != nil {
			ConsumeTime("Check Topic Assets Query", startTime, err)
			return nil, err
		}
	}
	ConsumeTime("Check Topic Assets Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")

	return &fundamentals, nil
}

type ResultTopic struct {
	Topics       map[string][]FundamentalsTopic `json:"Topics"`
	ResultLength int                            `json:"resultLength"`
}

func CheckAllTopicAssets(ctxO context.Context) ([]ResultTopic, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "CheckTopicAssets")
	defer span.End()

	startTime := StartTime("Check Topic Assets Query")

	pg := PGConnect()

	topics := []string{"A16Z", "Algorand", "ApeCoin", "Aptos", "Artificial Intelligence", "Avalanche",
		"BNB", "Binance", "Binance USD", "Bitcoin", "BlockFi", "Blockstream", "Cardano",
		"Chainlink", "Circle", "Coinbase", "Cosmos", "Cosmos Hub", "Dai", "Digital Currency Group",
		"Dogecoin", "Dragonfly", "Ethereum", "FTX", "Filecoin", "Gaming", "Gemini", "Goldman Sachs",
		"HSBC", "Hedera", "IBM", "IMF", "JPMorgan", "Kraken", "Litecoin", "Marathon", "Microsoft",
		"Miners", "Multicoin", "NFT", "OKB", "Optimism", "Pantera", "PayPal", "Polkadot", "Polychain",
		"Polygon", "Regulation", "Riot", "Robinhood", "Santander", "Sequoia", "Seven Seven Six", "Shiba Inu",
		"Solana", "Stablecoins", "Stronghold Digital", "TRON", "Tether", "Tezos", "TrueUSD", "USDC", "USDT",
		"Union Square Ventures", "Uniswap", "VeChain", "Web3", "XRP"}

	var resTopics []ResultTopic
	var notAssets []ResultTopic
	for _, topic := range topics {
		fundamentals := make(map[string][]FundamentalsTopic)

		query := `
			SELECT 
				symbol,
				name,
				slug
			FROM 
				public.fundamentalslatest
			where 
				name = '` + topic + `'
				 `

		queryResult, err := pg.QueryContext(ctx, query)
		span.AddEvent("Query Executed")

		if err != nil {

			ConsumeTime("Check Topic Assets Query", startTime, err)
			span.SetStatus(codes.Error, "unable to get data for name from PG")
			return nil, err

		}

		for queryResult.Next() {
			var fundamental FundamentalsTopic
			err := queryResult.Scan(&fundamental.Symbol, &fundamental.Name, &fundamental.Slug)
			if err != nil {
				ConsumeTime("Check Topic Assets Query", startTime, err)
				return nil, err
			}

			fundamentals[topic] = append(fundamentals[topic], fundamental)
		}
		if len(fundamentals[topic]) == 0 {
			f, _ := CheckAllTopicAsset(ctx, topic)
			fundamentals[topic] = append(fundamentals[topic], f...)
		}
		if len(fundamentals[topic]) > 1 {
			res := ResultTopic{Topics: fundamentals, ResultLength: len(fundamentals[topic])}
			notAssets = append(notAssets, res)
		}

		res := ResultTopic{Topics: fundamentals, ResultLength: len(fundamentals[topic])}
		resTopics = append(resTopics, res)
	}
	ConsumeTime("Check Topic Assets Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")

	file, _ := json.MarshalIndent(resTopics, " ", "")
	_ = os.WriteFile("resTopics.json", file, 0644)

	file1, _ := json.MarshalIndent(notAssets, " ", "")
	_ = os.WriteFile("notAssets.json", file1, 0644)

	return resTopics, nil
}

func CheckAllTopicAsset(ctxO context.Context, name string) ([]FundamentalsTopic, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "CheckTopicAssets")
	defer span.End()

	startTime := StartTime("Check Topic Assets Query")

	pg := PGConnect()

	query := `
			SELECT 
				symbol,
				name,
				slug
			FROM 
				public.fundamentalslatest
			where 
				name like '` + name + `%'
			order by market_cap desc
				 `

	queryResult, err := pg.QueryContext(ctx, query)
	span.AddEvent("Query Executed")

	if err != nil {

		ConsumeTime("Check Topic Assets Query", startTime, err)
		span.SetStatus(codes.Error, "unable to get data for name from PG")
		return nil, err

	}

	var fundamentals []FundamentalsTopic
	for queryResult.Next() {
		var fundamental FundamentalsTopic
		err := queryResult.Scan(&fundamental.Symbol, &fundamental.Name, &fundamental.Slug)
		if err != nil {
			ConsumeTime("Check Topic Assets Query", startTime, err)
			return nil, err
		}

		fundamentals = append(fundamentals, fundamental)
	}

	ConsumeTime("Check Topic Assets Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")

	file, _ := json.MarshalIndent(fundamentals, " ", "")
	_ = os.WriteFile("fundamentalsTopicTest2.json", file, 0644)

	return fundamentals, nil
}

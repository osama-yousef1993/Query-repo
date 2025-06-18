
type Item struct {
	Category                string `json:"Category"`
	NewsTag                 string `json:"NewsTag"`
	NewsPageMetaTitle       string `json:"NewsPageMetaTitle"`
	NewsPageMetaDescription string `json:"NewsPageMetaDescription"`
	NewsPageH1              string `json:"NewsPageH1"`
	Description             string `json:"description"`
}

func SaveNewsTopic(ctx context.Context) {

	fs := GetFirestoreClient()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	topics, _ := services.GetNewsTopics(ctx)

	jsonData := `
	[
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Aave",
        "NewsPageMetaTitle": "Aave (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Aave news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Aave News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Algorand",
        "NewsPageMetaTitle": "Algorand (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Algorand news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Algorand News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Aptos",
        "NewsPageMetaTitle": "Aptos (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Aptos news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Aptos News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Arbitrum",
        "NewsPageMetaTitle": "Arbitrum (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Arbitrum news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Arbitrum News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Astar",
        "NewsPageMetaTitle": "Astar (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Astar news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Astar News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Avalanche",
        "NewsPageMetaTitle": "Avalanche (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Avalanche news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Avalanche News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Axie Infinity",
        "NewsPageMetaTitle": "Axie Infinity (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Axie Infinity news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Axie Infinity News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "BNB",
        "NewsPageMetaTitle": "BNB (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest BNB news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest BNB News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "BitTorrent",
        "NewsPageMetaTitle": "BitTorrent (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking BitTorrent news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest BitTorrent News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Bitcoin",
        "NewsPageMetaTitle": "Bitcoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Bitcoin news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Bitcoin News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Bitcoin Cash",
        "NewsPageMetaTitle": "Bitcoin Cash (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Bitcoin Cash news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Bitcoin Cash News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Bitcoin Cash",
        "NewsPageMetaTitle": "Bitcoin Cash News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Bitcoin Cash news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Bitcoin Cash News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Bitcoin ETF",
        "NewsPageMetaTitle": "Bitcoin ETF News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Bitcoin ETF news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Bitcoin ETF News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Bitcoin Halving",
        "NewsPageMetaTitle": "Bitcoin Halving News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Bitcoin Halving news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Bitcoin Halving News"
    },
    {
        "Category": "Miners",
        "NewsTag": "Bitcoin Mining",
        "NewsPageMetaTitle": "Bitcoin Mining News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Bitcoin Mining news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Bitcoin Mining News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Bitcoin SV",
        "NewsPageMetaTitle": "Bitcoin SV (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Bitcoin SV News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Bittensor",
        "NewsPageMetaTitle": "Bittensor (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Bittensor news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Bittensor News"
    },
    {
        "Category": "Enterprise Blockchain",
        "NewsTag": "BlackRock",
        "NewsPageMetaTitle": "BlackRock News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "  Stay informed with the latest BlackRock news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest BlackRock News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Blockchain",
        "NewsPageMetaTitle": "Blockchain News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Blockchain news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Blockchain News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Blur",
        "NewsPageMetaTitle": "Blur (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Blur news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Blur News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Bonk",
        "NewsPageMetaTitle": "Bonk (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Bonk news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Bonk News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "CBDC",
        "NewsPageMetaTitle": "CBDC (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking CBDC news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest CBDC News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Cardano",
        "NewsPageMetaTitle": "Cardano (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Cardano news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Cardano News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Celestia",
        "NewsPageMetaTitle": "Celestia (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Celestia News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Chainlink",
        "NewsPageMetaTitle": "Chainlink (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Chainlink news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Chainlink News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Cosmos Hub",
        "NewsPageMetaTitle": "Cosmos Hub (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Cosmos Hub News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Cronos",
        "NewsPageMetaTitle": "Cronos (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Cronos news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Cronos News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Crypto ETF",
        "NewsPageMetaTitle": "Crypto ETF News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Crypto ETF news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Crypto ETF News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Crypto Gaming",
        "NewsPageMetaTitle": "Crypto Gaming News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Crypto Gaming news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Crypto Gaming News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Crypto Governance",
        "NewsPageMetaTitle": "Crypto Governance News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Crypto Governance news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Crypto Governance News"
    },
    {
        "Category": "Miners",
        "NewsTag": "Crypto Mining",
        "NewsPageMetaTitle": "Crypto Mining News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Crypto Mining news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Crypto Mining News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Crypto Regulation",
        "NewsPageMetaTitle": "Crypto Regulation News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Crypto Regulation news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Crypto Regulation News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Crypto Taxes",
        "NewsPageMetaTitle": "Crypto Taxes News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Crypto Taxes news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Crypto Taxes News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Crypto Wallet",
        "NewsPageMetaTitle": "Crypto Wallet News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Crypto Wallet news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Crypto Wallet News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "DAO",
        "NewsPageMetaTitle": "DAO (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with DAO news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest DAO News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Dai",
        "NewsPageMetaTitle": "Dai (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Dai news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Dai News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "DeFi",
        "NewsPageMetaTitle": "DeFi (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest DeFi news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest DeFi News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Decentraland",
        "NewsPageMetaTitle": "Decentraland (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Decentraland news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Decentraland News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Digital Wallet",
        "NewsPageMetaTitle": "Digital Wallet News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Digital Wallet News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Dogecoin",
        "NewsPageMetaTitle": "Dogecoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Dogecoin news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Dogecoin News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "EOS",
        "NewsPageMetaTitle": "EOS (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest EOS news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest EOS News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Ethereum",
        "NewsPageMetaTitle": "Ethereum (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Ethereum news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Ethereum News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Ethereum Classic",
        "NewsPageMetaTitle": "Ethereum Classic (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Ethereum Classic news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Ethereum Classic News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Fantom",
        "NewsPageMetaTitle": "Fantom (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Fantom News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Filecoin",
        "NewsPageMetaTitle": "Filecoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Filecoin news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Filecoin News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "First Digital USD",
        "NewsPageMetaTitle": "First Digital USD (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking First Digital USD news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest First Digital USD News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Flow",
        "NewsPageMetaTitle": "Flow (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Flow news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Flow News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "GALA",
        "NewsPageMetaTitle": "GALA (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with GALA news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest GALA News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Grayscale",
        "NewsPageMetaTitle": "Grayscale News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Grayscale news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Grayscale News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Hedera",
        "NewsPageMetaTitle": "Hedera (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Hedera News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Helium",
        "NewsPageMetaTitle": "Helium (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Helium news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Helium News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "IOTA",
        "NewsPageMetaTitle": "IOTA (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest IOTA news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest IOTA News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Immutable",
        "NewsPageMetaTitle": "Immutable (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Immutable news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Immutable News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Injective",
        "NewsPageMetaTitle": "Injective (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Injective news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Injective News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Internet Computer",
        "NewsPageMetaTitle": "Internet Computer (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Internet Computer news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Internet Computer News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Kaspa",
        "NewsPageMetaTitle": "Kaspa (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Kaspa news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Kaspa News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Kava",
        "NewsPageMetaTitle": "Kava (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Kava news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Kava News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "KuCoin",
        "NewsPageMetaTitle": "KuCoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking KuCoin news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest KuCoin News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "LEO Token",
        "NewsPageMetaTitle": "LEO Token (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking LEO Token news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest LEO Token News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Lido DAO",
        "NewsPageMetaTitle": "Lido DAO (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Lido DAO news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Lido DAO News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Lido Staked Ether",
        "NewsPageMetaTitle": "Lido Staked Ether (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Lido Staked Ether news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Lido Staked Ether News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Litecoin",
        "NewsPageMetaTitle": "Litecoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Litecoin news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Litecoin News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Maker",
        "NewsPageMetaTitle": "Maker (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Maker news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Maker News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Mantle",
        "NewsPageMetaTitle": "Mantle (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Mantle news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Mantle News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Mina Protocol",
        "NewsPageMetaTitle": "Mina Protocol (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Mina Protocol news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Mina Protocol News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Monero",
        "NewsPageMetaTitle": "Monero (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Monero news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Monero News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "MultiversX",
        "NewsPageMetaTitle": "MultiversX (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest MultiversX News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "NEAR Protocol",
        "NewsPageMetaTitle": "NEAR Protocol (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking NEAR Protocol news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest NEAR Protocol News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "NEO",
        "NewsPageMetaTitle": "NEO (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in NEO news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest NEO News"
    },
    {
        "Category": "Industry",
        "NewsTag": "NFT",
        "NewsPageMetaTitle": "NFT News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking NFT news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest NFT News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "OKB",
        "NewsPageMetaTitle": "OKB (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in OKB news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest OKB News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "ORDI",
        "NewsPageMetaTitle": "ORDI (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in ORDI news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest ORDI News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Optimism",
        "NewsPageMetaTitle": "Optimism (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Optimism news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Optimism News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Osmosis",
        "NewsPageMetaTitle": "Osmosis (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Osmosis news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Osmosis News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "PancakeSwap",
        "NewsPageMetaTitle": "PancakeSwap (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking PancakeSwap news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest PancakeSwap News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Polkadot",
        "NewsPageMetaTitle": "Polkadot (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Polkadot news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Polkadot News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Polygon",
        "NewsPageMetaTitle": "Polygon (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Polygon news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Polygon News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Quant",
        "NewsPageMetaTitle": "Quant (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Quant news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Quant News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Render",
        "NewsPageMetaTitle": "Render (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Render news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Render News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Rocket Pool ETH",
        "NewsPageMetaTitle": "Rocket Pool ETH (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Rocket Pool ETH news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Rocket Pool ETH News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "SATS (Ordinals)",
        "NewsPageMetaTitle": "SATS (Ordinals) (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking SATS (Ordinals) news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest SATS (Ordinals) News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Sei",
        "NewsPageMetaTitle": "Sei (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Sei news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Sei News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Shiba Inu",
        "NewsPageMetaTitle": "Shiba Inu (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Shiba Inu news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Shiba Inu News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Solana",
        "NewsPageMetaTitle": "Solana (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Solana news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Solana News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Stacks",
        "NewsPageMetaTitle": "Stacks (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Stacks news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Stacks News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Stellar",
        "NewsPageMetaTitle": "Stellar (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Stellar news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Stellar News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Sui",
        "NewsPageMetaTitle": "Sui (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Sui news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Sui News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Synthetix Network",
        "NewsPageMetaTitle": "Synthetix Network (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Synthetix Network news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest Synthetix Network News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "THORChain",
        "NewsPageMetaTitle": "THORChain (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest THORChain news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest THORChain News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "TRON",
        "NewsPageMetaTitle": "TRON (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in TRON news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest TRON News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Tether",
        "NewsPageMetaTitle": "Tether (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Tether news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Tether News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Tezos",
        "NewsPageMetaTitle": "Tezos (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Tezos news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Tezos News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "The Graph",
        "NewsPageMetaTitle": "The Graph (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking The Graph news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageH1": "Latest The Graph News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "The Sandbox",
        "NewsPageMetaTitle": "The Sandbox (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest The Sandbox news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest The Sandbox News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Theta Network",
        "NewsPageMetaTitle": "Theta Network (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest Theta Network news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest Theta Network News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Tokenize Xchange",
        "NewsPageMetaTitle": "Tokenize Xchange (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Tokenize Xchange News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Toncoin",
        "NewsPageMetaTitle": "Toncoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in Toncoin news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest Toncoin News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "TrueUSD",
        "NewsPageMetaTitle": "TrueUSD (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest TrueUSD news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest TrueUSD News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "USDC",
        "NewsPageMetaTitle": "USDC (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with USDC news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest USDC News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Uniswap",
        "NewsPageMetaTitle": "Uniswap (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with Uniswap news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Uniswap News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "VeChain",
        "NewsPageMetaTitle": "VeChain (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in VeChain news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest VeChain News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "WOO",
        "NewsPageMetaTitle": "WOO (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest WOO news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest WOO News"
    },
    {
        "Category": "Industry",
        "NewsTag": "Web3",
        "NewsPageMetaTitle": "Web3 News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Explore breaking Web3 news on Forbes. From new innovations to regulatory shifts, we provide insights for Web3 enthusiasts. Stay ahead in the Web3 game!",
        "NewsPageH1": "Latest Web3 News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "WhiteBIT Coin",
        "NewsPageMetaTitle": "WhiteBIT Coin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest WhiteBIT Coin News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "Wrapped Bitcoin",
        "NewsPageMetaTitle": "Wrapped Bitcoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageH1": "Latest Wrapped Bitcoin News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "XRP",
        "NewsPageMetaTitle": "XRP (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Unlock the latest in XRP news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageH1": "Latest XRP News"
    },
    {
        "Category": "Protocol Tokens",
        "NewsTag": "dYdX",
        "NewsPageMetaTitle": "dYdX (Ticker) News: Today's Latest Stories By Forbes",
        "NewsPageMetaDescription": "Stay informed with the latest dYdX news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageH1": "Latest dYdX News"
    }
]
	`

	var items []Item

	// Unmarshal the JSON data into the slice
	err := json.Unmarshal([]byte(jsonData), &items)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return
	}
	for _, item := range items {

		topic := topics[item.NewsTag]
		fund, err := CheckTopicAssets(ctx, topic.AliasesName)
		var title string
		var slug string
		var url string
		if err != nil {
			log.Info("not exist")
			title = item.NewsPageMetaTitle
			slug = topic.Slug
		}
		if fund.Symbol != "" {
			t := fmt.Sprintf("(%s)", fund.DisplaySymbol)
			title = strings.Replace(item.NewsPageMetaTitle, "(Ticker)", strings.ToUpper(t), -1)
			slug = fund.Slug
			url = fmt.Sprintf("/assets/%s", slug)
		} else {
			title = item.NewsPageMetaTitle
			if topic.Slug != "" {
				s := strings.ToLower(topic.Slug)
				slug = strings.ReplaceAll(s, " ", "-")
				url = fmt.Sprintf("/news/%s", slug)
			} else {
				s := strings.ToLower(item.NewsTag)
				slug = strings.ReplaceAll(s, " ", "-")
				url = fmt.Sprintf("/news/%s", slug)
			}
		}
		if topic.TopicName != "" {
			fs.Collection(collectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
				"topicName":            topic.TopicName,
				"bertieTag":            topic.BertieTag,
				"topicUrl":             url,
				"topicOrder":           topic.TopicOrder,
				"description":          topic.Description,
				"isTrending":           topic.IsTrending,
				"isAsset":              topic.IsAsset,
				"isFeaturedHome":       topic.IsFeaturedHome,
				"titleTemplate":        title,
				"slug":                 slug,
				"topicPageDescription": item.NewsPageMetaDescription,
				"newsHeader":           item.NewsPageH1,
				"aliasesName":          topic.AliasesName,
			}, firestore.MergeAll)
		} else {
			file, _ := json.MarshalIndent(item, " ", "")
			fileName := fmt.Sprintf("topicsFiles/%s.json", item.NewsTag)
			_ = os.WriteFile(fileName, file, 0644)
			slug := strings.ToLower(item.NewsTag)
			// newsHeader := fmt.Sprintf("%s News", item.NewsTag)
			topicUrl := fmt.Sprintf("/news/%s", slug)
			fs.Collection(collectionName).Doc(item.NewsTag).Set(ctx, map[string]interface{}{
				"topicName":            item.NewsTag,
				"bertieTag":            item.NewsTag,
				"topicUrl":             topicUrl,
				"topicOrder":           0,
				"description":          topic.Description,
				"isTrending":           false,
				"isAsset":              false,
				"isFeaturedHome":       false,
				"titleTemplate":        title,
				"slug":                 slug,
				"topicPageDescription": item.NewsPageMetaDescription,
				"newsHeader":           item.NewsPageH1,
				"aliasesName":          item.NewsTag,
			}, firestore.MergeAll)
		}

	}

}
func FixNewsTopic(ctx context.Context) {

	fs := GetFirestoreClient()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	topics, _ := services.FixNewsTopics(ctx)

	for index, topic := range topics {
		// title := ""
		// if strings.Contains(topic.TitleTemplate, "|") {
		// 	title = fmt.Sprintf("%s News: Today's Latest Stories By Forbes", topic.TopicName)
		// } else {
		// 	title = topic.TitleTemplate
		// }
		fs.Collection(collectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"topicOrder": index + 1,
		}, firestore.MergeAll)

	}

}

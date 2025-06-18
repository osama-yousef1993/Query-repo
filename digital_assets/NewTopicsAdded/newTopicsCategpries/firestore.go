
type Item struct {
	Category                string `json:"Category"`
	NewsTag                 string `json:"NewsTag"`
	NewsPageMetaTitle       string `json:"NewsPageMetaTitle"`
	NewsPageMetaDescription string `json:"NewsPageMetaDescription"`
	NewsPageH1              string `json:"NewsPageH1"`
	Description             string `json:"description"`
}

func SaveNewsTopic(ctx context.Context, topics []services.TopicCategories) {

	fs := GetFirestoreClient()

	jsonData := `
	[
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Aave News",
        "NewsPageMetaDescription": "Keep up with Aave news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Aave (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Aave"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Algorand News",
        "NewsPageMetaDescription": "Stay informed with the latest Algorand news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Algorand (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Algorand"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Aptos News",
        "NewsPageMetaDescription": "Stay informed with the latest Aptos news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Aptos (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Aptos"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Arbitrum News",
        "NewsPageMetaDescription": "Stay informed with the latest Arbitrum news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Arbitrum (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Arbitrum"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Astar News",
        "NewsPageMetaDescription": "Unlock the latest in Astar news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Astar (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Astar"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Avalanche News",
        "NewsPageMetaDescription": "Keep up with Avalanche news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Avalanche (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Avalanche"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Axie Infinity News",
        "NewsPageMetaDescription": "Unlock the latest in Axie Infinity news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Axie Infinity (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Axie Infinity"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest BNB News",
        "NewsPageMetaDescription": "Stay informed with the latest BNB news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "BNB (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "BNB"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Bitcoin News",
        "NewsPageMetaDescription": "Stay informed with the latest Bitcoin news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Bitcoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Bitcoin"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Bitcoin Cash News",
        "NewsPageMetaDescription": "Explore breaking Bitcoin Cash news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Bitcoin Cash (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Bitcoin Cash"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Bitcoin SV News",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Bitcoin SV (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Bitcoin SV"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Bittensor News",
        "NewsPageMetaDescription": "Stay informed with the latest Bittensor news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Bittensor (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Bittensor"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Cardano News",
        "NewsPageMetaDescription": "Unlock the latest in Cardano news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Cardano (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Cardano"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Celestia News",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Celestia (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Celestia"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Chainlink News",
        "NewsPageMetaDescription": "Unlock the latest in Chainlink news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Chainlink (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Chainlink"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Cosmos Hub News",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Cosmos Hub (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Cosmos Hub"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Cronos News",
        "NewsPageMetaDescription": "Explore breaking Cronos news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Cronos (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Cronos"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Dogecoin News",
        "NewsPageMetaDescription": "Unlock the latest in Dogecoin news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Dogecoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Dogecoin"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Ethereum News",
        "NewsPageMetaDescription": "Explore breaking Ethereum news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Ethereum (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Ethereum"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Ethereum Classic News",
        "NewsPageMetaDescription": "Unlock the latest in Ethereum Classic news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Ethereum Classic (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Ethereum Classic"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Fantom News",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Fantom (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Fantom"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Filecoin News",
        "NewsPageMetaDescription": "Unlock the latest in Filecoin news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Filecoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Filecoin"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Flow News",
        "NewsPageMetaDescription": "Keep up with Flow news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Flow (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Flow"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Hedera News",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Hedera (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Hedera"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Helium News",
        "NewsPageMetaDescription": "Stay informed with the latest Helium news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Helium (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Helium"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Immutable News",
        "NewsPageMetaDescription": "Keep up with Immutable news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Immutable (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Immutable"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Injective News",
        "NewsPageMetaDescription": "Stay informed with the latest Injective news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Injective (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Injective"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Internet Computer News",
        "NewsPageMetaDescription": "Stay informed with the latest Internet Computer news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Internet Computer (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Internet Computer"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Kaspa News",
        "NewsPageMetaDescription": "Explore breaking Kaspa news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Kaspa (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Kaspa"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest KuCoin News",
        "NewsPageMetaDescription": "Explore breaking KuCoin news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "KuCoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "KuCoin"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest LEO Token News",
        "NewsPageMetaDescription": "Explore breaking LEO Token news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "LEO Token (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "LEO Token"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Lido DAO News",
        "NewsPageMetaDescription": "Unlock the latest in Lido DAO news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Lido DAO (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Lido DAO"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Lido Staked Ether News",
        "NewsPageMetaDescription": "Explore breaking Lido Staked Ether news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Lido Staked Ether (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Lido Staked Ether"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Litecoin News",
        "NewsPageMetaDescription": "Explore breaking Litecoin news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Litecoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Litecoin"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Maker News",
        "NewsPageMetaDescription": "Unlock the latest in Maker news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Maker (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Maker"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Mantle News",
        "NewsPageMetaDescription": "Unlock the latest in Mantle news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Mantle (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Mantle"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Mina Protocol News",
        "NewsPageMetaDescription": "Explore breaking Mina Protocol news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Mina Protocol (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Mina Protocol"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Monero News",
        "NewsPageMetaDescription": "Explore breaking Monero news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Monero (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Monero"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest MultiversX News",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "MultiversX (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "MultiversX"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest NEAR Protocol News",
        "NewsPageMetaDescription": "Explore breaking NEAR Protocol news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "NEAR Protocol (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "NEAR Protocol"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest OKB News",
        "NewsPageMetaDescription": "Unlock the latest in OKB news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "OKB (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "OKB"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest ORDI News",
        "NewsPageMetaDescription": "Unlock the latest in ORDI news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "ORDI (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "ORDI"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Optimism News",
        "NewsPageMetaDescription": "Unlock the latest in Optimism news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Optimism (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Optimism"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Osmosis News",
        "NewsPageMetaDescription": "Stay informed with the latest Osmosis news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Osmosis (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Osmosis"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Polkadot News",
        "NewsPageMetaDescription": "Keep up with Polkadot news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Polkadot (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Polkadot"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Polygon News",
        "NewsPageMetaDescription": "Keep up with Polygon news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Polygon (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Polygon"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Quant News",
        "NewsPageMetaDescription": "Keep up with Quant news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Quant (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Quant"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Render News",
        "NewsPageMetaDescription": "Stay informed with the latest Render news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Render (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Render"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Rocket Pool ETH News",
        "NewsPageMetaDescription": "Unlock the latest in Rocket Pool ETH news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Rocket Pool ETH (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Rocket Pool ETH"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest SATS (Ordinals) News",
        "NewsPageMetaDescription": "Explore breaking SATS (Ordinals) news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "SATS (Ordinals) (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "SATS (Ordinals)"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Sei News",
        "NewsPageMetaDescription": "Keep up with Sei news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Sei (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Sei"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Shiba Inu News",
        "NewsPageMetaDescription": "Explore breaking Shiba Inu news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Shiba Inu (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Shiba Inu"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Solana News",
        "NewsPageMetaDescription": "Unlock the latest in Solana news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Solana (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Solana"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Stacks News",
        "NewsPageMetaDescription": "Explore breaking Stacks news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Stacks (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Stacks"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Stellar News",
        "NewsPageMetaDescription": "Unlock the latest in Stellar news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Stellar (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Stellar"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Sui News",
        "NewsPageMetaDescription": "Explore breaking Sui news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Sui (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Sui"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Synthetix Network News",
        "NewsPageMetaDescription": "Explore breaking Synthetix Network news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "Synthetix Network (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Synthetix Network"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest THORChain News",
        "NewsPageMetaDescription": "Stay informed with the latest THORChain news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "THORChain (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "THORChain"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest TRON News",
        "NewsPageMetaDescription": "Unlock the latest in TRON news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "TRON (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "TRON"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest The Graph News",
        "NewsPageMetaDescription": "Explore breaking The Graph news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "The Graph (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "The Graph"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest The Sandbox News",
        "NewsPageMetaDescription": "Stay informed with the latest The Sandbox news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "The Sandbox (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "The Sandbox"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Theta Network News",
        "NewsPageMetaDescription": "Stay informed with the latest Theta Network news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "Theta Network (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Theta Network"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Tokenize Xchange News",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Tokenize Xchange (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Tokenize Xchange"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Toncoin News",
        "NewsPageMetaDescription": "Unlock the latest in Toncoin news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Toncoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Toncoin"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Uniswap News",
        "NewsPageMetaDescription": "Keep up with Uniswap news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Uniswap (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Uniswap"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest VeChain News",
        "NewsPageMetaDescription": "Unlock the latest in VeChain news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "VeChain (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "VeChain"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest WhiteBIT Coin News",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "WhiteBIT Coin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "WhiteBIT Coin"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest Wrapped Bitcoin News",
        "NewsPageMetaDescription": "Keep up with digital wallet news and trends with Forbes. From new wallet uses to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Wrapped Bitcoin (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Wrapped Bitcoin"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest XRP News",
        "NewsPageMetaDescription": "Unlock the latest in XRP news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "XRP (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "XRP"
    },
    {
        "Category": "Protocol Tokens",
        "NewsPageH1": "Latest dYdX News",
        "NewsPageMetaDescription": "Stay informed with the latest dYdX news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "dYdX (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "dYdX"
    },
    {
        "Category": "Stablecoins",
        "NewsPageH1": "Latest Dai News",
        "NewsPageMetaDescription": "Unlock the latest in Dai news trends and developments on Forbes. Instant updates and market analysis to keep you informed and engaged in the crypto space.",
        "NewsPageMetaTitle": "Dai (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Dai"
    },
    {
        "Category": "Stablecoins",
        "NewsPageH1": "Latest First Digital USD News",
        "NewsPageMetaDescription": "Explore breaking First Digital USD news on Forbes. From price movements to regulatory shifts, we provide insights for crypto enthusiasts. Stay ahead in the crypto game!",
        "NewsPageMetaTitle": "First Digital USD (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "First Digital USD"
    },
    {
        "Category": "Stablecoins",
        "NewsPageH1": "Latest Tether News",
        "NewsPageMetaDescription": "Keep up with Tether news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "Tether (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "Tether"
    },
    {
        "Category": "Stablecoins",
        "NewsPageH1": "Latest TrueUSD News",
        "NewsPageMetaDescription": "Stay informed with the latest TrueUSD news! Our page delivers real-time updates, expert analyses, and market trends. Dive into the world of crypto today!",
        "NewsPageMetaTitle": "TrueUSD (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "TrueUSD"
    },
    {
        "Category": "Stablecoins",
        "NewsPageH1": "Latest USDC News",
        "NewsPageMetaDescription": "Keep up with USDC news and market trends with Forbes. From price fluctuations to industry advancements, we keep you in the loop with timely updates.",
        "NewsPageMetaTitle": "USDC (Ticker) News: Today's Latest Stories By Forbes",
        "NewsTag": "USDC"
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
	for _, category := range topics {
		for _, topic := range category.CategoryTopics {
			for _, item := range items {
				if category.CategoryName == item.Category {
					if topic.TopicName == item.NewsTag {
						fs.Collection("dev_category_news").Doc(item.Category).Collection("topics").Doc(topic.DocId).Set(ctx, map[string]interface{}{
							"topicName": topic.TopicName,
						}, firestore.MergeAll)
					} else {
						fs.Collection("dev_category_news").Doc(item.Category).Collection("topics").NewDoc().Set(ctx, map[string]interface{}{
							"topicName": item.NewsTag,
						}, firestore.MergeAll)
					}
				}
			}
		}
	}

}

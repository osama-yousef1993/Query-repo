import csv
import json

topic_names = [
    "A16Z",
    "Algorand",
    "ApeCoin",
    "Aptos",
    "Artificial Intelligence",
    "Avalanche",
    "KuCoin",
    "BNB",
    "Binance",
    "Binance USD",
    "Bitcoin",
    "BlockFi",
    "Blockstream",
    "Cardano",
    "Chainlink",
    "Circle",
    "Coinbase",
    "Cosmos",
    "Crypto Gaming",
    "Dai",
    "Digital Currency Group",
    "Dogecoin",
    "Dragonfly",
    "Ethereum",
    "FTX",
    "Filecoin",
    "Gemini",
    "Goldman Sachs",
    "HSBC",
    "Hedera",
    "IBM",
    "IMF",
    "JPMorgan",
    "Kraken",
    "KuCoin",
    "Litecoin",
    "Marathon",
    "Microsoft",
    "Miners",
    "Multicoin",
    "NFT",
    "OKB",
    "Optimism",
    "Pantera",
    "PayPal",
    "Polkadot",
    "Polychain",
    "Polygon",
    "Regulation",
    "Riot",
    "Robinhood",
    "Santander",
    "Sequoia",
    "Seven Seven Six",
    "Shiba Inu",
    "Solana",
    "Stablecoins",
    "Stronghold Digital",
    "TRON",
    "Tether",
    "Tezos",
    "TrueUSD",
    "USDC",
    "USDT",
    "Union Square Ventures",
    "Uniswap",
    "VeChain",
    "Web3",
    "XRP",
]

top_75_names = [
    {
        "symbol": "bitcoin",
        "name": "Bitcoin",
        "slug": "bitcoin-btc",
        "market_cap": 948765948167,
        "has_articles": True,
    },
    {
        "symbol": "ethereum",
        "name": "Ethereum",
        "slug": "ethereum-eth",
        "market_cap": 301587032735,
        "has_articles": True,
    },
    {
        "symbol": "tether",
        "name": "Tether",
        "slug": "tether-usdt",
        "market_cap": 96405378584,
        "has_articles": True,
    },
    {
        "symbol": "binancecoin",
        "name": "BNB",
        "slug": "bnb-bnb",
        "market_cap": 49521879894,
        "has_articles": True,
    },
    {
        "symbol": "solana",
        "name": "Solana",
        "slug": "solana-sol",
        "market_cap": 47499084728,
        "has_articles": True,
    },
    {
        "symbol": "ripple",
        "name": "XRP",
        "slug": "xrp-xrp",
        "market_cap": 28872125443,
        "has_articles": True,
    },
    {
        "symbol": "usd-coin",
        "name": "USDC",
        "slug": "usdc-usdc",
        "market_cap": 27936698197,
        "has_articles": True,
    },
    {
        "symbol": "staked-ether",
        "name": "Lido Staked Ether",
        "slug": "lido-staked-ether-steth",
        "market_cap": 24148948133,
        "has_articles": False,
    },
    {
        "symbol": "cardano",
        "name": "Cardano",
        "slug": "cardano-ada",
        "market_cap": 19031809748,
        "has_articles": True,
    },
    {
        "symbol": "avalanche-2",
        "name": "Avalanche",
        "slug": "avalanche-avax",
        "market_cap": 14678724102,
        "has_articles": True,
    },
    {
        "symbol": "chainlink",
        "name": "Chainlink",
        "slug": "chainlink-link",
        "market_cap": 11924511590,
        "has_articles": True,
    },
    {
        "symbol": "dogecoin",
        "name": "Dogecoin",
        "slug": "dogecoin-doge",
        "market_cap": 11744505800,
        "has_articles": True,
    },
    {
        "symbol": "tron",
        "name": "TRON",
        "slug": "tron-trx",
        "market_cap": 10949236467,
        "has_articles": True,
    },
    {
        "symbol": "polkadot",
        "name": "Polkadot",
        "slug": "polkadot-dot",
        "market_cap": 9583003392,
        "has_articles": True,
    },
    {
        "symbol": "matic-network",
        "name": "Polygon",
        "slug": "polygon-matic",
        "market_cap": 7850243272,
        "has_articles": True,
    },
    {
        "symbol": "wrapped-bitcoin",
        "name": "Wrapped Bitcoin",
        "slug": "wrapped-bitcoin-wbtc",
        "market_cap": 7606697592,
        "has_articles": False,
    },
    {
        "symbol": "the-open-network",
        "name": "Toncoin",
        "slug": "toncoin-ton",
        "market_cap": 7292715598,
        "has_articles": False,
    },
    {
        "symbol": "internet-computer",
        "name": "Internet Computer",
        "slug": "internet-computer-icp",
        "market_cap": 6019667189,
        "has_articles": False,
    },
    {
        "symbol": "shiba-inu",
        "name": "Shiba Inu",
        "slug": "shiba-inu-shib",
        "market_cap": 5580727909,
        "has_articles": False,
    },
    {
        "symbol": "bitcoin-cash",
        "name": "Bitcoin Cash",
        "slug": "bitcoin-cash-bch",
        "market_cap": 5547960230,
        "has_articles": False,
    },
    {
        "symbol": "litecoin",
        "name": "Litecoin",
        "slug": "litecoin-ltc",
        "market_cap": 5367584712,
        "has_articles": True,
    },
    {
        "symbol": "uniswap",
        "name": "Uniswap",
        "slug": "uniswap-uni",
        "market_cap": 5124183722,
        "has_articles": True,
    },
    {
        "symbol": "dai",
        "name": "Dai",
        "slug": "dai-dai",
        "market_cap": 4921671304,
        "has_articles": True,
    },
    {
        "symbol": "leo-token",
        "name": "LEO Token",
        "slug": "leo-token-leo",
        "market_cap": 3832396233,
        "has_articles": False,
    },
    {
        "symbol": "immutable-x",
        "name": "Immutable",
        "slug": "immutable-imx",
        "market_cap": 3804133509,
        "has_articles": False,
    },
    {
        "symbol": "cosmos",
        "name": "Cosmos Hub",
        "slug": "cosmos-hub-atom",
        "market_cap": 3764756966,
        "has_articles": False,
    },
    {
        "symbol": "ethereum-classic",
        "name": "Ethereum Classic",
        "slug": "ethereum-classic-etc",
        "market_cap": 3694964467,
        "has_articles": False,
    },
    {
        "symbol": "bittensor",
        "name": "Bittensor",
        "slug": "bittensor-tao",
        "market_cap": 3425070529,
        "has_articles": False,
    },
    {
        "symbol": "optimism",
        "name": "Optimism",
        "slug": "optimism-op",
        "market_cap": 3402995900,
        "has_articles": True,
    },
    {
        "symbol": "near",
        "name": "NEAR Protocol",
        "slug": "near-protocol-near",
        "market_cap": 3284529036,
        "has_articles": False,
    },
    {
        "symbol": "celestia",
        "name": "Celestia",
        "slug": "celestia-tia",
        "market_cap": 3214445521,
        "has_articles": False,
    },
    {
        "symbol": "kaspa",
        "name": "Kaspa",
        "slug": "kaspa-kas",
        "market_cap": 3195499568,
        "has_articles": False,
    },
    {
        "symbol": "stellar",
        "name": "Stellar",
        "slug": "stellar-xlm",
        "market_cap": 3176605497,
        "has_articles": False,
    },
    {
        "symbol": "injective-protocol",
        "name": "Injective",
        "slug": "injective-inj",
        "market_cap": 3124851644,
        "has_articles": False,
    },
    {
        "symbol": "aptos",
        "name": "Aptos",
        "slug": "aptos-apt",
        "market_cap": 3089633073,
        "has_articles": True,
    },
    {
        "symbol": "okb",
        "name": "OKB",
        "slug": "okb-okb",
        "market_cap": 2965004791,
        "has_articles": True,
    },
    {
        "symbol": "first-digital-usd",
        "name": "First Digital USD",
        "slug": "first-digital-usd-fdusd",
        "market_cap": 2774404258,
        "has_articles": False,
    },
    {
        "symbol": "filecoin",
        "name": "Filecoin",
        "slug": "filecoin-fil",
        "market_cap": 2720763353,
        "has_articles": True,
    },
    {
        "symbol": "blockstack",
        "name": "Stacks",
        "slug": "stacks-stx",
        "market_cap": 2668360760,
        "has_articles": False,
    },
    {
        "symbol": "hedera-hashgraph",
        "name": "Hedera",
        "slug": "hedera-hbar",
        "market_cap": 2640126516,
        "has_articles": True,
    },
    {
        "symbol": "lido-dao",
        "name": "Lido DAO",
        "slug": "lido-dao-ldo",
        "market_cap": 2608291181,
        "has_articles": False,
    },
    {
        "symbol": "arbitrum",
        "name": "Arbitrum",
        "slug": "arbitrum-arb",
        "market_cap": 2495624674,
        "has_articles": False,
    },
    {
        "symbol": "crypto-com-chain",
        "name": "Cronos",
        "slug": "cronos-cro",
        "market_cap": 2304575472,
        "has_articles": False,
    },
    {
        "symbol": "vechain",
        "name": "VeChain",
        "slug": "vechain-vet",
        "market_cap": 2211349721,
        "has_articles": True,
    },
    {
        "symbol": "monero",
        "name": "Monero",
        "slug": "monero-xmr",
        "market_cap": 2202178560,
        "has_articles": False,
    },
    {
        "symbol": "mantle",
        "name": "Mantle",
        "slug": "mantle-mnt",
        "market_cap": 2079843022,
        "has_articles": False,
    },
    {
        "symbol": "sui",
        "name": "Sui",
        "slug": "sui-sui",
        "market_cap": 1983125267,
        "has_articles": False,
    },
    {
        "symbol": "maker",
        "name": "Maker",
        "slug": "maker-mkr",
        "market_cap": 1863609572,
        "has_articles": False,
    },
    {
        "symbol": "render-token",
        "name": "Render",
        "slug": "render-rndr",
        "market_cap": 1750394904,
        "has_articles": False,
    },
    {
        "symbol": "sei-network",
        "name": "Sei",
        "slug": "sei-sei",
        "market_cap": 1634698302,
        "has_articles": False,
    },
    {
        "symbol": "bitcoin-cash-sv",
        "name": "Bitcoin SV",
        "slug": "bitcoin-sv-bsv",
        "market_cap": 1595967082,
        "has_articles": False,
    },
    {
        "symbol": "the-graph",
        "name": "The Graph",
        "slug": "the-graph-grt",
        "market_cap": 1583454421,
        "has_articles": False,
    },
    {
        "symbol": "thorchain",
        "name": "THORChain",
        "slug": "thorchain-rune",
        "market_cap": 1561236562,
        "has_articles": False,
    },
    {
        "symbol": "rocket-pool-eth",
        "name": "Rocket Pool ETH",
        "slug": "rocket-pool-eth-reth",
        "market_cap": 1549569546,
        "has_articles": False,
    },
    {
        "symbol": "quant-network",
        "name": "Quant",
        "slug": "quant-qnt",
        "market_cap": 1501983692,
        "has_articles": False,
    },
    {
        "symbol": "elrond-erd-2",
        "name": "MultiversX",
        "slug": "multiversx-egld",
        "market_cap": 1486474728,
        "has_articles": False,
    },
    {
        "symbol": "True-usd",
        "name": "TrueUSD",
        "slug": "Trueusd-tusd",
        "market_cap": 1464177508,
        "has_articles": True,
    },
    {
        "symbol": "mina-protocol",
        "name": "Mina Protocol",
        "slug": "mina-protocol-mina",
        "market_cap": 1447263431,
        "has_articles": False,
    },
    {
        "symbol": "algorand",
        "name": "Algorand",
        "slug": "algorand-algo",
        "market_cap": 1404188785,
        "has_articles": True,
    },
    {
        "symbol": "ordinals",
        "name": "ORDI",
        "slug": "ordi-ordi",
        "market_cap": 1309200326,
        "has_articles": False,
    },
    {
        "symbol": "aave",
        "name": "Aave",
        "slug": "aave-aave",
        "market_cap": 1292302153,
        "has_articles": False,
    },
    {
        "symbol": "beam-2",
        "name": "Beam",
        "slug": "beam-beam",
        "market_cap": 1291648847,
        "has_articles": False,
    },
    {
        "symbol": "tokenize-xchange",
        "name": "Tokenize Xchange",
        "slug": "tokenize-xchange-tkx",
        "market_cap": 1254857012,
        "has_articles": False,
    },
    {
        "symbol": "dydx-chain",
        "name": "dYdX",
        "slug": "dydx-dydx",
        "market_cap": 1250502475,
        "has_articles": False,
    },
    {
        "symbol": "flow",
        "name": "Flow",
        "slug": "flow-flow",
        "market_cap": 1230706016,
        "has_articles": False,
    },
    {
        "symbol": "mantle-staked-ether",
        "name": "Mantle Staked Ether",
        "slug": "mantle-staked-ether-meth",
        "market_cap": 1185487322,
        "has_articles": False,
    },
    {
        "symbol": "helium",
        "name": "Helium",
        "slug": "helium-hnt",
        "market_cap": 1156783976,
        "has_articles": False,
    },
    {
        "symbol": "havven",
        "name": "Synthetix Network",
        "slug": "synthetix-network-snx",
        "market_cap": 1125550271,
        "has_articles": False,
    },
    {
        "symbol": "flare-networks",
        "name": "Flare",
        "slug": "flare-flr",
        "market_cap": 1121112160,
        "has_articles": False,
    },
    {
        "symbol": "fantom",
        "name": "Fantom",
        "slug": "fantom-ftm",
        "market_cap": 1095413918,
        "has_articles": False,
    },
    {
        "symbol": "dymension",
        "name": "Dymension",
        "slug": "dymension-dym",
        "market_cap": 1093849021,
        "has_articles": False,
    },
    {
        "symbol": "the-sandbox",
        "name": "The Sandbox",
        "slug": "the-sandbox-sand",
        "market_cap": 1055433135,
        "has_articles": False,
    },
    {
        "symbol": "osmosis",
        "name": "Osmosis",
        "slug": "osmosis-osmo",
        "market_cap": 1039039733,
        "has_articles": False,
    },
    {
        "symbol": "axie-infinity",
        "name": "Axie Infinity",
        "slug": "axie-infinity-axs",
        "market_cap": 1034882683,
        "has_articles": False,
    },
    {
        "symbol": "astar",
        "name": "Astar",
        "slug": "astar-astr",
        "market_cap": 1030453667,
        "has_articles": False,
    },
]


all_topics = list()
exist_topics = list()
not_exist_topics = list()
for i in range(len(top_75_names)):
    topic = top_75_names[i]
    topics = dict()
    if topic["name"] in topic_names:
        exist_topics.append(topic["name"])
        topics["TopicName"] = topic["name"]
        topics["BertieTag"] = topic["name"]
        topics["Description"] = ""
        topics["IsTrending"] = False
        topics["IsAsset"] = True
        topics["IsFeaturedHome"] = False
        topics["Slug"] = topic["slug"]
        topics["TopicURl"] = f"news/{topic['slug']}"
        topics["TopicOrder"] = 0
        topics["TitleTemplate"] = f"Latest {topic['name']} News | Forbes Digital Assets"
        topics["TopicPageDescription"] = ""
        topics["NewsHeader"] = topic["name"]
        topics["AliasesName"] = topic["name"]
        topics["HasArticles"] = topic["has_articles"]
    else:
        not_exist_topics.append(topic["name"])
        topics["TopicName"] = topic["name"]
        topics["BertieTag"] = topic["name"]
        topics["Description"] = ""
        topics["IsTrending"] = False
        topics["IsAsset"] = True
        topics["IsFeaturedHome"] = False
        topics["Slug"] = topic["slug"]
        topics["TopicURl"] = f"news/{topic['slug']}"
        topics["TopicOrder"] = 0
        topics["TitleTemplate"] = f"Latest {topic['name']} News | Forbes Digital Assets"
        topics["TopicPageDescription"] = ""
        topics["NewsHeader"] = topic["name"]
        topics["AliasesName"] = topic["name"]
        topics["HasArticles"] = topic["has_articles"]

    all_topics.append(topics)


with open("all_top75_topics.json", "w") as f:
    f.write(json.dumps(all_topics))

with open("all_exist_topics.json", "w") as f:
    f.write(json.dumps(exist_topics))

with open("not_exist_topics_in_DS.json", "w") as f:
    f.write(json.dumps(not_exist_topics))

fields = [
    "TopicName",
    "BertieTag",
    "Description",
    "IsTrending",
    "IsAsset",
    "IsFeaturedHome",
    "Slug",
    "TopicURl",
    "TopicOrder",
    "TitleTemplate",
    "TopicPageDescription",
    "NewsHeader",
    "AliasesName",
    "HasArticles",
]
with open("top75_assets.csv", "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=fields)
    writer.writeheader()
    writer.writerows(all_topics)

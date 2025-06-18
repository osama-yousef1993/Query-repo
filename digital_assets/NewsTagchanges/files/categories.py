import json
category = [
    {
        "topicName": "KuCoin",
        "slug": "kucoin-kcs",
        "topicUrl": "/assets/kucoin-kcs",
        "isAsset": True,
    },
    {
        "topicName": "Polkadot",
        "slug": "polkadot-dot",
        "topicUrl": "/assets/polkadot-dot",
        "isAsset": True,
    },
    {
        "topicName": "Aptos",
        "slug": "aptos-apt",
        "topicUrl": "/assets/aptos-apt",
        "isAsset": True,
    },
    {
        "topicName": "Avalanche",
        "slug": "avalanche-avax",
        "topicUrl": "/assets/avalanche-avax",
        "isAsset": True,
    },
    {
        "topicName": "Polygon",
        "slug": "polygon-matic",
        "topicUrl": "/assets/polygon-matic",
        "isAsset": True,
    },
    {
        "topicName": "ApeCoin",
        "slug": "apecoin-ape",
        "topicUrl": "/assets/apecoin-ape",
        "isAsset": True,
    },
    {
        "topicName": "Filecoin",
        "slug": "filecoin-fil",
        "topicUrl": "/assets/filecoin-fil",
        "isAsset": True,
    },
    {
        "topicName": "Solana",
        "slug": "solana-sol",
        "topicUrl": "/assets/solana-sol",
        "isAsset": True,
    },
    {
        "topicName": "Tezos",
        "slug": "tezos-xtz",
        "topicUrl": "/assets/tezos-xtz",
        "isAsset": True,
    },
    {
        "topicName": "Chainlink",
        "slug": "chainlink-link",
        "topicUrl": "/assets/chainlink-link",
        "isAsset": True,
    },
    {
        "topicName": "Shiba Inu",
        "slug": "shiba-inu-shib",
        "topicUrl": "/assets/shiba-inu-shib",
        "isAsset": True,
    },
    {
        "topicName": "XRP",
        "slug": "xrp-xrp",
        "topicUrl": "/assets/xrp-xrp",
        "isAsset": True,
    },
    {
        "topicName": "Optimism",
        "slug": "optimism-op",
        "topicUrl": "/assets/optimism-op",
        "isAsset": True,
    },
    {
        "topicName": "OKB",
        "slug": "okb-okb",
        "topicUrl": "/assets/okb-okb",
        "isAsset": True,
    },
    {
        "topicName": "Cosmos",
        "slug": "cosmos-hub-atom",
        "topicUrl": "/assets/cosmos-hub-atom",
        "isAsset": True,
    },
    {
        "topicName": "Algorand",
        "slug": "algorand-algo",
        "topicUrl": "/assets/algorand-algo",
        "isAsset": True,
    },
    {
        "topicName": "Litecoin",
        "slug": "litecoin-ltc",
        "topicUrl": "/assets/litecoin-ltc",
        "isAsset": True,
    },
    {
        "topicName": "TRON",
        "slug": "tron-trx",
        "topicUrl": "/assets/tron-trx",
        "isAsset": True,
    },
    {
        "topicName": "Uniswap",
        "slug": "uniswap-uni",
        "topicUrl": "/assets/uniswap-uni",
        "isAsset": True,
    },
    {
        "topicName": "Cardano",
        "slug": "cardano-ada",
        "topicUrl": "/assets/cardano-ada",
        "isAsset": True,
    },
    {
        "topicName": "Dogecoin",
        "slug": "dogecoin-doge",
        "topicUrl": "/assets/dogecoin-doge",
        "isAsset": True,
    },
    {
        "topicName": "Dai",
        "slug": "dai-dai",
        "topicUrl": "/assets/dai-dai",
        "isAsset": True,
    },
    {
        "topicName": "Hedera",
        "slug": "hedera-hbar",
        "topicUrl": "/assets/hedera-hbar",
        "isAsset": True,
    },
    {
        "topicName": "VeChain",
        "slug": "vechain-vet",
        "topicUrl": "/assets/vechain-vet",
        "isAsset": True,
    },
]

names = list()

for cat in category:
    names.append(cat["topicName"])

sorted_list = sorted(names)
with open("Protocol_Tokens.json", "w") as file:
    file.write(json.dumps(sorted_list))


stable = [
    {
        "topicName": "USDT",
        "slug": "tether-usdt",
        "topicUrl": "/assets/tether-usdt",
        "isAsset": True,
    },
    {
        "topicName": "Dai",
        "slug": "dai-dai",
        "topicUrl": "/assets/dai-dai",
        "isAsset": True,
    },
    {
        "topicName": "Tether",
        "slug": "tether-usdt",
        "topicUrl": "/assets/tether-usdt",
        "isAsset": True,
    },
    {
        "topicName": "TrueUSD",
        "slug": "Trueusd-tusd",
        "topicUrl": "/assets/Trueusd-tusd",
        "isAsset": True,
    },
    {
        "topicName": "Binance USD",
        "slug": "binance-usd-busd",
        "topicUrl": "/news/binance-usd-busd",
        "isAsset": False,
    },
    {
        "topicName": "USDC",
        "slug": "usdc-usdc",
        "topicUrl": "/assets/usdc-usdc",
        "isAsset": True,
    },
]


names2 = list()

for cat in stable:
    names2.append(cat["topicName"])

sorted_list2 = sorted(names2)
with open("Stablecoins.json", "w") as file:
    file.write(json.dumps(sorted_list2))
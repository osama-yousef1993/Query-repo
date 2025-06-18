# import pandas as pd


# csv_data = pd.read_csv("d.csv")

# print(csv_data)

# # json_data = csv_data.to_json(orient="records")

# # with open("json_data.json", "w") as jsonfile:
# #     jsonfile.write(json_data)

import csv
import json

all_data = list()
new_all_data = list()

with open("d.csv", "r") as f:
    # print(f.readlines())
    reader = csv.reader(f, delimiter=",")
    for row in reader:
        if row not in all_data:
            all_data.append(row)
# with open("d1.csv", "r") as f:
#     # print(f.readlines())
#     reader = csv.reader(f, delimiter=",")
#     for row in reader:
#         if row not in all_data:
#             new_all_data.append(row)
# print("Row is: ", row)

# all_data.extend(new_all_data[1:])

# with open("d3.csv", "r") as f:
#     # print(f.readlines())
#     reader = csv.reader(f, delimiter=",")
#     for row in reader:
#         if row not in all_data:
#             all_data.append(row)
# with open("new_data.json", "w") as file:
#     file.write(json.dumps(all_data))

data_list = list()
for item in all_data[1:]:
    data_result = dict()
    data_result["Category"] = item[1].replace("\t", "")
    data_result["NewsTag"] = item[2].replace("\t", "")
    data_result["NewsPageMetaTitle"] = (
        item[3].replace("\t", "").replace("[News Tag]", data_result["NewsTag"])
    )
    data_result["NewsPageMetaDescription"] = ",".join(item[4:-1]).replace("\t", "")
    data_result["NewsPageH1"] = item[-1].replace("\t", "")
    data_list.append(data_result)

newlist = sorted(data_list, key=lambda k: k["NewsTag"])

stable_coins = [
    "Alchemix USD",
    "BOB",
    "BUSD",
    "Basis Cash",
    "Bean",
    "BiLira",
    "Binance-Peg BUSD",
    "Bridged USDC (Polygon PoS Bridge)",
    "CNH Tether",
    "Celo Dollar",
    "Celo Euro",
    "Coin98 Dollar",
    "Convertible JPY Token",
    "DOLA",
    "Dai",
    "Decentralized USD",
    "Djed",
    "Ethena USDe",
    "Euro Tether",
    "Fei USD",
    "First Digital USD",
    "Frax",
    "GHO",
    "GYEN",
    "Gemini Dollar",
    "HUSD",
    "Iron",
    "Iron BSC",
    "JPY Coin",
    "JPY Coin v1",
    "Jarvis Synthetic Euro",
    "Jarvis Synthetic Swiss Franc",
    "Klaytn Dai",
    "Liquity USD",
    "Lista USD",
    "MAI",
    "Magic Internet Money",
    "Monerium EUR emoney",
    "One Cash",
    "Origin Dollar",
    "PAX Gold",
    "Parallel",
    "Pax Dollar",
    "PayPal USD",
    "Prisma mkUSD",
    "SORA Synthetic USD",
    "STASIS EURO",
    "Silk",
    "Sperax USD",
    "SpiceUSD",
    "TOR",
    "TerraClassicUSD",
    "Tether",
    "Tether Gold",
    "TrueUSD",
    "USD Balance",
    "USDC",
    "USDD",
    "USDK",
    "USDX",
    "USK",
    "Utopia USD",
    "VNX EURO",
    "Vai",
    "Verified USD",
    "Vesta Stable",
    "Wrapped USTC",
    "XDAI",
    "XIDR",
    "XSGD",
    "ZUSD",
    "Zasset zUSD",
    "agEUR",
    "bDollar",
    "crvUSD",
    "dForce USD",
    "eUSD",
    "flexUSD",
    "mStable USD",
    "poundtoken",
    "sEUR",
    "sUSD",
    "xDollar Stablecoin",
]

tokens = [
    "Algorand",
    "ApeCoin",
    "Aptos",
    "Avalanche",
    "Cardano",
    "Chainlink",
    "Cosmos",
    "Dai",
    "Dogecoin",
    "Filecoin",
    "Hedera",
    "KuCoin",
    "Litecoin",
    "OKB",
    "Optimism",
    "Polkadot",
    "Polygon",
    "Shiba Inu",
    "Solana",
    "TRON",
    "Tezos",
    "Uniswap",
    "VeChain",
    "XRP",
]

stable_token = [
    "Binance USD",
    "Dai",
    "Tether",
    "TrueUSD",
    "USDC",
    "USDT"
]
token_category = list()
stable_token_category = list()
categories = dict()
for cat in newlist:
    if cat["NewsTag"] in stable_coins:
        if "Stablecoins" in categories.keys():
            cat_list = categories["Stablecoins"]
            cat["Category"] = "Stablecoins"
            if cat["NewsTag"] not in stable_token:
                stable_token_category.append(cat["NewsTag"])
            cat_list.append(cat)
            categories["Stablecoins"] = cat_list
        else:
            cat_list = list()
            cat["Category"] = "Stablecoins"
            cat_list.append(cat)
            if cat["NewsTag"] not in stable_token:
                stable_token_category.append(cat["NewsTag"])
            categories["Stablecoins"] = cat_list
    else:
        if cat["Category"] in categories.keys():
            cat_list = categories[cat["Category"]]
            cat_list.append(cat)
            if cat["NewsTag"] not in tokens:
                token_category.append(cat["NewsTag"])
            categories[cat["Category"]] = cat_list
        else:
            cat_list = list()
            cat_list.append(cat)
            if cat["NewsTag"] not in tokens:
                token_category.append(cat["NewsTag"])
            categories[cat["Category"]] = cat_list

with open("categories_list2.json", "w") as file:
    file.write(json.dumps(categories))

sorted_tokens = sorted(token_category)
with open("unique_token.json", "w") as file:
    file.write(json.dumps(sorted_tokens))
sorted_stable_tokens = sorted(stable_token_category)
with open("stable_unique_token.json", "w") as file:
    file.write(json.dumps(sorted_stable_tokens))

# categories_data = list()
# headers = all_data[0]
# for i in range(len(headers)):
#     header = headers[i]
#     d = dict()
#     d["category"] = header
#     names = list()
#     for j in all_data[1:]:
#         name = j[i]
#         if name:
#             names.append(name.strip())

#     d["topics"] = names
#     categories_data.append(d)


# with open("categoires.json", "w") as file:
#     file.write(json.dumps(categories_data))

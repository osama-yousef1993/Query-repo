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
with open("d1.csv", "r") as f:
    # print(f.readlines())
    reader = csv.reader(f, delimiter=",")
    for row in reader:
        if row not in all_data:
            new_all_data.append(row)
        # print("Row is: ", row)

all_data.extend(new_all_data[1:])

with open("new_all_data.json", "w") as file:
    file.write(json.dumps(all_data))

data_list = list()
for item in all_data[1:]:
    data_result = dict()
    data_result["Category"] = item[1].replace("\t", "")
    data_result["NewsTag"] = item[2].replace("\t", "")
    data_result["NewsPageMetaTitle"] = (
        (item[3].replace("\t", "").replace("[News Tag] (Ticker)", data_result["NewsTag"]))
        if "[News Tag] (Ticker)" in item[3]
        else (
            item[3]
            .replace("\t", "")
            .replace("[News Tag]", data_result["NewsTag"])
        )
    )
    data_result["NewsPageMetaDescription"] = ",".join(item[4:-1]).replace("\t", "")
    data_result["NewsPageH1"] = item[-1].replace("\t", "")
    data_list.append(data_result)

newlist = sorted(data_list, key=lambda k: k["NewsTag"])
with open("new_data_list1.json", "w") as file:
    file.write(json.dumps(newlist))
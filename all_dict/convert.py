
# import datetime
# readable = datetime.datetime.fromtimestamp(1704835345197).isoformat()
# print(readable)


# print(time.strftime("%Y-%m-%d %H:%M:%S", 1704835345197))
# from datetime import datetime

# timestamp = 1704960263
# # converting timestamp to date
# dt_object = datetime.fromtimestamp(timestamp)

# print('Date and Time is:', dt_object)

# import calendar
# import time

# timestamp = calendar.timegm(time.gmtime())
# print('Timestamp:', timestamp)
import json
resList = list()
with open("numberNames.json") as json_file:
    json_data = json.load(json_file)
    for da in json_data:
        res = dict()
        res['symbol'] = da['symbol']
        resList.append(res)
# with open('numberNames.json', 'r') as f:
#     data = f.readlines()
#     data = json.loads(json.dumps(data))
#     print(type(data))
#     for da in data:
#         print(type(da))
#         print(json.loads(json.dumps(da)))
#         res = dict()
#         res['symbol'] = da['symbol']
#         resList.append(res)

with open('resNumberNames.json', 'w') as file:
    file.write(json.dumps(resList))

import json

topic = list()
new_topic = list()

with open('test.txt', 'r') as f:

    topic.append(f.readlines())

print(">>>>>>>>>>>>>>>>"*20)
print(len(topic[0]))
print(">>>>>>>>>>>>>>>>"*20)

for top in topic[0]:
    t = top.replace('\n', '')
    t = t.strip()
    if t not in new_topic:
        new_topic.append(t)
    if t in new_topic:
        print(t)


print(">>>>>>>>>>>>>>>>"*20)
print(len(new_topic))
print(">>>>>>>>>>>>>>>>"*20)
print(new_topic)

print(json.dumps(sorted(new_topic), indent=4, sort_keys=True))

lklk= [
    {
        "Time": "2023-07-27T14:00:06.130972Z",
        "Price": 14995.64,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14995639,
        "volume_native": 7.75,
        "volume_usd": 14527.03
    },
    {
        "Time": "2023-07-27T14:15:06.208828Z",
        "Price": 14991.88,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14991879,
        "volume_native": 7.75,
        "volume_usd": 14523.38
    },
    {
        "Time": "2023-07-27T14:30:06.187744Z",
        "Price": 14975.79,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14975792,
        "volume_native": 7.75,
        "volume_usd": 14507.8
    },
    {
        "Time": "2023-07-27T14:45:06.149662Z",
        "Price": 14967.78,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14967780,
        "volume_native": 7.75,
        "volume_usd": 14500.04
    },
    {
        "Time": "2023-07-27T15:00:07.711485Z",
        "Price": 14947.01,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14947014,
        "volume_native": 7.75,
        "volume_usd": 14479.92
    },
    {
        "Time": "2023-07-27T15:15:06.661182Z",
        "Price": 14947.55,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14947552,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T15:30:06.705031Z",
        "Price": 14947.55,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14947552,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T15:45:06.073109Z",
        "Price": 14950.65,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14950654,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T16:00:05.683341Z",
        "Price": 14950.65,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14950654,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T16:15:07.676016Z",
        "Price": 14961.35,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14961348,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T16:30:07.537873Z",
        "Price": 14961.35,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14961348,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T16:45:08.078263Z",
        "Price": 14950.74,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14950737,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T17:00:06.404832Z",
        "Price": 14950.74,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14950737,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T17:15:05.825636Z",
        "Price": 14937.15,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14937146,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T17:30:06.74873Z",
        "Price": 14937.15,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14937146,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T17:45:06.777Z",
        "Price": 14925.48,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14925485,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T18:00:06.054075Z",
        "Price": 14925.48,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14925485,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T18:15:08.675072Z",
        "Price": 14915.11,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14915109,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T18:30:06.420814Z",
        "Price": 14915.11,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14915109,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T18:45:06.275647Z",
        "Price": 14893.45,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14893445,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T19:00:06.293079Z",
        "Price": 14893.45,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14893445,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T19:15:05.813463Z",
        "Price": 14876.83,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14876834,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T19:30:05.817919Z",
        "Price": 14876.83,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14876834,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T19:45:06.137511Z",
        "Price": 14883.75,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14883750,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T20:00:05.588728Z",
        "Price": 14883.75,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14883750,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T20:15:07.446381Z",
        "Price": 14887.27,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14887266,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T20:30:06.344804Z",
        "Price": 14887.27,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14887266,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T20:45:05.927841Z",
        "Price": 14882.87,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14882875,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T21:00:07.061031Z",
        "Price": 14882.87,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14882875,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T21:15:08.144356Z",
        "Price": 14876.96,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14876962,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T21:30:07.924621Z",
        "Price": 14876.96,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14876962,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T21:45:09.795205Z",
        "Price": 14865.57,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14865570,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T22:00:08.881986Z",
        "Price": 14865.57,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14865570,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T22:15:06.960034Z",
        "Price": 14882.32,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14882325,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T22:30:05.694446Z",
        "Price": 14882.32,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14882325,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T22:45:06.755175Z",
        "Price": 14869.66,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14869662,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T23:00:06.919629Z",
        "Price": 14869.66,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14869662,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T23:15:07.647843Z",
        "Price": 14847.9,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14847896,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T23:30:06.57168Z",
        "Price": 14847.9,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14847896,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-27T23:45:06.376062Z",
        "Price": 14864.49,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14864492,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T00:00:06.40015Z",
        "Price": 14864.49,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14864492,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T00:05:44Z",
        "Price": 14864.491932252304,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14864491.932252303,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T00:05:44Z",
        "Price": 14864.491932252304,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14864491.932252303,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T00:05:44Z",
        "Price": 14864.491932252304,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14864491.932252303,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T00:05:44Z",
        "Price": 14864.491932252304,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14864491.932252303,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T00:15:06.710013Z",
        "Price": 14884.14,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14884136,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T00:30:06.160488Z",
        "Price": 14884.14,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14884136,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T00:45:06.479562Z",
        "Price": 14896.4,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14896396,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T01:00:07.621165Z",
        "Price": 14896.4,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14896396,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T01:15:05.712301Z",
        "Price": 14895.74,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14895737,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T01:30:05.888857Z",
        "Price": 14895.74,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14895737,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T01:45:06.256973Z",
        "Price": 14899.27,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14899266,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T02:00:05.67032Z",
        "Price": 14899.27,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14899266,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T02:15:07.374476Z",
        "Price": 14903.99,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14903992,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T02:30:06.696376Z",
        "Price": 14903.99,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14903992,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T02:45:06.916413Z",
        "Price": 14890.24,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14890235,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T03:00:21.346007Z",
        "Price": 14890.24,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14890235,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T03:15:22.859615Z",
        "Price": 14895.92,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14895923,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T03:30:21.090418Z",
        "Price": 14895.92,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14895923,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T03:45:19.452718Z",
        "Price": 14915.9,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14915896,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T04:01:49.961019Z",
        "Price": 14915.9,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14915896,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T04:16:43.535007Z",
        "Price": 14889.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14889602,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T04:31:41.030393Z",
        "Price": 14889.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14889602,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T04:46:42.582392Z",
        "Price": 14906.43,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14906433,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T05:02:34.351633Z",
        "Price": 14906.43,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14906433,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T05:16:09.541107Z",
        "Price": 14901.71,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14901709,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T05:31:11.813952Z",
        "Price": 14901.71,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14901709,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T05:46:10.94264Z",
        "Price": 14891.03,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14891029,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T06:00:05.797241Z",
        "Price": 14891.03,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14891029,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T06:15:06.31142Z",
        "Price": 14855.37,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14855371,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T06:30:06.694027Z",
        "Price": 14855.37,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14855371,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T06:45:06.685233Z",
        "Price": 14874.86,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14874861,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T07:00:06.249968Z",
        "Price": 14874.86,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14874861,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T07:15:05.810587Z",
        "Price": 14874.99,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14874989,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T07:30:06.189538Z",
        "Price": 14874.99,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14874989,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T07:45:05.814067Z",
        "Price": 14895.31,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14895308,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T08:00:05.534337Z",
        "Price": 14895.31,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14895308,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T08:15:06.167191Z",
        "Price": 14875.53,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14875533,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T08:30:08.39646Z",
        "Price": 14875.53,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14875533,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T08:45:07.513735Z",
        "Price": 14899.48,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14899476,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T09:00:06.544985Z",
        "Price": 14899.48,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14899476,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-28T09:15:06.434635Z",
        "Price": 14897.84,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14897842,
        "volume_native": 8.61,
        "volume_usd": 16033.8
    },
    {
        "Time": "2023-07-28T09:30:06.177066Z",
        "Price": 14900.31,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14900309,
        "volume_native": 8.61,
        "volume_usd": 16036.46
    },
    {
        "Time": "2023-07-28T09:45:08.968012Z",
        "Price": 14909.11,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14909113,
        "volume_native": 8.61,
        "volume_usd": 16045.93
    },
    {
        "Time": "2023-07-28T10:00:06.982Z",
        "Price": 14936.63,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14936630,
        "volume_native": 8.61,
        "volume_usd": 16075.55
    },
    {
        "Time": "2023-07-28T10:15:08.242889Z",
        "Price": 14941.63,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14941629,
        "volume_native": 8.61,
        "volume_usd": 16080.93
    },
    {
        "Time": "2023-07-28T10:30:07.994333Z",
        "Price": 14935.14,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14935142,
        "volume_native": 8.61,
        "volume_usd": 16073.95
    },
    {
        "Time": "2023-07-28T10:45:09.606124Z",
        "Price": 14929.16,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14929162,
        "volume_native": 8.61,
        "volume_usd": 16067.51
    },
    {
        "Time": "2023-07-28T11:00:06.646845Z",
        "Price": 14937.66,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14937663,
        "volume_native": 8.61,
        "volume_usd": 16076.66
    },
    {
        "Time": "2023-07-28T11:15:05.610761Z",
        "Price": 14926.84,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14926837,
        "volume_native": 8.61,
        "volume_usd": 16065.01
    },
    {
        "Time": "2023-07-28T11:30:06.94687Z",
        "Price": 14925.17,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14925174,
        "volume_native": 8.61,
        "volume_usd": 16063.22
    },
    {
        "Time": "2023-07-28T11:45:06.285756Z",
        "Price": 14942.98,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14942980,
        "volume_native": 8.61,
        "volume_usd": 16082.38
    },
    {
        "Time": "2023-07-28T12:00:07.611264Z",
        "Price": 14943.82,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14943818,
        "volume_native": 8.61,
        "volume_usd": 16083.28
    },
    {
        "Time": "2023-07-28T12:15:10.439146Z",
        "Price": 14957.1,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14957102,
        "volume_native": 8.61,
        "volume_usd": 16097.58
    },
    {
        "Time": "2023-07-28T12:30:08.463218Z",
        "Price": 14957.16,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14957162,
        "volume_native": 8.61,
        "volume_usd": 16097.65
    },
    {
        "Time": "2023-07-28T12:45:06.698472Z",
        "Price": 14969.38,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14969376,
        "volume_native": 8.61,
        "volume_usd": 16110.79
    },
    {
        "Time": "2023-07-28T13:00:06.09131Z",
        "Price": 14977.4,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14977401,
        "volume_native": 8.61,
        "volume_usd": 16119.43
    },
    {
        "Time": "2023-07-28T13:15:07.157173Z",
        "Price": 14975.57,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14975574,
        "volume_native": 8.61,
        "volume_usd": 16117.46
    },
    {
        "Time": "2023-07-28T13:30:06.749356Z",
        "Price": 14973.44,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14973442,
        "volume_native": 8.61,
        "volume_usd": 16115.17
    },
    {
        "Time": "2023-07-28T13:45:06.069049Z",
        "Price": 14956,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14956004,
        "volume_native": 8.61,
        "volume_usd": 16096.4
    },
    {
        "Time": "2023-07-28T14:00:05.185514Z",
        "Price": 14956.27,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14956271,
        "volume_native": 8.61,
        "volume_usd": 16096.69
    },
    {
        "Time": "2023-07-28T14:15:08.002607Z",
        "Price": 14975.18,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14975179,
        "volume_native": 8.61,
        "volume_usd": 16117.04
    },
    {
        "Time": "2023-07-28T14:30:07.986618Z",
        "Price": 15023.14,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15023137,
        "volume_native": 8.61,
        "volume_usd": 16168.65
    },
    {
        "Time": "2023-07-28T14:45:06.720296Z",
        "Price": 15032.37,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15032373,
        "volume_native": 8.61,
        "volume_usd": 16178.59
    },
    {
        "Time": "2023-07-28T15:00:07.741507Z",
        "Price": 15019.41,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15019406,
        "volume_native": 8.61,
        "volume_usd": 16164.64
    },
    {
        "Time": "2023-07-28T15:15:06.599426Z",
        "Price": 15045.35,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15045353,
        "volume_native": 8.61,
        "volume_usd": 16192.56
    },
    {
        "Time": "2023-07-28T15:30:06.7293Z",
        "Price": 15031.87,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15031872,
        "volume_native": 8.61,
        "volume_usd": 16178.05
    },
    {
        "Time": "2023-07-28T15:45:07.437339Z",
        "Price": 14987.67,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14987669,
        "volume_native": 8.61,
        "volume_usd": 16130.48
    },
    {
        "Time": "2023-07-28T16:00:08.136345Z",
        "Price": 14996.98,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14996978,
        "volume_native": 8.61,
        "volume_usd": 16140.5
    },
    {
        "Time": "2023-07-28T16:15:08.078425Z",
        "Price": 14986.18,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14986185,
        "volume_native": 8.61,
        "volume_usd": 16128.88
    },
    {
        "Time": "2023-07-28T16:30:05.664977Z",
        "Price": 15007.57,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15007567,
        "volume_native": 8.61,
        "volume_usd": 16151.89
    },
    {
        "Time": "2023-07-28T16:45:08.252424Z",
        "Price": 15015.84,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15015837,
        "volume_native": 8.61,
        "volume_usd": 16160.8
    },
    {
        "Time": "2023-07-28T17:00:07.142628Z",
        "Price": 15019.47,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15019475,
        "volume_native": 8.61,
        "volume_usd": 16164.71
    },
    {
        "Time": "2023-07-28T17:15:06.284363Z",
        "Price": 14984.06,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14984061,
        "volume_native": 8.61,
        "volume_usd": 16126.6
    },
    {
        "Time": "2023-07-28T17:30:06.035358Z",
        "Price": 14974.76,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14974757,
        "volume_native": 8.61,
        "volume_usd": 16116.58
    },
    {
        "Time": "2023-07-28T17:45:06.679078Z",
        "Price": 14978.86,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14978862,
        "volume_native": 15.91,
        "volume_usd": 29789
    },
    {
        "Time": "2023-07-28T18:00:08.388087Z",
        "Price": 14953.26,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14953256,
        "volume_native": 15.91,
        "volume_usd": 29738
    },
    {
        "Time": "2023-07-28T18:15:08.507467Z",
        "Price": 14969.49,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14969489,
        "volume_native": 15.91,
        "volume_usd": 29771
    },
    {
        "Time": "2023-07-28T18:30:06.311262Z",
        "Price": 14985.73,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14985729,
        "volume_native": 15.91,
        "volume_usd": 29803
    },
    {
        "Time": "2023-07-28T18:45:08.499097Z",
        "Price": 14985.24,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14985237,
        "volume_native": 15.91,
        "volume_usd": 29802
    },
    {
        "Time": "2023-07-28T19:00:06.97078Z",
        "Price": 15002.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15002595,
        "volume_native": 15.91,
        "volume_usd": 29836
    },
    {
        "Time": "2023-07-28T19:15:06.087661Z",
        "Price": 15007.81,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15007809,
        "volume_native": 15.91,
        "volume_usd": 29847
    },
    {
        "Time": "2023-07-28T19:30:07.833314Z",
        "Price": 15001.32,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15001316,
        "volume_native": 15.91,
        "volume_usd": 29834
    },
    {
        "Time": "2023-07-28T19:45:08.313128Z",
        "Price": 14996.64,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14996636,
        "volume_native": 15.91,
        "volume_usd": 29825
    },
    {
        "Time": "2023-07-28T20:00:05.755812Z",
        "Price": 14996.65,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14996654,
        "volume_native": 15.91,
        "volume_usd": 29825
    },
    {
        "Time": "2023-07-28T20:15:06.0469Z",
        "Price": 14984.42,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14984423,
        "volume_native": 15.91,
        "volume_usd": 29800
    },
    {
        "Time": "2023-07-28T20:30:08.134991Z",
        "Price": 14984.92,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14984919,
        "volume_native": 15.91,
        "volume_usd": 29801
    },
    {
        "Time": "2023-07-28T20:45:07.255906Z",
        "Price": 15008.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15008602,
        "volume_native": 15.91,
        "volume_usd": 29848
    },
    {
        "Time": "2023-07-28T21:00:07.508575Z",
        "Price": 15015.25,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15015248,
        "volume_native": 15.91,
        "volume_usd": 29862
    },
    {
        "Time": "2023-07-28T21:15:07.294299Z",
        "Price": 15001.78,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15001782,
        "volume_native": 15.91,
        "volume_usd": 29835
    },
    {
        "Time": "2023-07-28T21:30:08.436086Z",
        "Price": 15011.12,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15011117,
        "volume_native": 15.91,
        "volume_usd": 29853
    },
    {
        "Time": "2023-07-28T21:45:05.8089Z",
        "Price": 15013.84,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15013842,
        "volume_native": 15.91,
        "volume_usd": 29859
    },
    {
        "Time": "2023-07-28T22:00:07.644327Z",
        "Price": 15007.77,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15007767,
        "volume_native": 15.91,
        "volume_usd": 29847
    },
    {
        "Time": "2023-07-28T22:15:06.600233Z",
        "Price": 15008.32,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15008324,
        "volume_native": 15.91,
        "volume_usd": 29848
    },
    {
        "Time": "2023-07-28T22:30:06.288064Z",
        "Price": 15008.12,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15008125,
        "volume_native": 15.91,
        "volume_usd": 29847
    },
    {
        "Time": "2023-07-28T22:45:06.605145Z",
        "Price": 15004.36,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15004363,
        "volume_native": 15.91,
        "volume_usd": 29840
    },
    {
        "Time": "2023-07-28T23:00:06.468375Z",
        "Price": 15010.26,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15010264,
        "volume_native": 15.91,
        "volume_usd": 29852
    },
    {
        "Time": "2023-07-28T23:15:08.02978Z",
        "Price": 15005.87,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15005872,
        "volume_native": 15.91,
        "volume_usd": 29843
    },
    {
        "Time": "2023-07-28T23:30:05.708158Z",
        "Price": 15009.29,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15009292,
        "volume_native": 15.91,
        "volume_usd": 29850
    },
    {
        "Time": "2023-07-28T23:45:06.682527Z",
        "Price": 15002.84,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15002837,
        "volume_native": 15.91,
        "volume_usd": 29837
    },
    {
        "Time": "2023-07-29T00:00:08.975578Z",
        "Price": 14997.24,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14997239,
        "volume_native": 15.91,
        "volume_usd": 29826
    },
    {
        "Time": "2023-07-29T00:05:15Z",
        "Price": 14995.885943715624,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14995885.943715628,
        "volume_native": 15.91,
        "volume_usd": 29823.068170564446
    },
    {
        "Time": "2023-07-29T00:05:15Z",
        "Price": 14995.885943715624,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14995885.943715628,
        "volume_native": 15.91,
        "volume_usd": 29823.068170564446
    },
    {
        "Time": "2023-07-29T00:05:15Z",
        "Price": 14995.885943715624,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14995885.943715628,
        "volume_native": 15.91,
        "volume_usd": 29823.068170564446
    },
    {
        "Time": "2023-07-29T00:15:06.374177Z",
        "Price": 14994.03,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14994028,
        "volume_native": 15.91,
        "volume_usd": 29819
    },
    {
        "Time": "2023-07-29T00:30:06.413258Z",
        "Price": 15012.65,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15012649,
        "volume_native": 15.91,
        "volume_usd": 29856
    },
    {
        "Time": "2023-07-29T00:45:08.615363Z",
        "Price": 15010.56,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15010557,
        "volume_native": 15.91,
        "volume_usd": 29852
    },
    {
        "Time": "2023-07-29T01:00:06.700713Z",
        "Price": 15004.63,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15004630,
        "volume_native": 15.91,
        "volume_usd": 29840
    },
    {
        "Time": "2023-07-29T01:15:06.532246Z",
        "Price": 14995.92,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14995918,
        "volume_native": 15.91,
        "volume_usd": 29823
    },
    {
        "Time": "2023-07-29T01:30:05.949018Z",
        "Price": 14990.69,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14990688,
        "volume_native": 15.91,
        "volume_usd": 29813
    },
    {
        "Time": "2023-07-29T01:45:06.412276Z",
        "Price": 15000.02,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15000024,
        "volume_native": 15.91,
        "volume_usd": 29831
    },
    {
        "Time": "2023-07-29T02:00:05.539217Z",
        "Price": 15016.38,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15016379,
        "volume_native": 15.91,
        "volume_usd": 29864
    },
    {
        "Time": "2023-07-29T02:15:07.3625Z",
        "Price": 15002.18,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15002180,
        "volume_native": 15.91,
        "volume_usd": 29836
    },
    {
        "Time": "2023-07-29T02:30:06.80583Z",
        "Price": 14990.8,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14990801,
        "volume_native": 15.91,
        "volume_usd": 29813
    },
    {
        "Time": "2023-07-29T02:45:06.004653Z",
        "Price": 14989.68,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14989681,
        "volume_native": 15.91,
        "volume_usd": 29811
    },
    {
        "Time": "2023-07-29T03:00:20.77916Z",
        "Price": 14994.39,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14994390,
        "volume_native": 15.91,
        "volume_usd": 29820
    },
    {
        "Time": "2023-07-29T03:15:21.274531Z",
        "Price": 14996.9,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14996896,
        "volume_native": 15.91,
        "volume_usd": 29825
    },
    {
        "Time": "2023-07-29T03:30:21.478775Z",
        "Price": 14994.31,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14994313,
        "volume_native": 15.91,
        "volume_usd": 29820
    },
    {
        "Time": "2023-07-29T03:45:20.241991Z",
        "Price": 14993.32,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14993321,
        "volume_native": 15.91,
        "volume_usd": 29818
    },
    {
        "Time": "2023-07-29T04:15:20.554011Z",
        "Price": 14989.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14989596,
        "volume_native": 15.91,
        "volume_usd": 29811
    },
    {
        "Time": "2023-07-29T04:30:20.357343Z",
        "Price": 15005.41,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15005410,
        "volume_native": 15.91,
        "volume_usd": 29842
    },
    {
        "Time": "2023-07-29T04:45:20.262533Z",
        "Price": 15014.61,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15014607,
        "volume_native": 15.91,
        "volume_usd": 29860
    },
    {
        "Time": "2023-07-29T05:01:18.48091Z",
        "Price": 15014.16,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15014162,
        "volume_native": 15.91,
        "volume_usd": 29859
    },
    {
        "Time": "2023-07-29T05:15:07.041384Z",
        "Price": 15008.91,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15008909,
        "volume_native": 15.91,
        "volume_usd": 29849
    },
    {
        "Time": "2023-07-29T05:30:06.478847Z",
        "Price": 14998.13,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14998131,
        "volume_native": 15.91,
        "volume_usd": 29828
    },
    {
        "Time": "2023-07-29T05:45:06.615814Z",
        "Price": 14988.55,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14988550,
        "volume_native": 15.91,
        "volume_usd": 29808
    },
    {
        "Time": "2023-07-29T06:00:06.157939Z",
        "Price": 14990.55,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14990555,
        "volume_native": 15.91,
        "volume_usd": 29812
    },
    {
        "Time": "2023-07-29T06:15:05.653524Z",
        "Price": 14986.25,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14986248,
        "volume_native": 15.91,
        "volume_usd": 29804
    },
    {
        "Time": "2023-07-29T06:30:06.112604Z",
        "Price": 14985.55,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14985545,
        "volume_native": 15.91,
        "volume_usd": 29803
    },
    {
        "Time": "2023-07-29T06:45:06.358946Z",
        "Price": 14984.64,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14984645,
        "volume_native": 15.91,
        "volume_usd": 29801
    },
    {
        "Time": "2023-07-29T07:00:06.23566Z",
        "Price": 14985.85,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14985848,
        "volume_native": 15.91,
        "volume_usd": 29803
    },
    {
        "Time": "2023-07-29T07:15:05.404187Z",
        "Price": 14988.83,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14988825,
        "volume_native": 15.91,
        "volume_usd": 29809
    },
    {
        "Time": "2023-07-29T07:30:06.067137Z",
        "Price": 14985.27,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14985272,
        "volume_native": 15.91,
        "volume_usd": 29802
    },
    {
        "Time": "2023-07-29T07:45:06.613726Z",
        "Price": 14987.48,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14987480,
        "volume_native": 15.91,
        "volume_usd": 29806
    },
    {
        "Time": "2023-07-29T08:00:06.252672Z",
        "Price": 14975.58,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14975578,
        "volume_native": 15.91,
        "volume_usd": 29783
    },
    {
        "Time": "2023-07-29T08:15:06.879332Z",
        "Price": 14981.04,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14981038,
        "volume_native": 15.91,
        "volume_usd": 29794
    },
    {
        "Time": "2023-07-29T08:30:06.70815Z",
        "Price": 14990.57,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14990574,
        "volume_native": 15.91,
        "volume_usd": 29813
    },
    {
        "Time": "2023-07-29T08:45:06.185457Z",
        "Price": 14980.83,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14980831,
        "volume_native": 15.91,
        "volume_usd": 29793
    },
    {
        "Time": "2023-07-29T09:00:06.46113Z",
        "Price": 14972.33,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14972334,
        "volume_native": 15.91,
        "volume_usd": 29776
    },
    {
        "Time": "2023-07-29T09:15:06.394167Z",
        "Price": 14972.88,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14972885,
        "volume_native": 15.91,
        "volume_usd": 29777
    },
    {
        "Time": "2023-07-29T09:30:05.923155Z",
        "Price": 14980.23,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14980233,
        "volume_native": 15.91,
        "volume_usd": 29792
    },
    {
        "Time": "2023-07-29T09:45:05.883513Z",
        "Price": 14973.92,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14973915,
        "volume_native": 15.91,
        "volume_usd": 29779
    },
    {
        "Time": "2023-07-29T10:00:05.883009Z",
        "Price": 14968.16,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14968163,
        "volume_native": 15.91,
        "volume_usd": 29768
    },
    {
        "Time": "2023-07-29T10:15:06.479297Z",
        "Price": 14970.53,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14970527,
        "volume_native": 7.3,
        "volume_usd": 13660.61
    },
    {
        "Time": "2023-07-29T10:30:05.891065Z",
        "Price": 14965.47,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14965467,
        "volume_native": 7.3,
        "volume_usd": 13655.99
    },
    {
        "Time": "2023-07-29T10:45:06.362851Z",
        "Price": 14969.42,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14969418,
        "volume_native": 7.3,
        "volume_usd": 13659.59
    },
    {
        "Time": "2023-07-29T11:00:06.466088Z",
        "Price": 14977.07,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14977069,
        "volume_native": 7.3,
        "volume_usd": 13666.58
    },
    {
        "Time": "2023-07-29T11:15:06.801116Z",
        "Price": 14965.03,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14965026,
        "volume_native": 7.3,
        "volume_usd": 13655.59
    },
    {
        "Time": "2023-07-29T11:30:06.365058Z",
        "Price": 14981.36,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14981360,
        "volume_native": 7.3,
        "volume_usd": 13670.49
    },
    {
        "Time": "2023-07-29T11:45:06.265827Z",
        "Price": 14974.57,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14974566,
        "volume_native": 7.3,
        "volume_usd": 13664.29
    },
    {
        "Time": "2023-07-29T12:00:06.539313Z",
        "Price": 14974.38,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14974378,
        "volume_native": 7.3,
        "volume_usd": 13664.12
    },
    {
        "Time": "2023-07-29T12:15:05.980581Z",
        "Price": 14967.8,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14967800,
        "volume_native": 7.3,
        "volume_usd": 13658.12
    },
    {
        "Time": "2023-07-29T12:30:05.950168Z",
        "Price": 14978.94,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14978938,
        "volume_native": 7.3,
        "volume_usd": 13668.28
    },
    {
        "Time": "2023-07-29T12:45:06.023167Z",
        "Price": 14988.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14988603,
        "volume_native": 7.3,
        "volume_usd": 13677.1
    },
    {
        "Time": "2023-07-29T13:00:06.048777Z",
        "Price": 14981.92,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14981919,
        "volume_native": 7.3,
        "volume_usd": 13671
    },
    {
        "Time": "2023-07-29T13:15:06.039795Z",
        "Price": 14994.69,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14994690,
        "volume_native": 7.3,
        "volume_usd": 13682.65
    },
    {
        "Time": "2023-07-29T13:30:06.207028Z",
        "Price": 14994.02,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14994016,
        "volume_native": 7.3,
        "volume_usd": 13682.04
    },
    {
        "Time": "2023-07-29T13:45:06.618908Z",
        "Price": 14989.04,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14989038,
        "volume_native": 7.3,
        "volume_usd": 13677.5
    },
    {
        "Time": "2023-07-29T14:00:05.586796Z",
        "Price": 14989.71,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14989709,
        "volume_native": 7.3,
        "volume_usd": 13678.11
    },
    {
        "Time": "2023-07-29T14:15:07.326155Z",
        "Price": 14986.92,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14986923,
        "volume_native": 7.3,
        "volume_usd": 13675.57
    },
    {
        "Time": "2023-07-29T14:30:05.981016Z",
        "Price": 14995.4,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14995398,
        "volume_native": 7.3,
        "volume_usd": 13683.3
    },
    {
        "Time": "2023-07-29T14:45:07.006661Z",
        "Price": 14987.93,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14987932,
        "volume_native": 7.3,
        "volume_usd": 13676.49
    },
    {
        "Time": "2023-07-29T15:00:06.6139Z",
        "Price": 14983.46,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14983456,
        "volume_native": 7.3,
        "volume_usd": 13672.4
    },
    {
        "Time": "2023-07-29T15:15:06.109598Z",
        "Price": 14982.96,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14982958,
        "volume_native": 7.3,
        "volume_usd": 13671.95
    },
    {
        "Time": "2023-07-29T15:30:06.147113Z",
        "Price": 14979.7,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14979701,
        "volume_native": 7.3,
        "volume_usd": 13668.98
    },
    {
        "Time": "2023-07-29T15:45:07.769959Z",
        "Price": 14978.89,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14978889,
        "volume_native": 7.3,
        "volume_usd": 13668.24
    },
    {
        "Time": "2023-07-29T16:00:07.140532Z",
        "Price": 14980.1,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14980097,
        "volume_native": 7.3,
        "volume_usd": 13669.34
    },
    {
        "Time": "2023-07-29T16:15:06.069202Z",
        "Price": 14977.77,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14977773,
        "volume_native": 7.3,
        "volume_usd": 13667.22
    },
    {
        "Time": "2023-07-29T16:30:06.089421Z",
        "Price": 14972.69,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14972694,
        "volume_native": 7.3,
        "volume_usd": 13662.58
    },
    {
        "Time": "2023-07-29T16:45:05.944134Z",
        "Price": 14978.43,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14978427,
        "volume_native": 7.3,
        "volume_usd": 13667.81
    },
    {
        "Time": "2023-07-29T17:00:06.719253Z",
        "Price": 14982.98,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14982983,
        "volume_native": 7.3,
        "volume_usd": 13671.97
    },
    {
        "Time": "2023-07-29T17:15:06.102375Z",
        "Price": 14987.18,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14987175,
        "volume_native": 7.3,
        "volume_usd": 13675.8
    },
    {
        "Time": "2023-07-29T17:30:05.939587Z",
        "Price": 14990.1,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14990101,
        "volume_native": 7.3,
        "volume_usd": 13678.47
    },
    {
        "Time": "2023-07-29T17:45:06.252977Z",
        "Price": 15000.09,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15000086,
        "volume_native": 7.3,
        "volume_usd": 13687.58
    },
    {
        "Time": "2023-07-29T18:00:06.842304Z",
        "Price": 15020.5,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15020496,
        "volume_native": 7.3,
        "volume_usd": 13706.2
    },
    {
        "Time": "2023-07-29T18:15:06.773394Z",
        "Price": 15011.56,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15011564,
        "volume_native": 7.3,
        "volume_usd": 13698.05
    },
    {
        "Time": "2023-07-29T18:30:06.215691Z",
        "Price": 14991.53,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14991531,
        "volume_native": 7.3,
        "volume_usd": 13679.77
    },
    {
        "Time": "2023-07-29T18:45:06.119004Z",
        "Price": 14999.96,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14999960,
        "volume_native": 7.3,
        "volume_usd": 13687.46
    },
    {
        "Time": "2023-07-29T19:00:05.951004Z",
        "Price": 14998.21,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14998214,
        "volume_native": 7.3,
        "volume_usd": 13685.87
    },
    {
        "Time": "2023-07-29T19:15:06.513781Z",
        "Price": 15018.1,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15018103,
        "volume_native": 7.3,
        "volume_usd": 13704.02
    },
    {
        "Time": "2023-07-29T19:30:06.349051Z",
        "Price": 15023.8,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15023797,
        "volume_native": 7.3,
        "volume_usd": 13709.22
    },
    {
        "Time": "2023-07-29T19:45:06.852776Z",
        "Price": 15020.69,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15020693,
        "volume_native": 7.3,
        "volume_usd": 13706.38
    },
    {
        "Time": "2023-07-29T20:00:06.418691Z",
        "Price": 15018.51,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15018511,
        "volume_native": 7.3,
        "volume_usd": 13704.39
    },
    {
        "Time": "2023-07-29T20:15:07.364851Z",
        "Price": 15031.59,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15031592,
        "volume_native": 7.3,
        "volume_usd": 13716.33
    },
    {
        "Time": "2023-07-29T20:30:07.397637Z",
        "Price": 15067.67,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15067674,
        "volume_native": 7.3,
        "volume_usd": 13749.25
    },
    {
        "Time": "2023-07-29T20:45:06.949565Z",
        "Price": 15069.29,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15069292,
        "volume_native": 7.3,
        "volume_usd": 13750.73
    },
    {
        "Time": "2023-07-29T21:00:08.655879Z",
        "Price": 15068.91,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15068907,
        "volume_native": 7.3,
        "volume_usd": 13750.38
    },
    {
        "Time": "2023-07-29T21:17:15.91821Z",
        "Price": 15085.08,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15085083,
        "volume_native": 7.3,
        "volume_usd": 13765.14
    },
    {
        "Time": "2023-07-29T21:30:08.767125Z",
        "Price": 15065.23,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15065228,
        "volume_native": 7.3,
        "volume_usd": 13747.02
    },
    {
        "Time": "2023-07-29T21:45:12.202884Z",
        "Price": 15057.76,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15057762,
        "volume_native": 7.3,
        "volume_usd": 13740.21
    },
    {
        "Time": "2023-07-29T22:00:09.77194Z",
        "Price": 15049.34,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15049336,
        "volume_native": 7.3,
        "volume_usd": 13732.52
    },
    {
        "Time": "2023-07-29T22:15:09.072045Z",
        "Price": 15054.82,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15054819,
        "volume_native": 7.3,
        "volume_usd": 13737.52
    },
    {
        "Time": "2023-07-29T22:30:17.925877Z",
        "Price": 15056.88,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15056882,
        "volume_native": 7.3,
        "volume_usd": 13739.4
    },
    {
        "Time": "2023-07-29T22:45:11.744314Z",
        "Price": 15046.86,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15046856,
        "volume_native": 7.3,
        "volume_usd": 13730.26
    },
    {
        "Time": "2023-07-29T23:00:11.366866Z",
        "Price": 15043.08,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15043080,
        "volume_native": 7.3,
        "volume_usd": 13726.81
    },
    {
        "Time": "2023-07-29T23:15:08.847765Z",
        "Price": 15038.62,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15038618,
        "volume_native": 7.3,
        "volume_usd": 13722.74
    },
    {
        "Time": "2023-07-29T23:30:11.684393Z",
        "Price": 15044.83,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15044832,
        "volume_native": 7.3,
        "volume_usd": 13728.41
    },
    {
        "Time": "2023-07-29T23:45:16.898Z",
        "Price": 15046.05,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15046052,
        "volume_native": 7.3,
        "volume_usd": 13729.52
    },
    {
        "Time": "2023-07-30T00:01:12.212905Z",
        "Price": 15047.92,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15047921,
        "volume_native": 7.3,
        "volume_usd": 13731.23
    },
    {
        "Time": "2023-07-30T00:05:54Z",
        "Price": 15049.043121936911,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15049043.121936915,
        "volume_native": 7.3,
        "volume_usd": 13732.251848767432
    },
    {
        "Time": "2023-07-30T00:05:54Z",
        "Price": 15049.043121936911,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15049043.121936915,
        "volume_native": 7.3,
        "volume_usd": 13732.251848767432
    },
    {
        "Time": "2023-07-30T00:05:54Z",
        "Price": 15049.043121936911,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15049043.121936915,
        "volume_native": 7.3,
        "volume_usd": 13732.251848767432
    },
    {
        "Time": "2023-07-30T00:15:11.269701Z",
        "Price": 15045.95,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15045950,
        "volume_native": 7.3,
        "volume_usd": 13729.43
    },
    {
        "Time": "2023-07-30T00:30:20.655381Z",
        "Price": 15034.65,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15034651,
        "volume_native": 7.3,
        "volume_usd": 13719.12
    },
    {
        "Time": "2023-07-30T00:45:13.695082Z",
        "Price": 15034.98,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15034983,
        "volume_native": 7.3,
        "volume_usd": 13719.42
    },
    {
        "Time": "2023-07-30T01:01:03.423821Z",
        "Price": 15037.19,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15037185,
        "volume_native": 7.3,
        "volume_usd": 13721.43
    },
    {
        "Time": "2023-07-30T01:17:08.410748Z",
        "Price": 15042.84,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15042843,
        "volume_native": 7.3,
        "volume_usd": 13726.59
    },
    {
        "Time": "2023-07-30T01:31:04.061418Z",
        "Price": 15036.13,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15036133,
        "volume_native": 7.3,
        "volume_usd": 13720.47
    },
    {
        "Time": "2023-07-30T01:46:05.768374Z",
        "Price": 15031.41,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15031411,
        "volume_native": 7.3,
        "volume_usd": 13716.16
    },
    {
        "Time": "2023-07-30T02:00:37.441915Z",
        "Price": 15026.06,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15026059,
        "volume_native": 7.3,
        "volume_usd": 13711.28
    },
    {
        "Time": "2023-07-30T02:15:06.756525Z",
        "Price": 15027.78,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15027781,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T02:30:08.496717Z",
        "Price": 15027.78,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15027781,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T02:45:06.412916Z",
        "Price": 15020.46,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15020459,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T03:00:20.256476Z",
        "Price": 15020.46,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15020459,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T03:15:25.034932Z",
        "Price": 15026.89,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15026891,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T03:30:20.785869Z",
        "Price": 15026.89,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15026891,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T03:45:20.584572Z",
        "Price": 15032.4,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15032399,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T04:01:41.805098Z",
        "Price": 15032.4,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15032399,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T04:16:47.21178Z",
        "Price": 15026.62,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15026622,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T04:31:42.936617Z",
        "Price": 15026.62,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15026622,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T04:46:44.935153Z",
        "Price": 15031.87,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15031865,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T05:02:37.222792Z",
        "Price": 15031.87,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15031865,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T05:16:10.28879Z",
        "Price": 15029.79,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15029792,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T05:31:11.590904Z",
        "Price": 15029.79,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15029792,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T05:46:11.77579Z",
        "Price": 15014.44,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15014440,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T06:00:06.48436Z",
        "Price": 15014.44,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15014440,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T06:15:07.768828Z",
        "Price": 15011.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15011599,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T06:30:06.882234Z",
        "Price": 15011.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15011599,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T06:45:06.868115Z",
        "Price": 14994.43,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14994433,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T07:00:07.529028Z",
        "Price": 14994.43,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14994433,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T07:15:08.775304Z",
        "Price": 15001.06,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15001064,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T07:31:14.937593Z",
        "Price": 15001.06,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15001064,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T07:46:14.878958Z",
        "Price": 14995.76,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14995758,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T08:00:09.283139Z",
        "Price": 14995.76,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14995758,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T08:15:07.98464Z",
        "Price": 15002.85,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15002851,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T08:30:06.768613Z",
        "Price": 15002.85,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15002851,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T08:45:06.708435Z",
        "Price": 14998.19,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14998191,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T09:00:06.606673Z",
        "Price": 14998.19,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14998191,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T09:15:07.64476Z",
        "Price": 15000.55,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15000548,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T09:30:06.893108Z",
        "Price": 15000.55,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15000548,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T09:45:06.516185Z",
        "Price": 14987.38,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14987381,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T10:00:06.580933Z",
        "Price": 14987.38,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14987381,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T10:15:07.186588Z",
        "Price": 14989.82,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14989821,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T10:30:06.259028Z",
        "Price": 14989.82,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14989821,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T10:45:05.98409Z",
        "Price": 15000.74,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15000737,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T11:00:06.842805Z",
        "Price": 15000.74,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15000737,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T11:15:07.088954Z",
        "Price": 14986.24,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14986239,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T11:30:05.941064Z",
        "Price": 14986.24,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14986239,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T11:45:07.054753Z",
        "Price": 15008.61,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15008608,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T12:00:05.960716Z",
        "Price": 15008.61,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15008608,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T12:15:06.176049Z",
        "Price": 15008.94,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15008941,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T12:30:06.638716Z",
        "Price": 15008.94,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15008941,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T12:45:06.66807Z",
        "Price": 15011.91,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15011909,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T13:00:07.83002Z",
        "Price": 15011.91,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15011909,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T13:15:07.124977Z",
        "Price": 15012.66,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15012665,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T13:30:06.462326Z",
        "Price": 15012.66,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15012665,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T13:45:07.113867Z",
        "Price": 15001.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15001597,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T14:00:05.804569Z",
        "Price": 15001.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15001597,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T14:15:08.643317Z",
        "Price": 14980.43,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14980429,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T14:30:07.3845Z",
        "Price": 14980.43,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14980429,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T14:45:05.898756Z",
        "Price": 15005.3,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15005303,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T15:00:06.749081Z",
        "Price": 15005.3,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15005303,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T15:15:06.944327Z",
        "Price": 15054.61,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15054606,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T15:30:06.966261Z",
        "Price": 15054.61,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15054606,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T15:45:06.586516Z",
        "Price": 15057.37,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15057373,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T16:00:07.337077Z",
        "Price": 15057.37,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15057373,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T16:15:06.247408Z",
        "Price": 15025.15,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15025150,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T16:30:06.763661Z",
        "Price": 15025.15,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15025150,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T16:45:06.728582Z",
        "Price": 15007.71,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15007715,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T17:00:07.37814Z",
        "Price": 15007.71,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15007715,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T17:15:06.225842Z",
        "Price": 15012.08,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15012082,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T17:30:07.008548Z",
        "Price": 15012.08,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15012082,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T17:45:06.412989Z",
        "Price": 15010.77,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15010768,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T18:00:07.113259Z",
        "Price": 15010.77,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15010768,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T18:15:06.371954Z",
        "Price": 15024.95,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15024951,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T18:30:06.667516Z",
        "Price": 15024.95,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15024951,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T18:45:06.618266Z",
        "Price": 15012.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15012597,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T19:00:06.924237Z",
        "Price": 15012.6,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15012597,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T19:15:08.626315Z",
        "Price": 15015.23,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15015226,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T19:30:07.355171Z",
        "Price": 15015.23,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15015226,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T19:45:07.278214Z",
        "Price": 15021.36,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15021358,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T20:00:06.751617Z",
        "Price": 15021.36,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 15021358,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T20:15:06.808642Z",
        "Price": 14877.66,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14877661,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T20:30:07.383411Z",
        "Price": 14877.66,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14877661,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T20:45:06.096619Z",
        "Price": 14855.15,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14855149,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T21:00:07.108239Z",
        "Price": 14855.15,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14855149,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T21:15:06.27003Z",
        "Price": 14896.85,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14896854,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T21:30:07.037875Z",
        "Price": 14896.85,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14896854,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T21:45:06.61864Z",
        "Price": 14871.52,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14871521,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T22:00:07.80456Z",
        "Price": 14871.52,
        "floorprice_usd": 8,
        "marketCap_native": 8000,
        "marketCap_usd": 14871521,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T22:15:07.995067Z",
        "Price": 14690.42,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14690422,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T22:30:06.528148Z",
        "Price": 14690.42,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14690422,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T22:45:06.540069Z",
        "Price": 14674.09,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14674094,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T23:00:07.445469Z",
        "Price": 14674.09,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14674094,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T23:15:06.075976Z",
        "Price": 14665.62,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14665620,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T23:30:06.270934Z",
        "Price": 14665.62,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14665620,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-30T23:45:06.161496Z",
        "Price": 14723.32,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14723320,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T00:00:06.712554Z",
        "Price": 14723.32,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14723320,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T00:05:38Z",
        "Price": 14723.320284163148,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14723320.28416315,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T00:05:38Z",
        "Price": 14723.320284163148,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14723320.28416315,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T00:05:38Z",
        "Price": 14723.320284163148,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14723320.28416315,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T00:15:07.594093Z",
        "Price": 14707.61,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14707610,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T00:30:07.203845Z",
        "Price": 14707.61,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14707610,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T00:45:06.119946Z",
        "Price": 14717.33,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14717329,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T01:00:07.285873Z",
        "Price": 14717.33,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14717329,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T01:15:05.866982Z",
        "Price": 14786.73,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14786725,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T01:30:08.676419Z",
        "Price": 14786.73,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14786725,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T01:45:06.37373Z",
        "Price": 14771.25,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14771254,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T02:00:06.106552Z",
        "Price": 14771.25,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14771254,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T02:15:07.596028Z",
        "Price": 14786.25,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14786253,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T02:30:06.94384Z",
        "Price": 14786.25,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14786253,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T02:45:07.42805Z",
        "Price": 14806.88,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14806884,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T03:00:19.996368Z",
        "Price": 14806.88,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14806884,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T03:15:21.172173Z",
        "Price": 14776.22,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14776219,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T03:30:20.198054Z",
        "Price": 14776.22,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14776219,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T03:45:19.964408Z",
        "Price": 14768.87,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14768866,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T04:01:52.362941Z",
        "Price": 14768.87,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14768866,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T04:15:21.64588Z",
        "Price": 14761.62,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14761623,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T04:30:21.247761Z",
        "Price": 14761.62,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14761623,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T04:45:22.009152Z",
        "Price": 14750.16,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14750155,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T05:01:26.33897Z",
        "Price": 14750.16,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14750155,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T05:15:08.113573Z",
        "Price": 14728.19,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14728193,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T05:30:07.27038Z",
        "Price": 14728.19,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14728193,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T05:45:07.250519Z",
        "Price": 14732.06,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14732060,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T06:00:08.480498Z",
        "Price": 14732.06,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14732060,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T06:15:06.43381Z",
        "Price": 14722.67,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14722671,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T06:30:06.551265Z",
        "Price": 14722.67,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14722671,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T06:45:07.418776Z",
        "Price": 14734.54,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14734537,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T07:00:06.822099Z",
        "Price": 14734.54,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14734537,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T07:15:08.628934Z",
        "Price": 14745.29,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14745288,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T07:30:08.314153Z",
        "Price": 14745.29,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14745288,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T07:45:09.70005Z",
        "Price": 14748.84,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14748838,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T08:00:06.495551Z",
        "Price": 14748.84,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14748838,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T08:15:05.961074Z",
        "Price": 14735.09,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14735089,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T08:30:08.717725Z",
        "Price": 14735.09,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14735089,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T08:45:07.053706Z",
        "Price": 14751.75,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14751752,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T09:00:07.147625Z",
        "Price": 14751.75,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14751752,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T09:15:06.539189Z",
        "Price": 14756.66,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14756660,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T09:30:06.712582Z",
        "Price": 14756.66,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14756660,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T09:45:08.390653Z",
        "Price": 14742.58,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14742582,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T10:00:06.979361Z",
        "Price": 14742.58,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14742582,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T10:15:07.947056Z",
        "Price": 14757.04,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14757042,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T10:30:08.635099Z",
        "Price": 14757.04,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14757042,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T10:45:07.194898Z",
        "Price": 14766.59,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14766588,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T11:00:10.161944Z",
        "Price": 14766.59,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14766588,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T11:15:06.4069Z",
        "Price": 14752.34,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14752338,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T11:30:06.513439Z",
        "Price": 14752.34,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14752338,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T11:45:06.608248Z",
        "Price": 14758.49,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14758486,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T12:00:07.050329Z",
        "Price": 14758.49,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14758486,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T12:15:08.677734Z",
        "Price": 14751.93,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14751927,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T12:31:08.556345Z",
        "Price": 14751.93,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14751927,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T12:45:53.117814Z",
        "Price": 14752.55,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14752553,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T13:00:06.469962Z",
        "Price": 14752.55,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14752553,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T13:15:07.063816Z",
        "Price": 14788.99,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14788986,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T13:30:06.359287Z",
        "Price": 14788.99,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14788986,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T13:45:08.622625Z",
        "Price": 14778.38,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14778384,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T14:00:06.074536Z",
        "Price": 14778.38,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14778384,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T14:15:08.539986Z",
        "Price": 14735.51,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14735513,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T14:30:06.914314Z",
        "Price": 14735.51,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14735513,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T14:45:06.61261Z",
        "Price": 14700.98,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14700979,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T15:00:07.169761Z",
        "Price": 14700.98,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14700979,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T15:15:05.95296Z",
        "Price": 14703.66,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14703662,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T15:30:06.188017Z",
        "Price": 14703.66,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14703662,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T15:45:06.460962Z",
        "Price": 14683.18,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14683179,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T16:00:08.569249Z",
        "Price": 14683.18,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14683179,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T16:15:09.430041Z",
        "Price": 14675.87,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14675873,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T16:30:06.87308Z",
        "Price": 14675.87,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14675873,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T16:45:07.180924Z",
        "Price": 14676.12,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14676117,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T17:00:06.898755Z",
        "Price": 14676.12,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14676117,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T17:15:08.626852Z",
        "Price": 14697.7,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14697700,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T17:30:06.685692Z",
        "Price": 14697.7,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14697700,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T17:45:06.742777Z",
        "Price": 14673.31,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14673308,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T18:00:06.454196Z",
        "Price": 14673.31,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14673308,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T18:30:06.963984Z",
        "Price": 14703.62,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14703623,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T18:45:06.615844Z",
        "Price": 14676.73,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14676734,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T19:00:08.769403Z",
        "Price": 14676.73,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14676734,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T19:15:06.707898Z",
        "Price": 14699.24,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14699239,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T19:30:06.414579Z",
        "Price": 14699.24,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14699239,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T19:45:06.066703Z",
        "Price": 14698.48,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14698481,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T20:00:05.414058Z",
        "Price": 14698.48,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14698481,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T20:15:06.522733Z",
        "Price": 14682.99,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14682986,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T20:30:06.892298Z",
        "Price": 14682.99,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14682986,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T20:45:07.850887Z",
        "Price": 14689.14,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14689136,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T21:00:07.10643Z",
        "Price": 14689.14,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14689136,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T21:15:05.813922Z",
        "Price": 14650.86,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14650856,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T21:30:08.291122Z",
        "Price": 14650.86,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14650856,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T21:45:06.405816Z",
        "Price": 14668.04,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14668039,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T22:00:07.515483Z",
        "Price": 14668.04,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14668039,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T22:15:06.974978Z",
        "Price": 14657.47,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14657469,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T22:30:07.56163Z",
        "Price": 14657.47,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14657469,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T22:45:09.026755Z",
        "Price": 14667.2,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14667202,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T23:00:06.949423Z",
        "Price": 14667.2,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14667202,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T23:15:06.78348Z",
        "Price": 14658.09,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14658087,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T23:30:06.779749Z",
        "Price": 14658.09,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14658087,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-07-31T23:45:06.98002Z",
        "Price": 14660.83,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14660829,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T00:00:07.725445Z",
        "Price": 14660.83,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14660829,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T00:05:14Z",
        "Price": 14660.007530654306,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14660007.530654304,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T00:05:14Z",
        "Price": 14660.007530654306,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14660007.530654304,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T00:15:08.264347Z",
        "Price": 14660.01,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14660008,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T00:30:06.69581Z",
        "Price": 14660.01,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14660008,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T00:45:07.005425Z",
        "Price": 14676.72,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14676720,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T01:00:06.712947Z",
        "Price": 14676.72,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14676720,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T01:15:07.039177Z",
        "Price": 14708.29,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14708292,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T01:30:07.860476Z",
        "Price": 14708.29,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14708292,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T01:45:06.844961Z",
        "Price": 14691.42,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14691415,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T02:00:06.165334Z",
        "Price": 14691.42,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14691415,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T02:15:09.015009Z",
        "Price": 14635.61,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14635608,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T02:30:06.690841Z",
        "Price": 14635.61,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14635608,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T02:45:07.138598Z",
        "Price": 14456.53,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14456528,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T03:00:20.140206Z",
        "Price": 14456.53,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14456528,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T03:15:21.378476Z",
        "Price": 14438.26,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14438258,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T03:30:20.666878Z",
        "Price": 14438.26,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14438258,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T03:45:20.554462Z",
        "Price": 14441.98,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14441980,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T04:15:20.308151Z",
        "Price": 14423.53,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14423530,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T04:30:20.560764Z",
        "Price": 14423.53,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14423530,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T04:45:21.328184Z",
        "Price": 14408.02,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14408024,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T05:01:25.053043Z",
        "Price": 14408.02,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14408024,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T05:15:08.661858Z",
        "Price": 14425.38,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14425383,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T05:30:09.712087Z",
        "Price": 14425.38,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14425383,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T05:45:07.024683Z",
        "Price": 14421.12,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14421123,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T06:00:07.591294Z",
        "Price": 14421.12,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14421123,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T06:15:22.150756Z",
        "Price": 14450.29,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14450292,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T06:31:44.951805Z",
        "Price": 14450.29,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14450292,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T06:45:11.517377Z",
        "Price": 14463.19,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14463191,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T07:00:16.632876Z",
        "Price": 14463.19,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14463191,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T07:15:09.493852Z",
        "Price": 14466.84,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14466841,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T07:30:08.49633Z",
        "Price": 14466.84,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14466841,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T07:45:08.159865Z",
        "Price": 14465.55,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14465551,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T08:00:06.107908Z",
        "Price": 14465.55,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14465551,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T08:15:06.300167Z",
        "Price": 14484.61,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14484605,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T08:30:07.122151Z",
        "Price": 14484.61,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14484605,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T08:45:06.553624Z",
        "Price": 14502.63,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14502634,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T09:00:08.862449Z",
        "Price": 14502.63,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14502634,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T09:15:09.241317Z",
        "Price": 14495.62,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14495623,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T09:30:08.407865Z",
        "Price": 14495.62,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14495623,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T09:45:08.712573Z",
        "Price": 14483.93,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14483925,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T10:00:08.940945Z",
        "Price": 14483.93,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14483925,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T10:15:07.902234Z",
        "Price": 14475.1,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14475099,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T10:30:07.747131Z",
        "Price": 14475.1,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14475099,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T10:45:08.281862Z",
        "Price": 14471.37,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14471367,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T11:00:08.186562Z",
        "Price": 14471.37,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14471367,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T11:15:08.21362Z",
        "Price": 14486.62,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14486621,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T11:30:07.002903Z",
        "Price": 14486.62,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14486621,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T11:45:08.45565Z",
        "Price": 14477.92,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14477920,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T12:00:05.874965Z",
        "Price": 14477.92,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14477920,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T12:15:10.201666Z",
        "Price": 14456.69,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14456686,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T12:30:07.085223Z",
        "Price": 14456.69,
        "floorprice_usd": 7.9,
        "marketCap_native": 7900,
        "marketCap_usd": 14456686,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T12:45:08.456806Z",
        "Price": 13709.93,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13709927,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T12:50:14.382507Z",
        "Price": 13709.93,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13709927,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T12:56:16.54528Z",
        "Price": 13709.93,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13709927,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T12:57:37.941367Z",
        "Price": 13709.93,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13709927,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T13:00:06.750528Z",
        "Price": 13709.93,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13709927,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T13:15:06.909649Z",
        "Price": 13737.55,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13737553,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T13:30:07.142582Z",
        "Price": 13737.55,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13737553,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T13:45:07.388142Z",
        "Price": 13751.81,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13751815,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T14:00:06.932369Z",
        "Price": 13751.81,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13751815,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T14:15:07.619027Z",
        "Price": 13753.62,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13753616,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T14:30:07.957925Z",
        "Price": 13753.62,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13753616,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T14:45:07.155094Z",
        "Price": 13742.7,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13742703,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T15:00:08.587372Z",
        "Price": 13742.7,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13742703,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T15:15:06.812786Z",
        "Price": 13715.01,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13715012,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T15:30:06.498779Z",
        "Price": 13715.01,
        "floorprice_usd": 7.5,
        "marketCap_native": 7500,
        "marketCap_usd": 13715012,
        "volume_native": 0,
        "volume_usd": 0
    },
    {
        "Time": "2023-08-01T15:45:07.013618Z",
        "Price": 13353.97,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13353967,
        "volume_native": 6.71,
        "volume_usd": 12275.04
    },
    {
        "Time": "2023-08-01T16:00:08.882039Z",
        "Price": 13365.4,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13365400,
        "volume_native": 6.71,
        "volume_usd": 12285.55
    },
    {
        "Time": "2023-08-01T16:15:06.610584Z",
        "Price": 13415.98,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13415980,
        "volume_native": 6.71,
        "volume_usd": 12332.04
    },
    {
        "Time": "2023-08-01T16:30:07.385616Z",
        "Price": 13365.4,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13365400,
        "volume_native": 6.71,
        "volume_usd": 12285.55
    },
    {
        "Time": "2023-08-01T16:45:07.174539Z",
        "Price": 13372,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13371996,
        "volume_native": 6.71,
        "volume_usd": 12291.61
    },
    {
        "Time": "2023-08-01T17:00:07.215879Z",
        "Price": 13372.12,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13372122,
        "volume_native": 6.71,
        "volume_usd": 12291.73
    },
    {
        "Time": "2023-08-01T17:15:06.911402Z",
        "Price": 13386.08,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13386084,
        "volume_native": 6.71,
        "volume_usd": 12304.56
    },
    {
        "Time": "2023-08-01T17:30:09.333172Z",
        "Price": 13404.84,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13404844,
        "volume_native": 6.71,
        "volume_usd": 12321.81
    },
    {
        "Time": "2023-08-01T17:45:06.755078Z",
        "Price": 13391.64,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13391639,
        "volume_native": 6.71,
        "volume_usd": 12309.67
    },
    {
        "Time": "2023-08-01T18:00:08.406748Z",
        "Price": 13400.74,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13400745,
        "volume_native": 6.71,
        "volume_usd": 12318.04
    },
    {
        "Time": "2023-08-01T18:15:06.194229Z",
        "Price": 13420.54,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13420540,
        "volume_native": 6.71,
        "volume_usd": 12336.23
    },
    {
        "Time": "2023-08-01T18:30:07.404117Z",
        "Price": 13430.74,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13430743,
        "volume_native": 6.71,
        "volume_usd": 12345.61
    },
    {
        "Time": "2023-08-01T18:45:06.671055Z",
        "Price": 13467.11,
        "floorprice_usd": 7.3,
        "marketCap_native": 7300,
        "marketCap_usd": 13467113,
        "volume_native": 6.71,
        "volume_usd": 12379.04
    },
    {
        "Time": "2023-08-01T19:00:08.21362Z",
        "Price": 13480.26,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13480259,
        "volume_native": 6.71,
        "volume_usd": 12392.83
    },
    {
        "Time": "2023-08-01T19:15:06.729041Z",
        "Price": 13510.89,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13510893,
        "volume_native": 6.71,
        "volume_usd": 12420.99
    },
    {
        "Time": "2023-08-01T19:30:06.885537Z",
        "Price": 13507.71,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13507715,
        "volume_native": 6.71,
        "volume_usd": 12418.07
    },
    {
        "Time": "2023-08-01T19:45:07.805971Z",
        "Price": 13496.13,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13496133,
        "volume_native": 6.71,
        "volume_usd": 12407.42
    },
    {
        "Time": "2023-08-01T20:00:06.548873Z",
        "Price": 13502.11,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13502105,
        "volume_native": 6.71,
        "volume_usd": 12412.91
    },
    {
        "Time": "2023-08-01T20:15:06.411806Z",
        "Price": 13506.1,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13506099,
        "volume_native": 6.71,
        "volume_usd": 12416.58
    },
    {
        "Time": "2023-08-01T20:30:07.263082Z",
        "Price": 13502.77,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13502775,
        "volume_native": 6.71,
        "volume_usd": 12413.53
    },
    {
        "Time": "2023-08-01T20:45:07.208449Z",
        "Price": 13487.5,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13487497,
        "volume_native": 6.71,
        "volume_usd": 12399.48
    },
    {
        "Time": "2023-08-01T21:00:07.157457Z",
        "Price": 13501.65,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13501654,
        "volume_native": 6.71,
        "volume_usd": 12412.49
    },
    {
        "Time": "2023-08-01T21:15:06.576119Z",
        "Price": 13494.4,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13494402,
        "volume_native": 6.71,
        "volume_usd": 12405.83
    },
    {
        "Time": "2023-08-01T21:30:06.483353Z",
        "Price": 13508.42,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13508425,
        "volume_native": 6.71,
        "volume_usd": 12418.72
    },
    {
        "Time": "2023-08-01T21:45:06.58528Z",
        "Price": 13501.1,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13501105,
        "volume_native": 6.71,
        "volume_usd": 12411.99
    },
    {
        "Time": "2023-08-01T22:00:07.501001Z",
        "Price": 13488.51,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13488514,
        "volume_native": 6.71,
        "volume_usd": 12400.41
    },
    {
        "Time": "2023-08-01T22:15:06.939042Z",
        "Price": 13485.64,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13485640,
        "volume_native": 6.71,
        "volume_usd": 12397.77
    },
    {
        "Time": "2023-08-01T22:30:06.870906Z",
        "Price": 13491.9,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13491898,
        "volume_native": 6.71,
        "volume_usd": 12403.53
    },
    {
        "Time": "2023-08-01T22:45:07.574668Z",
        "Price": 13489.06,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13489057,
        "volume_native": 6.71,
        "volume_usd": 12400.91
    },
    {
        "Time": "2023-08-01T23:00:07.972125Z",
        "Price": 13493.81,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13493813,
        "volume_native": 6.71,
        "volume_usd": 12405.29
    },
    {
        "Time": "2023-08-01T23:15:07.229436Z",
        "Price": 13538.96,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13538962,
        "volume_native": 6.71,
        "volume_usd": 12446.79
    },
    {
        "Time": "2023-08-01T23:30:07.366881Z",
        "Price": 13579.02,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13579022,
        "volume_native": 6.71,
        "volume_usd": 12483.62
    },
    {
        "Time": "2023-08-01T23:45:07.444425Z",
        "Price": 13634.89,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13634895,
        "volume_native": 6.71,
        "volume_usd": 12534.99
    },
    {
        "Time": "2023-08-02T00:00:07.171037Z",
        "Price": 13642.22,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13642221,
        "volume_native": 6.71,
        "volume_usd": 12541.72
    },
    {
        "Time": "2023-08-02T00:05:14Z",
        "Price": 13699.052209166477,
        "floorprice_usd": 7.299,
        "marketCap_native": 7299,
        "marketCap_usd": 13699052.209166475,
        "volume_native": 6.7102,
        "volume_usd": 12593.96905520604
    },
    {
        "Time": "2023-08-02T00:05:14Z",
        "Price": 13699.052209166477,
        "floorprice_usd": 7.299,
        "marketCap_native": 7299,
        "marketCap_usd": 13699052.209166475,
        "volume_native": 6.7102,
        "volume_usd": 12593.96905520604
    },
    {
        "Time": "2023-08-02T00:15:07.215604Z",
        "Price": 13682.23,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13682233,
        "volume_native": 6.71,
        "volume_usd": 12578.51
    },
    {
        "Time": "2023-08-02T00:30:07.333539Z",
        "Price": 13675.68,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13675683,
        "volume_native": 6.71,
        "volume_usd": 12572.48
    },
    {
        "Time": "2023-08-02T00:45:07.16817Z",
        "Price": 13640.65,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13640654,
        "volume_native": 6.71,
        "volume_usd": 12540.28
    },
    {
        "Time": "2023-08-02T01:00:06.97843Z",
        "Price": 13642.13,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13642130,
        "volume_native": 6.71,
        "volume_usd": 12541.64
    },
    {
        "Time": "2023-08-02T01:15:06.374621Z",
        "Price": 13653.4,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13653399,
        "volume_native": 6.71,
        "volume_usd": 12552
    },
    {
        "Time": "2023-08-02T01:30:07.602662Z",
        "Price": 13689.23,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13689232,
        "volume_native": 6.71,
        "volume_usd": 12584.94
    },
    {
        "Time": "2023-08-02T01:45:06.555508Z",
        "Price": 13681.05,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13681047,
        "volume_native": 6.71,
        "volume_usd": 12577.42
    },
    {
        "Time": "2023-08-02T02:00:06.196584Z",
        "Price": 13660.22,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13660219,
        "volume_native": 6.71,
        "volume_usd": 12558.27
    },
    {
        "Time": "2023-08-02T02:15:07.103193Z",
        "Price": 13654.05,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13654053,
        "volume_native": 6.71,
        "volume_usd": 12552.6
    },
    {
        "Time": "2023-08-02T02:30:06.40066Z",
        "Price": 13634.32,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13634317,
        "volume_native": 6.71,
        "volume_usd": 12534.46
    },
    {
        "Time": "2023-08-02T02:45:06.58573Z",
        "Price": 13643.81,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13643813,
        "volume_native": 6.71,
        "volume_usd": 12543.19
    },
    {
        "Time": "2023-08-02T03:00:11.015131Z",
        "Price": 13633.87,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13633866,
        "volume_native": 6.71,
        "volume_usd": 12534.04
    },
    {
        "Time": "2023-08-02T03:15:20.818413Z",
        "Price": 13609.08,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13609076,
        "volume_native": 6.71,
        "volume_usd": 12511.25
    },
    {
        "Time": "2023-08-02T03:30:57.60784Z",
        "Price": 13605.84,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13605838,
        "volume_native": 6.71,
        "volume_usd": 12508.27
    },
    {
        "Time": "2023-08-02T03:45:20.97742Z",
        "Price": 13602.43,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13602431,
        "volume_native": 6.71,
        "volume_usd": 12505.14
    },
    {
        "Time": "2023-08-02T04:01:51.692281Z",
        "Price": 13584.62,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13584625,
        "volume_native": 6.71,
        "volume_usd": 12488.77
    },
    {
        "Time": "2023-08-02T04:15:22.033446Z",
        "Price": 13590.21,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13590208,
        "volume_native": 6.71,
        "volume_usd": 12493.9
    },
    {
        "Time": "2023-08-02T04:30:23.984339Z",
        "Price": 13594.01,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13594012,
        "volume_native": 6.71,
        "volume_usd": 12497.4
    },
    {
        "Time": "2023-08-02T04:45:21.543752Z",
        "Price": 13606.44,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13606443,
        "volume_native": 6.71,
        "volume_usd": 12508.83
    },
    {
        "Time": "2023-08-02T05:01:21.453415Z",
        "Price": 13600.9,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13600901,
        "volume_native": 6.71,
        "volume_usd": 12503.74
    },
    {
        "Time": "2023-08-02T05:15:08.933331Z",
        "Price": 13582.17,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13582174,
        "volume_native": 6.71,
        "volume_usd": 12486.52
    },
    {
        "Time": "2023-08-02T05:30:08.259637Z",
        "Price": 13583.32,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13583318,
        "volume_native": 6.71,
        "volume_usd": 12487.57
    },
    {
        "Time": "2023-08-02T05:45:06.06822Z",
        "Price": 13574.5,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13574495,
        "volume_native": 6.71,
        "volume_usd": 12479.46
    },
    {
        "Time": "2023-08-02T06:00:08.459422Z",
        "Price": 13585.04,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13585042,
        "volume_native": 6.71,
        "volume_usd": 12489.16
    },
    {
        "Time": "2023-08-02T06:15:07.942938Z",
        "Price": 13597.16,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13597163,
        "volume_native": 6.71,
        "volume_usd": 12500.3
    },
    {
        "Time": "2023-08-02T06:30:06.939507Z",
        "Price": 13580.24,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13580243,
        "volume_native": 6.71,
        "volume_usd": 12484.74
    },
    {
        "Time": "2023-08-02T06:45:08.878565Z",
        "Price": 13576.17,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13576166,
        "volume_native": 6.71,
        "volume_usd": 12481
    },
    {
        "Time": "2023-08-02T07:00:06.856321Z",
        "Price": 13568.52,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13568523,
        "volume_native": 6.71,
        "volume_usd": 12473.97
    },
    {
        "Time": "2023-08-02T07:15:06.152568Z",
        "Price": 13562.4,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13562399,
        "volume_native": 6.71,
        "volume_usd": 12468.34
    },
    {
        "Time": "2023-08-02T07:30:06.822429Z",
        "Price": 13573.99,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13573985,
        "volume_native": 6.71,
        "volume_usd": 12478.99
    },
    {
        "Time": "2023-08-02T07:45:06.191245Z",
        "Price": 13561.96,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13561962,
        "volume_native": 6.71,
        "volume_usd": 12467.94
    },
    {
        "Time": "2023-08-02T08:00:05.50133Z",
        "Price": 13549.11,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13549107,
        "volume_native": 6.71,
        "volume_usd": 12456.12
    },
    {
        "Time": "2023-08-02T08:15:06.212974Z",
        "Price": 13546.37,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13546373,
        "volume_native": 6.71,
        "volume_usd": 12453.61
    },
    {
        "Time": "2023-08-02T08:30:07.163714Z",
        "Price": 13535.59,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13535592,
        "volume_native": 6.71,
        "volume_usd": 12443.69
    },
    {
        "Time": "2023-08-02T08:45:07.585957Z",
        "Price": 13551.12,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13551117,
        "volume_native": 6.71,
        "volume_usd": 12457.97
    },
    {
        "Time": "2023-08-02T09:00:06.882411Z",
        "Price": 13551.83,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13551826,
        "volume_native": 6.71,
        "volume_usd": 12458.62
    },
    {
        "Time": "2023-08-02T09:15:05.948679Z",
        "Price": 13565.02,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13565016,
        "volume_native": 6.71,
        "volume_usd": 12470.74
    },
    {
        "Time": "2023-08-02T09:30:06.59995Z",
        "Price": 13565.92,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13565924,
        "volume_native": 6.71,
        "volume_usd": 12471.58
    },
    {
        "Time": "2023-08-02T09:45:06.638887Z",
        "Price": 13546.52,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13546523,
        "volume_native": 6.71,
        "volume_usd": 12453.74
    },
    {
        "Time": "2023-08-02T10:00:07.015119Z",
        "Price": 13536.36,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13536361,
        "volume_native": 6.71,
        "volume_usd": 12444.4
    },
    {
        "Time": "2023-08-02T10:15:06.604432Z",
        "Price": 13532.23,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13532230,
        "volume_native": 6.71,
        "volume_usd": 12440.6
    },
    {
        "Time": "2023-08-02T10:30:07.302096Z",
        "Price": 13535.63,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13535633,
        "volume_native": 6.71,
        "volume_usd": 12443.73
    },
    {
        "Time": "2023-08-02T10:45:06.60205Z",
        "Price": 13549.72,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13549715,
        "volume_native": 6.71,
        "volume_usd": 12456.68
    },
    {
        "Time": "2023-08-02T11:00:06.84228Z",
        "Price": 13548.24,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13548243,
        "volume_native": 6.71,
        "volume_usd": 12455.33
    },
    {
        "Time": "2023-08-02T11:15:07.833968Z",
        "Price": 13552.31,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13552314,
        "volume_native": 6.71,
        "volume_usd": 12459.07
    },
    {
        "Time": "2023-08-02T11:30:06.153589Z",
        "Price": 13559.2,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13559197,
        "volume_native": 6.71,
        "volume_usd": 12465.4
    },
    {
        "Time": "2023-08-02T11:45:08.621981Z",
        "Price": 13569.01,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13569010,
        "volume_native": 6.71,
        "volume_usd": 12474.42
    },
    {
        "Time": "2023-08-02T12:00:06.552335Z",
        "Price": 13575.69,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13575690,
        "volume_native": 6.71,
        "volume_usd": 12480.56
    },
    {
        "Time": "2023-08-02T12:15:06.331295Z",
        "Price": 13566.2,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13566198,
        "volume_native": 6.71,
        "volume_usd": 12471.83
    },
    {
        "Time": "2023-08-02T12:30:06.767743Z",
        "Price": 13548.62,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13548624,
        "volume_native": 6.71,
        "volume_usd": 12455.68
    },
    {
        "Time": "2023-08-02T12:45:08.961398Z",
        "Price": 13537.15,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13537146,
        "volume_native": 6.71,
        "volume_usd": 12445.12
    },
    {
        "Time": "2023-08-02T13:00:07.235833Z",
        "Price": 13531.8,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13531801,
        "volume_native": 6.71,
        "volume_usd": 12440.21
    },
    {
        "Time": "2023-08-02T13:15:06.360261Z",
        "Price": 13528.16,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13528157,
        "volume_native": 6.71,
        "volume_usd": 12436.86
    },
    {
        "Time": "2023-08-02T13:30:11.106831Z",
        "Price": 13516.98,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13516976,
        "volume_native": 6.71,
        "volume_usd": 12426.58
    },
    {
        "Time": "2023-08-02T13:45:06.674708Z",
        "Price": 13523.85,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13523847,
        "volume_native": 6.71,
        "volume_usd": 12432.9
    },
    {
        "Time": "2023-08-02T14:00:07.432179Z",
        "Price": 13511.49,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13511491,
        "volume_native": 6.71,
        "volume_usd": 12421.54
    },
    {
        "Time": "2023-08-02T14:15:06.568156Z",
        "Price": 13491.61,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13491607,
        "volume_native": 6.71,
        "volume_usd": 12403.26
    },
    {
        "Time": "2023-08-02T14:30:07.04715Z",
        "Price": 13477.75,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13477752,
        "volume_native": 6.71,
        "volume_usd": 12390.52
    },
    {
        "Time": "2023-08-02T14:45:08.213295Z",
        "Price": 13463.1,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13463100,
        "volume_native": 6.71,
        "volume_usd": 12377.05
    },
    {
        "Time": "2023-08-02T15:00:08.405348Z",
        "Price": 13450.18,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13450178,
        "volume_native": 6.71,
        "volume_usd": 12365.17
    },
    {
        "Time": "2023-08-02T15:15:07.953775Z",
        "Price": 13430.48,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13430480,
        "volume_native": 6.71,
        "volume_usd": 12347.06
    },
    {
        "Time": "2023-08-02T15:30:08.637805Z",
        "Price": 13433.04,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13433042,
        "volume_native": 6.71,
        "volume_usd": 12349.42
    },
    {
        "Time": "2023-08-02T15:45:08.068985Z",
        "Price": 13442.71,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13442709,
        "volume_native": 6.71,
        "volume_usd": 12358.31
    },
    {
        "Time": "2023-08-02T16:00:06.944237Z",
        "Price": 13442.53,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13442533,
        "volume_native": 6.71,
        "volume_usd": 12358.14
    },
    {
        "Time": "2023-08-02T16:15:07.163913Z",
        "Price": 13438.6,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13438597,
        "volume_native": 6.71,
        "volume_usd": 12354.52
    },
    {
        "Time": "2023-08-02T16:30:08.095642Z",
        "Price": 13420.89,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13420895,
        "volume_native": 6.71,
        "volume_usd": 12338.25
    },
    {
        "Time": "2023-08-02T16:45:08.511015Z",
        "Price": 13436.04,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13436039,
        "volume_native": 6.7,
        "volume_usd": 12333.4
    },
    {
        "Time": "2023-08-02T17:00:08.329794Z",
        "Price": 13337.37,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13337371,
        "volume_native": 6.7,
        "volume_usd": 12242.83
    },
    {
        "Time": "2023-08-02T17:15:06.389553Z",
        "Price": 13331.74,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13331743,
        "volume_native": 6.7,
        "volume_usd": 12237.66
    },
    {
        "Time": "2023-08-02T17:30:06.686236Z",
        "Price": 13393.7,
        "floorprice_usd": 7.3,
        "marketCap_native": 7299,
        "marketCap_usd": 13393703,
        "volume_native": 6.7,
        "volume_usd": 12294.54
    },
    {
        "Time": "2023-08-02T17:45:06.76506Z",
        "Price": 15056.37,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15056370,
        "volume_native": 6.7,
        "volume_usd": 12302.16
    },
    {
        "Time": "2023-08-02T18:00:07.331758Z",
        "Price": 15013.06,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15013060,
        "volume_native": 6.7,
        "volume_usd": 12266.77
    },
    {
        "Time": "2023-08-02T18:15:06.037345Z",
        "Price": 15021,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15020999,
        "volume_native": 6.7,
        "volume_usd": 12273.26
    },
    {
        "Time": "2023-08-02T18:30:06.76764Z",
        "Price": 15016.22,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15016223,
        "volume_native": 6.7,
        "volume_usd": 12269.35
    },
    {
        "Time": "2023-08-02T18:45:06.696872Z",
        "Price": 15011.37,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15011374,
        "volume_native": 6.7,
        "volume_usd": 12265.39
    },
    {
        "Time": "2023-08-02T19:00:07.198829Z",
        "Price": 15017.81,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15017813,
        "volume_native": 6.7,
        "volume_usd": 12270.65
    },
    {
        "Time": "2023-08-02T19:15:06.286214Z",
        "Price": 15042.11,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15042107,
        "volume_native": 6.7,
        "volume_usd": 12290.5
    },
    {
        "Time": "2023-08-02T19:30:06.533041Z",
        "Price": 15070.68,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15070678,
        "volume_native": 6.7,
        "volume_usd": 12313.85
    },
    {
        "Time": "2023-08-02T19:45:06.393855Z",
        "Price": 15078.71,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15078713,
        "volume_native": 6.7,
        "volume_usd": 12320.41
    },
    {
        "Time": "2023-08-02T20:00:07.012541Z",
        "Price": 15095.89,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15095886,
        "volume_native": 6.7,
        "volume_usd": 12334.44
    },
    {
        "Time": "2023-08-02T20:15:06.17104Z",
        "Price": 15096.84,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15096836,
        "volume_native": 6.7,
        "volume_usd": 12335.22
    },
    {
        "Time": "2023-08-02T20:30:07.583789Z",
        "Price": 15103.66,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15103661,
        "volume_native": 6.7,
        "volume_usd": 12340.8
    },
    {
        "Time": "2023-08-02T20:45:06.577186Z",
        "Price": 15103.66,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15103661,
        "volume_native": 6.7,
        "volume_usd": 12340.8
    },
    {
        "Time": "2023-08-02T21:00:07.393715Z",
        "Price": 15107.83,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15107827,
        "volume_native": 6.7,
        "volume_usd": 12344.2
    },
    {
        "Time": "2023-08-02T21:15:05.557446Z",
        "Price": 15105,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15105001,
        "volume_native": 6.7,
        "volume_usd": 12341.89
    },
    {
        "Time": "2023-08-02T21:30:07.047439Z",
        "Price": 15107.29,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15107294,
        "volume_native": 6.7,
        "volume_usd": 12343.76
    },
    {
        "Time": "2023-08-02T21:45:06.720816Z",
        "Price": 15099.28,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15099280,
        "volume_native": 6.7,
        "volume_usd": 12337.22
    },
    {
        "Time": "2023-08-02T22:00:06.787878Z",
        "Price": 15114.52,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15114515,
        "volume_native": 6.7,
        "volume_usd": 12349.66
    },
    {
        "Time": "2023-08-02T22:15:06.611926Z",
        "Price": 15111.77,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15111769,
        "volume_native": 6.7,
        "volume_usd": 12347.42
    },
    {
        "Time": "2023-08-02T22:30:07.361074Z",
        "Price": 15121.41,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15121406,
        "volume_native": 6.7,
        "volume_usd": 12355.29
    },
    {
        "Time": "2023-08-02T22:45:06.656839Z",
        "Price": 15117.96,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15117960,
        "volume_native": 6.7,
        "volume_usd": 12352.48
    },
    {
        "Time": "2023-08-02T23:00:07.093754Z",
        "Price": 15106.98,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15106981,
        "volume_native": 6.7,
        "volume_usd": 12343.51
    },
    {
        "Time": "2023-08-02T23:15:06.727229Z",
        "Price": 15090.67,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15090665,
        "volume_native": 6.7,
        "volume_usd": 12330.18
    },
    {
        "Time": "2023-08-02T23:30:08.178833Z",
        "Price": 15068.55,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15068549,
        "volume_native": 6.7,
        "volume_usd": 12312.11
    },
    {
        "Time": "2023-08-02T23:45:06.737827Z",
        "Price": 15066.99,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15066987,
        "volume_native": 6.7,
        "volume_usd": 12310.83
    },
    {
        "Time": "2023-08-03T00:00:07.627727Z",
        "Price": 15071.66,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15071664,
        "volume_native": 6.7,
        "volume_usd": 12314.65
    },
    {
        "Time": "2023-08-03T00:05:25Z",
        "Price": 15080.1839904851,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15080183.9904851,
        "volume_native": 6.7,
        "volume_usd": 12321.61374832319
    },
    {
        "Time": "2023-08-03T00:15:06.99456Z",
        "Price": 15078.37,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15078374,
        "volume_native": 6.7,
        "volume_usd": 12320.13
    },
    {
        "Time": "2023-08-03T00:30:06.367545Z",
        "Price": 15097.19,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15097191,
        "volume_native": 6.7,
        "volume_usd": 12335.51
    },
    {
        "Time": "2023-08-03T00:45:06.433833Z",
        "Price": 15099.49,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15099488,
        "volume_native": 6.7,
        "volume_usd": 12337.39
    },
    {
        "Time": "2023-08-03T01:00:07.192361Z",
        "Price": 15095.63,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15095633,
        "volume_native": 6.7,
        "volume_usd": 12334.24
    },
    {
        "Time": "2023-08-03T01:15:06.170217Z",
        "Price": 15076.47,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15076470,
        "volume_native": 6.7,
        "volume_usd": 12318.58
    },
    {
        "Time": "2023-08-03T01:30:06.023647Z",
        "Price": 15086.14,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15086136,
        "volume_native": 6.7,
        "volume_usd": 12326.48
    },
    {
        "Time": "2023-08-03T01:45:07.203871Z",
        "Price": 15091.95,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15091954,
        "volume_native": 6.7,
        "volume_usd": 12331.23
    },
    {
        "Time": "2023-08-03T02:00:05.609958Z",
        "Price": 15112.86,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15112862,
        "volume_native": 6.7,
        "volume_usd": 12348.31
    },
    {
        "Time": "2023-08-03T02:15:06.150265Z",
        "Price": 15118.55,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15118548,
        "volume_native": 6.7,
        "volume_usd": 12352.96
    },
    {
        "Time": "2023-08-03T02:30:06.540527Z",
        "Price": 15100.15,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15100155,
        "volume_native": 6.7,
        "volume_usd": 12337.93
    },
    {
        "Time": "2023-08-03T02:45:06.487024Z",
        "Price": 15068.09,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15068095,
        "volume_native": 6.7,
        "volume_usd": 12311.74
    },
    {
        "Time": "2023-08-03T03:00:20.424225Z",
        "Price": 15085.09,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15085095,
        "volume_native": 6.7,
        "volume_usd": 12325.63
    },
    {
        "Time": "2023-08-03T03:15:20.16105Z",
        "Price": 15088.39,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15088392,
        "volume_native": 6.7,
        "volume_usd": 12328.32
    },
    {
        "Time": "2023-08-03T03:30:20.966363Z",
        "Price": 15083.19,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15083191,
        "volume_native": 6.7,
        "volume_usd": 12324.07
    },
    {
        "Time": "2023-08-03T03:45:21.496885Z",
        "Price": 15078.29,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15078291,
        "volume_native": 6.7,
        "volume_usd": 12320.07
    },
    {
        "Time": "2023-08-03T04:02:02.76634Z",
        "Price": 15080.3,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15080298,
        "volume_native": 6.7,
        "volume_usd": 12321.71
    },
    {
        "Time": "2023-08-03T04:16:43.140158Z",
        "Price": 15084.69,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15084686,
        "volume_native": 6.7,
        "volume_usd": 12325.29
    },
    {
        "Time": "2023-08-03T04:31:45.167493Z",
        "Price": 15074.69,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15074689,
        "volume_native": 6.7,
        "volume_usd": 12317.12
    },
    {
        "Time": "2023-08-03T04:46:42.632778Z",
        "Price": 15059.42,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15059422,
        "volume_native": 6.7,
        "volume_usd": 12304.65
    },
    {
        "Time": "2023-08-03T05:02:36.612837Z",
        "Price": 15057.82,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15057815,
        "volume_native": 6.7,
        "volume_usd": 12303.34
    },
    {
        "Time": "2023-08-03T05:16:11.518055Z",
        "Price": 15055.35,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15055353,
        "volume_native": 6.7,
        "volume_usd": 12301.33
    },
    {
        "Time": "2023-08-03T05:31:12.171046Z",
        "Price": 15041.17,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15041173,
        "volume_native": 6.7,
        "volume_usd": 12289.74
    },
    {
        "Time": "2023-08-03T05:46:10.489457Z",
        "Price": 15057.12,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15057117,
        "volume_native": 6.7,
        "volume_usd": 12302.77
    },
    {
        "Time": "2023-08-03T06:00:08.446244Z",
        "Price": 15062.86,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15062865,
        "volume_native": 6.7,
        "volume_usd": 12307.46
    },
    {
        "Time": "2023-08-03T06:15:06.548167Z",
        "Price": 15063.62,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15063618,
        "volume_native": 6.7,
        "volume_usd": 12308.08
    },
    {
        "Time": "2023-08-03T06:30:09.751698Z",
        "Price": 15067.23,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15067230,
        "volume_native": 6.7,
        "volume_usd": 12311.03
    },
    {
        "Time": "2023-08-03T06:45:11.43046Z",
        "Price": 15060.26,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15060257,
        "volume_native": 6.7,
        "volume_usd": 12305.33
    },
    {
        "Time": "2023-08-03T07:00:06.687858Z",
        "Price": 15043.98,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15043979,
        "volume_native": 6.7,
        "volume_usd": 12292.03
    },
    {
        "Time": "2023-08-03T07:15:07.898867Z",
        "Price": 15035.74,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15035741,
        "volume_native": 6.7,
        "volume_usd": 12285.3
    },
    {
        "Time": "2023-08-03T07:30:07.088247Z",
        "Price": 15005.42,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15005421,
        "volume_native": 6.7,
        "volume_usd": 12260.53
    },
    {
        "Time": "2023-08-03T07:45:08.284229Z",
        "Price": 15013.24,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15013238,
        "volume_native": 6.7,
        "volume_usd": 12266.91
    },
    {
        "Time": "2023-08-03T08:00:06.964943Z",
        "Price": 15011.68,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15011677,
        "volume_native": 6.7,
        "volume_usd": 12265.64
    },
    {
        "Time": "2023-08-03T08:15:10.671142Z",
        "Price": 15020.39,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15020392,
        "volume_native": 6.7,
        "volume_usd": 12272.76
    },
    {
        "Time": "2023-08-03T08:30:08.026418Z",
        "Price": 14991.98,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 14991978,
        "volume_native": 6.7,
        "volume_usd": 12249.54
    },
    {
        "Time": "2023-08-03T08:45:09.207505Z",
        "Price": 14986.89,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 14986895,
        "volume_native": 6.7,
        "volume_usd": 12245.39
    },
    {
        "Time": "2023-08-03T09:00:08.342017Z",
        "Price": 15003.56,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15003560,
        "volume_native": 6.7,
        "volume_usd": 12259.01
    },
    {
        "Time": "2023-08-03T09:15:10.718619Z",
        "Price": 15037.7,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15037704,
        "volume_native": 6.7,
        "volume_usd": 12286.9
    },
    {
        "Time": "2023-08-03T09:30:07.203545Z",
        "Price": 15026.57,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15026572,
        "volume_native": 6.7,
        "volume_usd": 12277.81
    },
    {
        "Time": "2023-08-03T09:45:08.156834Z",
        "Price": 15026.12,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15026118,
        "volume_native": 6.7,
        "volume_usd": 12277.44
    },
    {
        "Time": "2023-08-03T10:00:07.926674Z",
        "Price": 15031.82,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15031824,
        "volume_native": 6.7,
        "volume_usd": 12282.1
    },
    {
        "Time": "2023-08-03T10:15:08.066237Z",
        "Price": 15039.32,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15039322,
        "volume_native": 6.7,
        "volume_usd": 12288.23
    },
    {
        "Time": "2023-08-03T10:30:09.429397Z",
        "Price": 15038.17,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15038172,
        "volume_native": 6.7,
        "volume_usd": 12287.29
    },
    {
        "Time": "2023-08-03T10:45:08.381338Z",
        "Price": 15046.02,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15046023,
        "volume_native": 6.7,
        "volume_usd": 12293.7
    },
    {
        "Time": "2023-08-03T11:00:08.589029Z",
        "Price": 15056.67,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15056668,
        "volume_native": 6.7,
        "volume_usd": 12302.4
    },
    {
        "Time": "2023-08-03T11:15:08.091977Z",
        "Price": 15049.28,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15049280,
        "volume_native": 6.7,
        "volume_usd": 12296.36
    },
    {
        "Time": "2023-08-03T11:30:07.430824Z",
        "Price": 15045.89,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15045890,
        "volume_native": 6.7,
        "volume_usd": 12293.59
    },
    {
        "Time": "2023-08-03T11:45:07.483162Z",
        "Price": 15047.71,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15047713,
        "volume_native": 6.7,
        "volume_usd": 12295.08
    },
    {
        "Time": "2023-08-03T12:00:06.89603Z",
        "Price": 15054.77,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15054771,
        "volume_native": 6.7,
        "volume_usd": 12300.85
    },
    {
        "Time": "2023-08-03T12:15:06.818778Z",
        "Price": 15056.4,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15056398,
        "volume_native": 6.7,
        "volume_usd": 12302.18
    },
    {
        "Time": "2023-08-03T12:30:07.166623Z",
        "Price": 15048.49,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15048489,
        "volume_native": 6.7,
        "volume_usd": 12295.72
    },
    {
        "Time": "2023-08-03T12:45:07.301154Z",
        "Price": 15029.1,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15029095,
        "volume_native": 6.7,
        "volume_usd": 12279.87
    },
    {
        "Time": "2023-08-03T13:00:06.294025Z",
        "Price": 15030.45,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15030451,
        "volume_native": 6.7,
        "volume_usd": 12280.98
    },
    {
        "Time": "2023-08-03T13:15:06.217379Z",
        "Price": 15046.82,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15046818,
        "volume_native": 6.7,
        "volume_usd": 12294.35
    },
    {
        "Time": "2023-08-03T13:30:13.847868Z",
        "Price": 15049.15,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15049152,
        "volume_native": 6.7,
        "volume_usd": 12296.26
    },
    {
        "Time": "2023-08-03T13:45:07.713809Z",
        "Price": 15055.98,
        "floorprice_usd": 8.2,
        "marketCap_native": 8200,
        "marketCap_usd": 15055984,
        "volume_native": 6.7,
        "volume_usd": 12301.84
    }
]
print("tbsftbrtsnrty", len(lklk))





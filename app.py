from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import csv

app = Flask(__name__)
CORS(app, resources=r'/*')
app.config['JSON_AS_ASCII'] = False


@app.route('/mesBE/upload', methods=['POST'])
def upload():
    data_dict = request.get_data()
    file = request.files['file']
    name = file.filename
    file.save("./data/" + name)
    return name


@app.route('/mesBE/read', methods=['POST'])
def read():
    data = request.get_json()
    name = data['name']
    data_res = []
    index = 1
    with open("./data/" + name, 'r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # 读取第一行作为表头
        for row in csv_reader:
            data_res.append({
                'id': index,
                'source': row[0],
                'target': row[1],
                'time': row[2],
            })
            index = index + 1
    return data_res


@app.route('/mesBE/searchData', methods=['POST'])
def searchData():
    data = request.get_json()
    data_ip = searchFromIPS(data['data'], data['form']['user'])
    data_time = searchFromTime(data_ip, data['form']['timestart'], data['form']['timeend'])
    data_hop = searchHopOnce(data['data'], data_time, data['form']['hop'], data['form']['timestart'],
                             data['form']['timeend'])
    return data_hop


# 检索Ip段，多个ip
def searchFromIPS(data, user):
    Ips = user.split(";")
    res = []
    for data_mes in data:
        for ip in Ips:
            parts = ip.split(".")
            ip_result = ".".join(parts[:3])+'.'
            if ip_result in data_mes['source'] or ip_result in data_mes['target']:
                res.append(data_mes)
    return res


# 检索时间
def searchFromTime(data, timestart, timeend):
    res = []
    for data_time in data:
        if timestart <= data_time['timesecond'] <= timeend:
            res.append(data_time)
    return res


# 检索一条信息的前后n跳数
def searchHopOnce(data, mes_hop, n, start, end):
    res = []
    mes = mes_hop
    n = int(n)
    while n > 0:
        temp = []
        for index in mes:
            if isinstance(index, int):
                mes_index = data[index - 1]
            else:
                mes_index = index
                res.append(index['id'])
            for data_index in data:
                if start <= data_index['timesecond'] <= end:
                    if data_index['timesecond'] < mes_index['timesecond'] and mes_index['source'] == data_index['target']:
                        res.append(data_index['id'])
                        temp.append(data_index['id'])
                    if data_index['timesecond'] > mes_index['timesecond'] and mes_index['target'] == data_index['source']:
                        res.append(data_index['id'])
                        temp.append(data_index['id'])
        mes = list(set(temp))
        n -= 1
    res_final = []
    for index in list(set(res)):
        res_final.append(data[index - 1])
    return res_final


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port='5002')

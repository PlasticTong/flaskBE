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
    with open("./data/" + name, 'r',encoding='utf-8-sig') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # 读取第一行作为表头

        # 获取标题所在列的索引
        source_index = header.index('source')
        target_index = header.index('target')
        time_index = header.index('time')
        for row in csv_reader:
            data_res.append({
                'id': index,
                'source': row[source_index],
                'target': row[target_index],
                'time': row[time_index],
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
            ip_result = ".".join(parts[:3]) + '.'
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
                    if data_index['timesecond'] < mes_index['timesecond'] and mes_index['source'] == data_index[
                        'target']:
                        res.append(data_index['id'])
                        temp.append(data_index['id'])
                    if data_index['timesecond'] > mes_index['timesecond'] and mes_index['target'] == data_index[
                        'source']:
                        res.append(data_index['id'])
                        temp.append(data_index['id'])
        mes = list(set(temp))
        n -= 1
    res_final = []
    for index in list(set(res)):
        res_final.append(data[index - 1])
    return res_final


@app.route('/mesBE/check_ip', methods=['POST'])
def check_ip():
    data_json = request.get_json()
    data = data_json["data"]
    form = data_json["form"]
    ips = form['user'].split(";")
    hop = int(form['hop'])
    start_time = form["timestart"]
    end_time = form["timeend"]
    df = pd.DataFrame(data, index=None)
    df = df[(df.timesecond >= start_time) & (df.timesecond <= end_time)]
    new_df = pd.DataFrame()
    for ip_ in ips:
        up, down = [ip_], [ip_]
        for i in range(1, hop + 1):
            if len(up) != 0:
                for ip in up:
                    if i == 1:
                        df_1 = df[df.target == ip]
                        if len(df_1) != 0:
                            up = df_1.loc[:, ['source', 'timesecond']].values
                            new_df = pd.concat([new_df, df_1], axis=0)
                        else:
                            up = []
                    else:
                        df_1 = df[(df.target == ip[0]) & (df.timesecond < ip[1])]
                        if len(df_1) != 0:
                            up = df_1.loc[:, ['source', 'timesecond']].values
                            new_df = pd.concat([new_df, df_1], axis=0)
                        else:
                            up = []
            if len(down) != 0:
                for ip in down:
                    if i == 1:
                        df_2 = df[df.source == ip]
                        if len(df_2) != 0:
                            down = df_2.loc[:, ['target', 'timesecond']].values
                            new_df = pd.concat([new_df, df_2], axis=0)
                        else:
                            down = []
                    else:
                        df_2 = df[(df.source == ip[0]) & (df.timesecond > ip[1])]
                        if len(df_2) != 0:
                            down = df_2.loc[:, ['target', 'timesecond']].values
                            new_df = pd.concat([new_df, df_2], axis=0)
                        else:
                            down = []
            # for ip in up:
            #     df_1 = df[df.target == ip]
            #     up = df_1.source.tolist()
            #     print(up)
            #     new_df = pd.concat([new_df, df_1], axis=0)
            # for ip in down:
            #     df_2 = df[df.source == ip]
            #     down = df_2.target.tolist()
            #     new_df = pd.concat([new_df, df_2], axis=0)
    new_df = new_df.drop_duplicates(keep='first')
    dd = new_df.sort_values('id').to_dict("records")
    return dd


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port='5003')

import requests
import os
import json
import datetime
import time
from urllib import parse
import schedule
class NetflowObj:
    def __init__(self, src_ip, src_port, des_ip, des_port, bytes, proto):
        self.src_ip = src_ip
        self.src_port = src_port
        self.des_ip = des_ip
        self.des_port = des_port
        self.bytes = bytes
        self.proto = proto


    def __str__(self) -> str:
        return f"{self.src_ip}:{self.src_port} -> {self.des_ip}:{self.des_port} ======{self.bytes}"

    def get_key(self) -> str:
        return f"{self.src_ip}:{self.des_ip}"
        # return f"{self.src_ip}_{self.des_ip}"

class NetflowAggObj:
    def __init__(self, netflowObj:NetflowObj, start_time:str, end_time:str):
        self.src_ip = netflowObj.src_ip
        self.des_ip = netflowObj.des_ip
        self.start_time = start_time
        self.end_time = end_time
        self.bytes = netflowObj.bytes
        self.count = 1

    def agg(self, netflowObj:NetflowObj) -> int:
        self.bytes = self.bytes + netflowObj.bytes
        self.count = self.count + 1
        return self.bytes
    def __str__(self) -> str:
        print(self.src_ip,self.des_ip,self.bytes,self.count)

def get_node_path(host:str):
    return host[-3:]

def lokiapi(host: str, start: str, end: str, limit: int = None) -> list[NetflowObj]:
    try:
        startdt = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        enddt = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        query_data = {
            'start': startdt.timestamp(),
            'end': enddt.timestamp()
        }
        if limit is not None:
            query_data['limit'] = limit
        query = parse.urlencode(query=query_data)
        base_url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}"
        resp = requests.get(f'{base_url}&{query}')
        # url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}&limit={limit}&start={startdt.timestamp()}&end={enddt.timestamp()}"
        if resp.status_code != 200:
            raise Exception(f"{resp.text}")
        return parse_lokiapi_data(resp.text)
    except:
        return []

def parse_lokiapi_data(data) -> list[NetflowObj]:
    try:
        source = json.loads(data)['data']['result'][0]['values']
        newflows = [json.loads(record[1]) for record in source]
        data = []
        for newflow in newflows:
            data.append(NetflowObj(src_ip=newflow['source']['ip'], src_port=newflow['source']['port'],
                    des_ip=newflow['destination']['ip'], des_port=newflow['destination']['port'],
                    bytes=newflow['network']['bytes'], proto=newflow['network']['transport']))
        return data
    except:
        return []

def get_data_by_5m(host, start) -> list[NetflowObj]:
    end = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    return lokiapi(host=host, start=start, end=end, limit=1000000)

def agg_5m(host:str, start:str) -> list[NetflowAggObj]:
    end = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    data = get_data_by_5m(host=host, start=start)
    dic = {}
    for netflow in data:
        netflowAggObj = dic.get(netflow.get_key(), None)
        if netflowAggObj is None:
            dic[netflow.get_key()] = NetflowAggObj(netflow, start, end)
        else:
            netflowAggObj.agg(netflow)
    return list(dic.values())
    # draw.count_pic(data=data, xlabel="bytes(power of 10)", ylabel="count", title="5min", filename="5min.png")
def agg_1h(host:str, start:str) -> list[NetflowAggObj]:
    node_path = get_node_path(host=host)
    log_path_dir = os.path.join('/root/work/log',
                 get_node_path(host=host),
                 '5m',
                 datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d")
                 )
    dic = {}
    for i in range(12):
        log_filename = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5*i)).strftime("%H:%M:%S")+'.log'
        log_file_path = os.path.join(log_path_dir, log_filename)
        if not os.path.exists(log_file_path):
            continue
        with open(log_file_path,"r") as f:
            data = f.readlines()
            for obj in data:
                netflowAggObj=NetflowAggObj()
                netflowAggObj.__dict__.update(json.loads(obj))
                print(netflowAggObj)
    # draw.count_pic(data=data, xlabel="bytes(power of 10)", ylabel="count", title="5min", filename="5min.png")


def get_latest_5m_start_datetime():
    now = datetime.datetime.now()
    timestamp = datetime.datetime.timestamp(now)
    return datetime.datetime.fromtimestamp(timestamp - timestamp % 300 - 300)

def get_latest_1h_start_datetime():
    now = datetime.datetime.now()
    timestamp = datetime.datetime.timestamp(now)
    return datetime.datetime.fromtimestamp(timestamp - timestamp % 3600 - 3600)

def write_log(data:list[NetflowAggObj], path:str):
    log_dir = os.path.dirname(path)
    log_filename = os.path.basename(path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    with open(os.path.join(log_dir, log_filename),"w") as f:
        for obj in data:
            f.write(json.dumps(obj.__dict__)+'\n')

def job_5m(host:str):
    start_datetime=get_latest_5m_start_datetime()
    data = agg_5m(host=host, start=start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    date = start_datetime.strftime('%Y-%m-%d')
    time = start_datetime.strftime('%H-%M-%S')
    log_dir = os.path.join('/root/work/log','140','5m',date,f'{time}.log')
    write_log(data=data, path=log_dir)

def job_1h(host:str):
    start_datetime=get_latest_1h_start_datetime()
    data = agg_1h(host=host, start=start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    date = start_datetime.strftime('%Y-%m-%d')
    time = start_datetime.strftime('%H-%M-%S')
    log_dir = os.path.join('/root/work/log','140','5m',date,f'{time}.log')
    write_log(data=data, path=log_dir)

if __name__ == '__main__':
    host = "223.193.36.79:7140"
    schedule.every(5).minutes.do(job_5m, host)
    while True:
        schedule.run_pending()
        time.sleep(1)

# http://223.193.36.79:7140/loki/api/v1/query_range?query={job=%22netflow%22}?start=1700619300.0&end=1700619600.0&limit=1000000
# 103012
# 75806

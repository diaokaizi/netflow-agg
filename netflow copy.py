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
        return json.dumps(self.__dict__)
    def get_key(self) -> str:
        return f"{self.src_ip}:{self.des_ip}"
        # return f"{self.src_ip}_{self.des_ip}"

class NetflowAggObj(NetflowObj):
    def __init__(self, src_ip:str, des_ip:str, bytes:int, start_time:str, end_time:str, count:int):
        self.src_ip = src_ip
        self.des_ip =des_ip
        self.start_time = start_time
        self.end_time = end_time
        self.bytes = bytes
        self.count = count

    def agg(self, bytes:int, count:int) -> int:
        self.bytes = self.bytes + bytes
        self.count = self.count + count
        return self.bytes
    def __str__(self) -> str:
        return json.dumps(self.__dict__)
def get_node_path(host:str):
    return host[-3:]

def get_5m_filename(date_time:datetime.datetime):
    return f'{date_time.strftime("%H-%M-%S")}.log'

def get_5m_dir(host:str, date_time:datetime.datetime):
    return os.path.join('/root/work/logtest',get_node_path(host=host),'5m',date_time.strftime('%Y-%m-%d'))

def get_5m_path(host:str, date_time:datetime.datetime):
    return os.path.join(get_5m_dir(host=host, date_time=date_time), get_5m_filename(date_time))

def get_1h_filename(date_time:datetime.datetime):
    return f'{date_time.strftime("%H-%M-%S")}.log'

def get_1h_dir(host:str, date_time:datetime.datetime):
    return os.path.join('/root/work/logtest',get_node_path(host=host),'1h',date_time.strftime('%Y-%m-%d'))

def get_1h_path(host:str, date_time:datetime.datetime):
    return os.path.join(get_1h_dir(host=host, date_time=date_time), get_1h_filename(date_time))


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
            dic[netflow.get_key()] = NetflowAggObj(
                    src_ip=netflow.src_ip,
                    des_ip=netflow.des_ip,
                    bytes=netflow.bytes,
                    start_time=start,
                    end_time=end,
                    count=1
                )
        else:
            netflowAggObj.agg(bytes=netflow.bytes, count=1)
    return list(dic.values())
    # draw.count_pic(data=data, xlabel="bytes(power of 10)", ylabel="count", title="5min", filename="5min.png")
def agg_1h(host:str, start:str) -> list[NetflowAggObj]:
    date_time = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    dic = {}
    for i in range(12):
        log_path = get_5m_path(host=host, date_time=date_time)
        if not os.path.exists(log_path):
            continue
        with open(log_path,"r") as f:
            data = f.readlines()
            for obj in data:
                json_object = json.loads(obj)
                netflowAggObj=NetflowAggObj(
                    src_ip=json_object["src_ip"],
                    des_ip=json_object["des_ip"],
                    bytes=json_object["bytes"],
                    start_time=json_object["start_time"],
                    end_time=json_object["end_time"],
                    count=json_object["count"] if "count" in json_object else 0,
                )
                netflowAggObj_dic = dic.get(netflowAggObj.get_key(), None)
                if netflowAggObj_dic is None:
                    dic[netflowAggObj.get_key()] = netflowAggObj
                else:
                    netflowAggObj_dic.agg(bytes=netflowAggObj.bytes, count=netflowAggObj.count)
    return list(dic.values())

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
    write_log(data=data, path=get_5m_path(host=host, date_time=start_datetime))

def job_1h(host:str):
    start_datetime=get_latest_1h_start_datetime()
    data = agg_1h(host=host, start=start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    write_log(data=data, path=get_1h_path(host=host, date_time=start_datetime))

if __name__ == '__main__':
    host = "223.193.36.79:7140"
    schedule.every(5).minutes.do(job_5m, host)
    schedule.every(1).hours.do(job_1h, host)
    while True:
        schedule.run_pending()
        time.sleep(1)
# http://223.193.36.79:7140/loki/api/v1/query_range?query={job=%22netflow%22}?start=1700619300.0&end=1700619600.0&limit=1000000
# 103012
# 75806

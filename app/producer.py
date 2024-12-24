import configparser
import json
import lzma
import os
import sys
import time
import pymysql
import schedule
import pika
import uuid
import requests
import pandas as pd

def get_task_name():
    timestamp = time.time()
    return int(timestamp)


def read_config(config_file):
    # 创建 ConfigParser 对象
    config = configparser.ConfigParser()
    # 读取配置文件
    config.read(config_file)
    return config


def get_node_name(head):
    address = head.split("_")[0]
    return address


# def get_node_names():
#     node_names = []
#     config_folder = "config"
#     for config_file in os.listdir(config_folder):
#         if config_file.endswith(".ini"):
#             config = read_config("config/" + config_file)
#             node_name = config.get('Queue', 'name')
#             node_names.append(node_name)
#     return node_names


def get_queues_bound_to_fanout_exchange(exchange_name):
    # RabbitMQ Management API 的基本 URL
    api_url = "http://8.210.155.15:15672/api"
    # 构建 API 请求的 URL
    url = f"{api_url}/exchanges/yd/{exchange_name}/bindings/source"

    # 发送 GET 请求获取绑定信息
    response = requests.get(url, auth=('admin', 'Liuling123!'))

    # 检查响应状态码
    if response.status_code == 200:
        # 解析 JSON 响应
        bindings = response.json()
        # 提取队列信息
        queues = [binding['destination'] for binding in bindings]

        return queues
    else:
        # 打印错误信息
        print(f"Failed to fetch bindings. Status code: {response.status_code}")
        return None


def sche_time_run(httpdns_provider):
    # 读取配置文件中的定时执行时间列表
    config = read_config("config.ini")
    times_str = config.get('schedule', "times")
    scheduled_times = times_str.split(',')
    # 使用schedule库设置定时任务
    for time_str in scheduled_times:
        schedule.every().day.at(time_str).do(main, httpdns_provider)

    while True:
        schedule.run_pending()
        time.sleep(1)


def update_config(new_times):
    # 更新配置文件中的时间列表
    config = read_config("config.ini")
    config.set('schedule', 'times', ','.join(new_times))

    # 将更新写入配置文件
    with open('config.ini', 'w') as config_file:
        config.write(config_file)


def update_schedule(new_times, httpdns_provider):
    # new_times是列表['00:00', '04:00', '08:00', '12:00', '15:34', '20:00']
    update_config(new_times)
    # 清除原有的定时任务
    schedule.clear()

    sche_time_run(httpdns_provider)


class dns_producer:
    def __init__(self, httpdns_provider):
        self.corr_id = None
        self.response = None
        self.task_id = None
        self.httpdns_provider = httpdns_provider
        self.timeout = 6000000  # 设置超时时间为60秒
        self.start_time = time.time()  # 记录程序开始时间
        self.fp = None
        self.bind_consumer = []
        self.credentials = pika.PlainCredentials('admin', 'Liuling123!')  # mq用户名和密码
        # 虚拟队列需要指定参数 virtual_host，如果是默认的可以不填。
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='8.210.155.15', port=5672, virtual_host='yd', heartbeat=0,
                                      credentials=self.credentials))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='yd', exchange_type='fanout', durable=True, passive=True)
        self.bind_consumer = get_queues_bound_to_fanout_exchange('yd')
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=False)

    def on_response(self, ch, method, properties, body):
        if self.corr_id == properties.correlation_id:
            self.response = json.loads(lzma.decompress(body).decode())
            # 提取 head 和 body 信息
            head = self.response.get('head')
            body = self.response.get('body')
            node_name = get_node_name(head)
            self.bind_consumer.remove(node_name)
            # 创建文件夹并保存结果到 JSON 文件
            create_folder_and_save_result(self.task_id, head, body, self.httpdns_provider)
            self.fp = f"task/task_{self.task_id}"

    def call(self, message):
        self.corr_id = str(uuid.uuid4())
        self.task_id = get_task_name()
        message["task_id"] = self.task_id
        # 消息压缩
        compress_info = lzma.compress(json.dumps(message).encode())
        # 向队列插入数值 routing_key是队列名。delivery_mode = 2 声明消息在队列中持久化，delivery_mod = 1 消息非持久化。routing_key 不需要配置
        self.channel.basic_publish(
            exchange='yd',
            routing_key='',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
                delivery_mode=2
            ),
            body=compress_info
        )

        # config_folder = "config"
        # config_files = [f for f in os.listdir(config_folder) if f.endswith(".ini")]

        while self.response is None or self.bind_consumer:
            self.connection.process_data_events()
            current_time = time.time()
            if current_time - self.start_time > self.timeout:
                print(f"节点：{self.bind_consumer}Timeout")
                break
        return self.fp


def create_folder_and_save_result(task_id, head, body, httpdns_provider):
    # 创建文件夹
    folder_name = f"task/task_{task_id}"
    os.makedirs(folder_name, exist_ok=True)

    # JSON 文件名为节点名
    file_name = f'{httpdns_provider}_{head}.json'
    file_path = os.path.join(folder_name, file_name)

    # 保存对应的值到 JSON 文件中
    with open(file_path, 'w') as file:
        json.dump(body, file, indent=2)


def get_domain_dns():
    # 建立数据库连接
    conn = pymysql.connect(
        host='39.98.130.93',  # 主机名（或IP地址）
        port=36868,  # 端口号，默认为3306
        user='root',  # 用户名
        password='PlatForm!',  # 密码
        charset='utf8'  # 设置字符编码
    )

    # 创建游标对象
    cursor = conn.cursor()
    # 选择数据库
    conn.select_db("detect_dns_lc")
    # 执行查询操作
    cursor.execute('SELECT ip FROM dns_list_public')
    # 获取查询结果，返回元组
    dns = cursor.fetchall()
    cursor.execute('SELECT domain FROM domain_list')
    domain = cursor.fetchall()
    dns_list = []
    for row in dns:
        dns_list.append(row[0])
    domain_list = []
    for row in domain:
        domain_list.append(row[0])
    # 关闭游标和连接
    cursor.close()
    conn.close()
    return domain_list, dns_list


def main(domains, httpdns_provider):
    producer = dns_producer(httpdns_provider)
    domains.sort()
    print(len(domains))
    message = {'domains': domains[:1000000], "httpdns_provider": httpdns_provider}
    fp = producer.call(message)
    print(fp)
    return fp


def read_txt(fp):
    with open(fp, 'r') as f:
        content = [line.strip('\n') for line in f.readlines()]
    return content


if __name__ == '__main__':
    # domains, _ = get_domain_dns()
    # domains = domains[6:] * 40

    # df = pd.read_csv('./cloudflare-radar_top-1000000-domains_20241003-20241010.csv')
    # domains = list(df['domain'])
    # 演示使用
    domains = read_txt(sys.argv[1])
    httpdns = read_txt(sys.argv[2])[0]
    # 支持 A, AAAA, ANY, CAA, CNAME, MX, NAPTR, NS, PTR, SOA, SRV, TXT
    # 测试过A、AAAA、MX、CNAME、NS
    main(domains, httpdns)
    # 定时
    # new_times = ['10:21']
    # update_schedule(new_times,'A')

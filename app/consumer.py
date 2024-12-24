import asyncio
import aiohttp
import uvloop
import multiprocessing
from multiprocessing import Process
import math
import time
import json
import lzma
import pika
import warnings
import tqdm
import hashlib
import resource
import pytz as pytz
import datetime

import get_id
from huoshan import HuoshanHTTPDNSResolver

warnings.filterwarnings('ignore', category=DeprecationWarning)
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
new_soft_limit = 1048576

(rlimit_nofile_soft, rlimit_nofile_hard) = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft_limit, rlimit_nofile_hard))
httpdns_provider = ''

class BaiduHTTPDNSResolver:
    def __init__(self, account_id, secret):
        self.base_url = "http://180.76.76.200/v3/resolve"
        self.account_id = account_id
        self.secret = secret

    def generate_sign(self, dn, t):
        sign_str = f"{dn}-{self.secret}-{t}"
        return hashlib.md5(sign_str.encode()).hexdigest()

    async def resolve_domain(self, session, dn, ip=None):
        t = str(int(time.time()) + 300)
        sign = self.generate_sign(dn, t)
        params = {
            "account_id": self.account_id,
            "dn": dn,
            "sign": sign,
            "t": t
        }
        if ip:
            params['ip'] = ip

        async with session.get(self.base_url, params=params) as response:
            return await response.json(content_type='text/plain; charset=utf-8')


def get_resolver(httpdns):
    if httpdns == "baidu":
        return BaiduHTTPDNSResolver('137279', 'sbDYgAEM7JXVv7xpgo18')
    elif httpdns == "huoshan":
        return HuoshanHTTPDNSResolver('2102411820', 'rU195Gb2aWaHdnfs')
    else:
        # Placeholder for other providers like Tencent
        raise NotImplementedError(f"HTTPDNS provider '{httpdns}' is not implemented.")


def get_current_timestamp():
    tz = pytz.timezone('Asia/Shanghai')
    return datetime.datetime.fromtimestamp(int(time.time()), tz).strftime('%Y-%m-%d-%H-%M-%S')


def list_split(list_temp, n):
    for i in range(0, len(list_temp), n):
        yield list_temp[i:i + n]


async def resolve_domain(domain, resolver, session, semaphore):
    async with semaphore:
        try:
            result = await resolver.resolve_domain(session, domain)
            global httpdns_provider
            if httpdns_provider == 'baidu':
                if result['msg'] == 'ok':
                    return {domain: result['data'][domain]['ip']}
                else:
                    return {domain: {"error": result['msg']}}
            elif httpdns_provider == 'huoshan':
                if 'error' in result:
                    return {domain: result}
                else:
                    return {domain: result['ips']}
        except asyncio.TimeoutError:
            return {domain: {"error": "Timeout"}}


async def resolve_domains(domains, resolver, coroutine_num):
    semaphore = asyncio.Semaphore(coroutine_num)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), connector=aiohttp.TCPConnector(limit=1000)) as session:
        tasks = []
        for domain in domains:
            task = resolve_domain(domain, resolver, session, semaphore)
            tasks.append(task)

        results = []
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing HTTPDNS Queries"):
            result = await f
            results.append(result)
    return results


def run_async_tasks(task_id, domains, resolver, coroutine_num):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(resolve_domains(domains, resolver, coroutine_num))
    loop.close()
    return results


class QueryProcess(Process):
    def __init__(self, task_id, process_name, q, domains, resolver, coroutine_num):
        super().__init__()
        self.process_name = process_name
        self.q = q
        self.domains = domains
        self.resolver = resolver
        self.coroutine_num = coroutine_num
        self.task_id = task_id

    def run(self):
        result = run_async_tasks(self.task_id, self.domains, self.resolver, self.coroutine_num)
        self.q.put(result)


def get_process_num(process_times):
    process_num = multiprocessing.cpu_count()
    return int(math.ceil(process_num * process_times))


def allocating_task(task_id, domains, resolver, process_times=0.5, coroutine_num=1000):
    manager = multiprocessing.Manager()
    q = manager.Queue()
    process_num = get_process_num(process_times)
    print('process_num:', process_num)

    all_results = {}

    # 根据进程数将整个域名列表分配给多个进程
    avg_list = list_split(domains, math.ceil(len(domains) / process_num))
    try:
        p_list = [
            QueryProcess(task_id, f'Process_{i}', q, each_list, resolver, coroutine_num)
            for i, each_list in enumerate(avg_list)
        ]
        for p in p_list:
            p.start()
        for p in p_list:
            p.join()
    except Exception as e:
        print(f"处理域名时出错: {e}")

    while not q.empty():
        tmp = q.get()
        for result in tmp:
            for key, value in result.items():
                if key not in all_results:
                    all_results[key] = []
                all_results[key] = value

    return all_results


class Consumer:
    def __init__(self):
        self.queue_name = get_id.get_unique_identifier()
        self.start_time = None
        self.end_time = None

    def on_request(self, ch, method, properties, body):
        self.start_time = get_current_timestamp()
        try:
            message_data = json.loads(lzma.decompress(body).decode())
            task_id = message_data["task_id"]
            domains = message_data["domains"]
            global httpdns_provider
            httpdns_provider = message_data["httpdns_provider"]

            resolver = get_resolver(httpdns_provider)
            results = allocating_task(task_id, domains, resolver)  # 移除了批处理逻辑
        except json.JSONDecodeError as e:
            results = None
            print(f"解析 JSON 时出错: {e}")

        self.end_time = get_current_timestamp()
        head = f"{self.queue_name}_{self.start_time}_{self.end_time}"
        response = {"head": head, "body": results}
        compress_info = lzma.compress(json.dumps(response).encode())

        ch.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(
                correlation_id=properties.correlation_id
            ),
            body=compress_info
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def receive_message(self):
        credentials = pika.PlainCredentials('admin', 'Liuling123!')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='8.210.155.15', port=5672, virtual_host='yd', heartbeat=0,
                                      credentials=credentials))
        channel = connection.channel()
        channel.exchange_declare(exchange='yd', durable=True, exchange_type='fanout')
        result = channel.queue_declare(queue=self.queue_name, exclusive=True)
        channel.queue_bind(exchange='yd', queue=result.method.queue)
        channel.basic_consume(queue=result.method.queue, on_message_callback=self.on_request, auto_ack=False)
        channel.start_consuming()


if __name__ == '__main__':
    consumer = Consumer()
    consumer.receive_message()

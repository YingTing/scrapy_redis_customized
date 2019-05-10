import argparse
import os
import subprocess
import socket


IP_ALL = ("10.2.200.1", "10.2.200.2", "10.2.200.3", "10.2.200.4",
      "10.2.200.5", "10.2.200.6", "10.2.200.7", "10.2.200.8",
      "10.2.200.17")

IP = IP_ALL[:]

IMAGE_NAME = "gym_spider"
CONTAINER_NAME = "gym_spider{}_" + socket.gethostname()
IMAGE_NAME_DEV = "gym_spider_dev"
DB_NAME = "gym_goods_db"
DB_IP = "10.2.5.226"
DB_NAME_STAGING = "gym_goods_db_staging"
DB_IP_STAGING = "10.2.6.1"
DB_STAGING_PORT = "3307"
PROXY_SERVER = "n019"
PROXY_SERVER_IP = "10.2.5.107"
PROXY_PORT = "3128"
PIPEWORK_PATH = "pipework/pipework"
SPIDERS = {
            "pchome": "-s DOWNLOAD_DELAY=6 ",
            "pcstore": "-s DOWNLOAD_DELAY=4",
            "books": "-s DOWNLOAD_DELAY=60",
            "epayless": "-s DOWNLOAD_DELAY=6",
            "myfone": "-s DOWNLOAD_DELAY=40",
            "ruten": "-s DOWNLOAD_DELAY=180",
            "momomall": "-s DOWNLOAD_DELAY=20",
            "eslite": "-s DOWNLOAD_DELAY=20",
            "tkec": "-s DOWNLOAD_DELAY=6",
            "senao": "-s DOWNLOAD_DELAY=20",
            "eclife": "-s DOWNLOAD_DELAY=60",
            "sanjing3c": "-s DOWNLOAD_DELAY=40",
            "3c3c": "-s DOWNLOAD_DELAY=40",
            "rt-mart": "-s DOWNLOAD_DELAY=20",
            "etungo": "-s DOWNLOAD_DELAY=20",
            "biggo": "-s DOWNLOAD_DELAY=20"
            }

DATAFEEDS = {
    "yahoo_buy_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False --loglevel INFO',
    "yahoo_mall_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False --loglevel INFO',
    "yahoo_bid_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False --loglevel INFO',
    "shopee_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False --loglevel INFO',
    "rakuten_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False -s CONCURRENT_ITEMS=300 --loglevel INFO',
    "kuobrother_datafeed_section": '-s CONCURRENT_REQUESTS=3 -s AUTOTHROTTLE_ENABLED=False --loglevel INFO',
    "momo_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False -s CONCURRENT_ITEMS=300 --loglevel INFO',
    "trplus_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False -s CONCURRENT_ITEMS=300 --loglevel INFO',
    "friday_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False -s CONCURRENT_ITEMS=300 --loglevel INFO',
    "umall_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False -s CONCURRENT_ITEMS=300 --loglevel INFO',
    "obdesign_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False -s CONCURRENT_ITEMS=300 --loglevel INFO',
    "ibonmart_datafeed": '-s CONCURRENT_REQUESTS=1 -s AUTOTHROTTLE_ENABLED=False -s CONCURRENT_ITEMS=300 --loglevel INFO'
}

DISPATCHERS = {
    "search_dispatcher": '-s AUTOTHROTTLE_ENABLED=False',
    "mall_item_url": '-s AUTOTHROTTLE_ENABLED=False',
    "item_group": '-s AUTOTHROTTLE_ENABLED=False'
}

def create_net(args):
    os.system("sudo docker network create crawler-net")

def _enumerate_params(ips=IP):
    for i, ip in enumerate(IP):
        yield dict(globals(),
            container=CONTAINER_NAME.format(i),
            ip=ip)

def get_cmd_output(cmd):
    out = subprocess.check_output(cmd, shell=True)
    return out.decode('utf-8').strip()

def get_spider_version():
    return get_cmd_output("git show -s --format=%cd --date=format:%Y%m%d%H%M%S HEAD")
'''
def prod(args):
    os.system("sudo docker build -f Dockerfile.prod -t {} .".format(IMAGE_NAME))
    spider_version = get_spider_version()
    for p in _enumerate_params():
        os.system("sudo docker stop {container}".format(**p))
        os.system("sudo docker rm {container}".format(**p))
        os.system("sudo docker run --rm -h {container} --name {container} -d --network crawler-net "
                  "--cap-add=NET_ADMIN --add-host={DB_NAME}:{DB_IP} --add-host={PROXY_SERVER}:{PROXY_SERVER_IP} "
                  "-e SPIDER_VERSION={SPIDER_VERSION} {IMAGE_NAME}".format(SPIDER_VERSION=spider_version, **p))
        os.system("sudo {PIPEWORK_PATH} eth0 {container} {ip}/16".format(**p))
        docker_internal_ip = get_cmd_output('sudo docker exec {container} ip route | grep default | cut -d " " -f 3'.format(**p))
        os.system("sudo docker exec {container} ip route del default via {internal_ip} dev eth0".format(internal_ip=docker_internal_ip, **p))
        os.system("sudo docker exec {container} ip route add default via 10.2.254.1 dev eth1".format(**p))
'''
def prod(args):
    os.system("sudo docker build -f Dockerfile.prod -t {} .".format(IMAGE_NAME))
    spider_version = get_spider_version()
    for index in range(args.num_containers):
        container_name = CONTAINER_NAME.format(index)
        os.system("sudo docker stop {}".format(container_name))
        os.system("sudo docker rm {}".format(container_name))
        os.system("sudo docker run --rm -h {container} --name {container} -d --cpus=0.9 --memory=7G "
                  "--cap-add=NET_ADMIN --add-host={DB_NAME}:{DB_IP} --add-host={PROXY_SERVER}:{PROXY_SERVER_IP} "
                  "--add-host=crawler-redis:10.2.5.225 "
                  "-e SPIDER_VERSION={SPIDER_VERSION} -e http_proxy={PROXY_SERVER_IP}:{PROXY_PORT} -e https_proxy={PROXY_SERVER_IP}:{PROXY_PORT} {IMAGE_NAME}"\
                  .format(container=container_name, SPIDER_VERSION=spider_version, \
                        DB_NAME=DB_NAME, DB_IP=DB_IP, PROXY_SERVER=PROXY_SERVER, PROXY_SERVER_IP=PROXY_SERVER_IP, \
                        PROXY_PORT=PROXY_PORT, IMAGE_NAME=IMAGE_NAME))

def prod2(args):
    os.system("sudo docker build -f Dockerfile.prod -t {} .".format(IMAGE_NAME))
    spider_version = get_spider_version()
    for index in range(args.num_containers):
        container_name = CONTAINER_NAME.format(index)
        os.system("sudo docker stop {}".format(container_name))
        os.system("sudo docker rm {}".format(container_name))
        os.system("sudo docker run --rm -h {container} --name {container} -d "
                  "--add-host=crawler-redis:10.2.5.225 --add-host={DB_NAME}:{DB_IP} "
                  "-e SPIDER_VERSION={SPIDER_VERSION} {IMAGE_NAME}".format(
                    container=container_name, SPIDER_VERSION=spider_version,
                     DB_NAME=DB_NAME, DB_IP=DB_IP, IMAGE_NAME=IMAGE_NAME))
    
def stop_containers(args):
    for index in range(args.num_containers):
        container_name = CONTAINER_NAME.format(index)
        os.system("sudo docker stop {}".format(container_name))

def dev(args):
    os.system("sudo docker build -f Dockerfile.dev -t {} .".format(IMAGE_NAME_DEV))
    spider_version = get_spider_version()
    options = SPIDERS.get(args.spider, '')
    container = 'gym_spider_dev'
    cwd = os.getcwd()
    os.system("sudo docker run --name {} --rm --net=host -e SPIDER_VERSION={} "
              "--mount \"type=bind,source={},target=/usr/src/app\" "
              "{} scrapy crawl {} {} --loglevel DEBUG"
                    .format(container, spider_version, cwd, IMAGE_NAME_DEV, args.spider, options))

def start_spider(spider, container):
    cmd = "sudo docker exec {container} scrapy crawl {spider}".format(
                container=container,
                spider=spider)
    print(cmd)
    os.system(cmd)

def crawl(args):
    #for p in _enumerate_params():
    for p in range(args.num_containers):
        spiders = SPIDERS.keys() if args.spider == 'all' else [args.spider#    default_spiders = ['pchome', 'pcstore', 'books', 'epayless', 'ruten']
]
        for s in spiders:
            options = SPIDERS.get(s, '')
            #cmd = "sudo docker exec -it {container} scrapy crawl {spider} " \
            #      "{options}".format(spider=s, options=options, container=CONTAINER_NAME.format(p))
            cmd = "sudo docker exec -d {container} scrapy crawl {spider} " \
                  "{options} --logfile {spider}.log".format(spider=s, options=options, container=CONTAINER_NAME.format(p))
            print(cmd)
            os.system(cmd)

def datafeed(args):
    for index in range(args.num_containers):
        #spiders = DATAFEEDS.keys() if args.spider == 'all' else args.spider.split(' ')
        spiders = DATAFEEDS.keys() if args.spider == 'all' else args.spider
        container_name = CONTAINER_NAME.format(index)
        for s in spiders:
            options = DATAFEEDS.get(s, '')            
            #cmd = "sudo docker exec -it {container} scrapy crawl {spider} " \
            #      "{options}".format(container=container_name, spider=s, options=options)
            cmd = "sudo docker exec -d {container} scrapy crawl {spider} " \
                  "{options} --logfile {spider}.log".format(container=container_name, spider=s, options=options)
            print(cmd)
            os.system(cmd)

def dispatch(args):
    container = CONTAINER_NAME.format(args.container_index)
    print('container: ' + container)
    dispatchers = DISPATCHERS.keys() if args.spider == 'all' else [args.spider]
    for s in dispatchers:
        options = DISPATCHERS.get(s, '')
        cmd = "sudo docker exec -d {container} scrapy crawl {spider} " \
                  "{options} --logfile {spider}.log".format(
                        container=container,
                        spider=s, options=options)
        print(cmd)
        os.system(cmd)

def read_log(args):
    os.system("sudo docker exec {container} tail -f {spider}.log".format(
                    container=CONTAINER_NAME.format(args.container_index),
                    spider=args.spider))

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_net = subparsers.add_parser('create-net', help='create docker network')
    parser_net.set_defaults(func=create_net)

    parser_prod = subparsers.add_parser('prod', help='prod build')
    parser_prod.add_argument("num_containers", type=int)
    parser_prod.set_defaults(func=prod)

    parser_prod2 = subparsers.add_parser('prod2', help='prod build (no ip bind)')
    parser_prod2.add_argument("num_containers", type=int)
    parser_prod2.set_defaults(func=prod2)

    parser_stop = subparsers.add_parser('stop', help='stop containers')
    parser_stop.add_argument("num_containers", type=int)
    parser_stop.set_defaults(func=stop_containers)#    default_spiders = ['pchome', 'pcstore', 'books', 'epayless', 'ruten']

    parser_dev = subparsers.add_parser('dev', help="dev build")
    parser_dev.add_argument("spider")
    parser_dev.set_defaults(func=dev)

    parser_dp = subparsers.add_parser('dispatch', help='run dispatcher jobs')
    parser_dp.add_argument("-c", "--container-index", type=int, dest="container_index",
                        default=0, help='container index (0~)')
    parser_dp.add_argument("spider", help='"all" for all dispatchers')
    parser_dp.set_defaults(func=dispatch)

    parser_crawl = subparsers.add_parser('crawl', help='run spider jobs')
    parser_crawl.add_argument("num_containers", type=int)
    parser_crawl.add_argument("spider", help='"all" for all spiders')
    parser_crawl.set_defaults(func=crawl)

    parser_df = subparsers.add_parser('datafeed', help='run datafeed jobs')
    parser_df.add_argument("num_containers", type=int)
    parser_df.add_argument("spider", help='"all" for all spiders(datafeeds)', nargs='+')
    parser_df.set_defaults(func=datafeed)

    parser_log = subparsers.add_parser('log', help='read log from spider')
    parser_log.add_argument("-c", "--container-index", type=int, dest="container_index",
                        default=0, help='container index (0~)')
    parser_log.add_argument("spider", help='spider name such as pchome')
    parser_log.set_defaults(func=read_log)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

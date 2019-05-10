import argparse
from urllib.request import urlopen

from redis import StrictRedis
import json
from datetime import datetime, timedelta
from ftplib import FTP, FTP_TLS
import os.path
import time
from time import strftime, gmtime
import logging
from run_show_status import show_status
import run_utility as util

logger = logging.getLogger(__name__)
mylog = print

update_faster_mall = []
utc_time_mall = []
local_time_mall = ['ibonmart']

# entry point from run_all.py
def check_and_push_new_files(args):
    redis_server = StrictRedis(host=args.redis_host, port=args.redis_port, db=args.redis_db)
    mall = args.mall_name
    datafeed_dict = util.datafeed["datafeeds"]
    # time_record for shopee，紀錄強制更新 datafeed 時，last_modified 的時間
    time_record = None

    while True:
        for m in mall:
            if datafeed_dict.get(m):
                data = datafeed_dict[m]
                if m == "trplus":
                    _check_and_push_new_files(args, data, m, redis_server, util.parse_dirtime(args.datatime_after), args.push)
                elif m == "shopee":
                    time_record = _check_and_push_new_files(args, data, m, redis_server, util.parse_dirtime_as_utc(args.datatime_after), args.push, time_record = time_record)
                else:
                    _check_and_push_new_files(args, data, m, redis_server, util.parse_dirtime_as_utc(args.datatime_after), args.push)

        if args.repeat == 0:
            break
        time.sleep(args.repeat)

def _check_and_push_new_files(args, data, mall, redis_server, datatime_after, push_to_redis, check_period_secs=0, time_record = None):
    # get mall's redis status key and redis key
    if mall == "yahoo":
        full_or_partial = 'full' if args.full else 'partial'
        redis_key = util.get_redis_key_for_status(mall, args.mall_property, full_or_partial)
        datafeed_redis_key = util.get_datafeed_redis_key(mall, args.mall_property)
    elif mall == "umall":
        full_or_partial = 'full' if args.full else 'partial'
        redis_key = util.get_redis_key_for_status(mall, full_or_partial=full_or_partial)
        datafeed_redis_key = util.get_datafeed_redis_key(mall)
    else:
        redis_key = util.get_redis_key_for_status(mall)
        datafeed_redis_key = util.get_datafeed_redis_key(mall)
    
    print('repeat:{}, datafeed_redis_key:{}'.format(args.repeat, datafeed_redis_key))

    if args.clean_key:
        print('clean key:' + redis_key)
        redis_server.delete(redis_key)

    # load mall's status in json from redis
    status_json = redis_server.get(redis_key)
    if status_json:
        last_update_status = json.loads(status_json.decode('utf-8'))
    else:
        last_update_status = {}

    file_status_dict = last_update_status.setdefault('file_status', {})
    check_time_after = datetime.utcnow() - timedelta(seconds=check_period_secs)

    if check_time_after > datatime_after:
        check_time_after = datatime_after

    # FTP general case with .xml file
    if mall in ("friday", "momo", "rakuten", "umall", "ibonmart", "obdesign"):
        try:
            ftp = FTP(host=data["FTP_HOST"], user=data["FTP_USER"], passwd=data["FTP_PASSWORD"])
        except Exception as e:
            time.sleep(3)

            # re-connect to FTP server
            ftp = FTP(host=data["FTP_HOST"], user=data["FTP_USER"], passwd=data["FTP_PASSWORD"])

        ftp.cwd(data["FILE_PATH"])

        # full or partial data are stored in different folder
        if mall == "umall":
            root_path = util.make_root_path(mall, full_or_partial=full_or_partial)
            ftp.cwd(root_path)

        for d in ftp.nlst():
            head, tail = os.path.split(d)

            # rakuten or ibonmart FTP file path
            if (tail.startswith('ALL') and tail.endswith('.zip')) or \
                (tail.startswith('41_ibonmart_item') and tail.endswith('.ZIP')):
                ftp_path = 'ftp://' + data["FTP_HOST"] + '/' + d
            # GYM FTP server file path (momo, friday, obdesign)
            elif (tail.startswith('momoshop') and tail.endswith('.xml')) or \
                (tail.startswith('49_gohappy_item') and tail.endswith('.bak')) or \
                (tail.startswith('products_m61') and tail.endswith('.xml')):
                ftp_path = 'ftp://' + data["FTP_USER"] + '@' + data["FTP_HOST"] + data["FILE_PATH"] + d
            # GYM FTP server file path full_or_partial(umall)
            # umall 可能在一分鐘內，datafeed 從 .xml 變成 .bak，導致來不及 push to redis，所以改抓 .bak 檔
            elif tail.startswith('Product_All') and tail.endswith('.bak'):
                ftp_path = 'ftp://' + data["FTP_USER"] + '@' + data["FTP_HOST"] + data["FILE_PATH"] + root_path + d
            # no match file format
            else:
                continue

            # MDTM file modification time
            mdtm_status, last_modified = ftp.sendcmd('MDTM ' + d).split()
            if util.parse_filetime(last_modified) < check_time_after:
                continue
            else:
                pass
                # print(util.parse_filetime(last_modified))
                # print(check_time_after)

            ftp.voidcmd('TYPE I')  # binary mode for ftp.size()
            file_size = ftp.size(d)
 
            if util.check_file_is_new(file_status_dict.get(d), last_modified, file_size):
                updated = True
                now = datetime.utcnow()

                # now = utc+8 time : ibonmart
                if mall in local_time_mall:
                    now = now + timedelta(hours=8)

                sig_dict = {'last_modified': last_modified, 'size': file_size}
                signature = json.dumps(sig_dict)

                ## TODO 檔案更新速度較快的商城，來不及將 ftp datafeed push 進 redis 就被改檔名了.
                ## TODO check_file_is_new is always false
                
                # last_modified 更新已超過五分鐘
                if util.parse_filetime(last_modified) < now - timedelta(minutes=5):
                    if push_to_redis:
                        #redis_server.delete(datafeed_redis_key)
                        mylog('rpush {}, {}, {}'.format(datafeed_redis_key, ftp_path, signature))
                        redis_server.rpush(datafeed_redis_key, ftp_path)
                    else:
                        mylog('new {}, {}'.format(ftp_path, signature))

                    file_status_dict[d] = sig_dict
                else:
                    mylog('## {}, {}, {}'.format(d, util.format_filetime(now), signature))

        ftp.quit()

    # FTP special case (files in YYYYMM/ folder)
    elif mall in ("trplus"):
        try:
            ftp = FTP(host=data["FTP_HOST"], user=data["FTP_USER"], passwd=data["FTP_PASSWORD"])
        except Exception as e:
            time.sleep(3)

            # re-connect to FTP server
            ftp = FTP(host=data["FTP_HOST"], user=data["FTP_USER"], passwd=data["FTP_PASSWORD"])
            
        now = datetime.now()
        current_year = str(now.year)
        current_month = now.month
        user_month = datatime_after.month
        user_month_bool = False
        if user_month < current_month:
            user_month_bool = True
            if user_month < 10:
                str_user_month = "0" + str(user_month)
            else:
                str_user_month = str(current_month)
            ftp.cwd(data["FILE_PATH"] + current_year + str_user_month)
        else:
            if current_month < 10:
                str_current_month = "0" + str(current_month)
            else:
                str_current_month = str(current_month)
            ftp.cwd(data["FILE_PATH"] + current_year + str_current_month)

        for d in ftp.nlst():
            head, tail = os.path.split(d)
            if tail.endswith('.xml'):
                # MDTM file modification time
                mdtm_status, last_modified = ftp.sendcmd('MDTM ' + d).split()
                
                if util.parse_filetime(last_modified) < check_time_after:
                    continue
                ftp.voidcmd('TYPE I')  # binary mode for ftp.size()
                file_size = ftp.size(d)

                if util.check_file_is_new(file_status_dict.get(d), last_modified, file_size):
                    updated = True
                    now = datetime.utcnow()

                    sig_dict = {'last_modified': last_modified, 'size': file_size}
                    signature = json.dumps(sig_dict)

                    if util.parse_filetime(last_modified) < now - timedelta(minutes=2):
                        if push_to_redis:
                            mylog('DELETE ALL!!! and push {}, {}, {}'.format(datafeed_redis_key, d, signature))
                            redis_server.delete(datafeed_redis_key)
                            folder_path = current_year + str_current_month
                            file_path_tmp = 'ftp://{}@{}/bandq/{}/xml/{}/{}'
                            ftp_path = file_path_tmp.format(data["FTP_USER"], data["FTP_HOST"], mall, folder_path, d)
                            redis_server.rpush(datafeed_redis_key, ftp_path)
                        else:
                            mylog('new {}, {}'.format(d, signature))

                        file_status_dict[d] = sig_dict
                    else:
                        mylog('## {}, {}, {}'.format(d, util.format_filetime(now), signature))

        if user_month_bool:
            if current_month < 10:
                str_current_month = "0" + str(current_month)
            else:
                str_current_month = str(current_month)
            ftp.cwd(data["FILE_PATH"] + current_year + str_current_month)
            for d in ftp.nlst():
                head, tail = os.path.split(d)
                if tail.endswith('.xml'):
                    # MDTM file modification time
                    mdtm_status, last_modified = ftp.sendcmd('MDTM ' + d).split()

                    if util.parse_filetime(last_modified) < check_time_after:
                        continue
                    ftp.voidcmd('TYPE I')  # binary mode for ftp.size()
                    file_size = ftp.size(d)

                    if util.check_file_is_new(file_status_dict.get(d), last_modified, file_size):
                        updated = True
                        now = datetime.utcnow()

                        sig_dict = {'last_modified': last_modified, 'size': file_size}
                        signature = json.dumps(sig_dict)

                        if util.parse_filetime(last_modified) < now - timedelta(minutes=2):
                            if push_to_redis:
                                mylog('DELETE ALL!!! and push {}, {}, {}'.format(datafeed_redis_key, d, signature))
                                redis_server.delete(datafeed_redis_key)
                                ftp_path = 'ftp://' + data["FTP_USER"] + '@' + data["FTP_HOST"] + '/' + mall + '/' + d
                                redis_server.rpush(datafeed_redis_key, ftp_path)
                            else:
                                mylog('new {}, {}'.format(d, signature))

                            file_status_dict[d] = sig_dict
                        else:
                            mylog('## {}, {}, {}'.format(d, util.format_filetime(now), signature))
        ftp.quit()

    # Yahoo FTP server (mall separated by buy/mall/bid)
    elif mall in ("yahoo"):
        try:
            ftp = FTP_TLS(host=data["FTP_HOST"], user=data["FTP_USER"], passwd=data["FTP_PASSWORD"])
            ftp.prot_p()
        except Exception as error:
            mylog('FTP server connection error happened: {}'.format(error))
            time.sleep(3)

            # re-connect to FTP server
            ftp = FTP_TLS(host=data["FTP_HOST"], user=data["FTP_USER"], passwd=data["FTP_PASSWORD"])
            ftp.prot_p()

        root_path = util.make_root_path(mall, args.mall_property, full_or_partial)
        ftp.cwd(root_path)
        for d in ftp.nlst():
            time = util.parse_dirtime_yahoo(d)

            if time < check_time_after:
                continue

            for fn in ftp.nlst(d):
                head, tail = os.path.split(fn)
                if tail.startswith(args.mall_property) and tail.endswith('.gzip'):
                    # MDTM file modification time
                    mdtm_status, last_modified = ftp.sendcmd('MDTM ' + fn).split()
                    if util.parse_filetime(last_modified) < check_time_after:
                        continue

                    ftp.voidcmd('TYPE I')  # binary mode for ftp.size()
                    file_size = ftp.size(fn)

                    if util.check_file_is_new(file_status_dict.get(fn), last_modified, file_size):
                        path = root_path + '/' + fn
                        updated = True
                        now = datetime.utcnow()

                        sig_dict = {'last_modified': last_modified, 'size': file_size}
                        signature = json.dumps(sig_dict)

                        # print(util.parse_filetime(last_modified))
                        # print(now - timedelta(minutes=5))
                        if util.parse_filetime(last_modified) < now - timedelta(minutes=5):
                            if push_to_redis:
                                # TODO
                                # 在看到 000000.json.gzip 時，把此 redis-key 的舊資料清空
                                redis_server.rpush(datafeed_redis_key, 'ftp://' + path)
                                mylog('push {}, {}, {}'.format(datafeed_redis_key, path, signature))
                            else:
                                mylog('new {}, {}'.format(path, signature))

                            file_status_dict[fn] = sig_dict
                        else:
                            mylog('## {}, {}, {}'.format(path, util.format_filetime(now), signature))
        ftp.quit()

    # shopee check the CDN and determine the datafeed is ok while completely finish updating
    elif mall == "shopee":
        push_list = []

        for i, u in enumerate(data["urls"]):
            try:
                with urlopen(u) as site:
                    meta = site.info()
                    """ meta content
                        Server: nginx/1.12.2
                        Date: Thu, 24 Jan 2019 09:56:15 GMT
                        Content-Type: application/gzip
                        Transfer-Encoding: chunked
                        Connection: close
                        x-amz-id-2: UNFT1B/3y8DAGqQotwzURQr8kz4oFd2btlpIf3LNxFTegrm6rjQQiFNLuOHEBtMQPEqnqRwh/7s=
                        x-amz-request-id: 16027A990B82FE04
                        Last-Modified: Thu, 24 Jan 2019 08:09:57 GMT
                        ETag: W/"1b825e17dffd9f6e0192697bd9c2cfba-1"
                        X-Origin: S3GZ
                        X-Robots-Tag: noindex
                    """
                    # An identifier for a specific version of a resource, often a message digest
                    etag = meta.get("ETag")
                    # The last modified date for the requested object (in "HTTP-date" format as defined by RFC 7231)
                    last_modified = meta.get("Last-Modified")

                    if util.parse_gmttime(last_modified) < check_time_after:
                        continue

                    head, fn = os.path.split(u)
                    if util.check_file_is_new_url(file_status_dict.get(fn), etag, last_modified):
                        now = datetime.utcnow()
                        sig_dict = {'etag': etag, 'last_modified': last_modified}

                        # because of CDN: we need more time to check
                        if util.parse_gmttime(last_modified) < now - timedelta(hours=5):
                            push_list.append([u, sig_dict])
                        else:
                            mylog('## {}, {}, {}'.format(u, util.format_filetime(now), json.dumps(sig_dict)))
            # avoid connection error =>
            # urllib.error.URLError: <urlopen error [Errno 104] Connection reset by peer>
            except Exception as error:
                mylog('An error happened: {}'.format(error))
                continue

        waiting_set = set(redis_server.lrange(datafeed_redis_key, 0, -1))

        if time_record is None:
            time_record = last_modified

        mylog('~~~ last_modified: {}'.format(util.parse_gmttime(last_modified)))
        mylog('~~~ time_record: {}'.format(util.parse_gmttime(time_record)))

        updated = False
        if len(push_list) == len(data["urls"]):
            mylog('all urls are ready')
            for u, sig_dict in push_list:
                signature = json.dumps(sig_dict)
                file_status_dict[fn] = sig_dict
                if push_to_redis:
                    if u in waiting_set:
                        mylog('already in waiting list: {}, {}'.format(u, signature))
                    else:
                        redis_server.rpush(datafeed_redis_key, u)
                        mylog('push {}, {}'.format(u, signature))
                else:
                    mylog('new {}, {}'.format(u, signature))
                    
                # update time_record
                time_record = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime())
            updated = True

        # 超過 25 小時沒更新，則全部強制更新，避免 spider 三天後全部下架
        elif util.parse_gmttime(time_record) < datetime.utcnow() - timedelta(hours=25):

            if push_to_redis:
                for u in data["urls"]:
                    if u in waiting_set:
                        mylog('already in waiting list: {}'.format(u))
                    else:
                        redis_server.rpush(datafeed_redis_key, u)
                        mylog('Forcibly push {}'.format(u))
                updated = True

            # update time_record
            time_record = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime())

        if updated:
            mylog("###### " + util.format_filetime(datetime.utcnow()) + " #######")
        last_update_status['timestamp'] = util.format_filetime(datetime.utcnow())

        redis_server.set(redis_key, json.dumps(last_update_status, sort_keys=True))
        return time_record

    elif mall == "books":
        json_data = {"url": data["urls"][0], "callback": "parse_top100", "dont_filter": "True"}
        json_data = json.dumps(json_data)

        score = data.get('score', 1.0)

        if push_to_redis:
            redis_server.zincrby(datafeed_redis_key, json_data, score)
            mylog('zincrby {}, {}, {}'.format(datafeed_redis_key, score, json_data))
        else:
            mylog('new {}'.format(json_data))

    elif mall == "kuobrother_section" or mall == "kuobrother":
        if push_to_redis:
            for url in data["urls"]:
                redis_server.rpush(datafeed_redis_key, url)
                mylog('push {}, {}'.format(datafeed_redis_key, url))
        else:
            for url in data["urls"]:
                mylog('new {}'.format(url))

    # save mall spider status to redis
    mylog("###### " + util.format_filetime(datetime.utcnow()) + " #######")
    last_update_status['timestamp'] = util.format_filetime(datetime.utcnow())
    last_update_status['check_time_after'] = util.format_filetime(datetime.utcnow())

    redis_server.set(redis_key, json.dumps(last_update_status, sort_keys=True))
    
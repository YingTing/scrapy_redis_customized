import argparse
import logging
import json
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)
mylog = print

# docker entry folder is "/usr/src/app/product/" 
# but datafeed_info.json at "/usr/src/app/"
datafeed_json_path = '../datafeed_info.json'
with open(datafeed_json_path) as f:
    datafeed= json.load(f)
    FTP_info = dict()

def parse_dirtime_yahoo(s):
    return datetime.strptime(s, '%Y%m%d%H')

# compatible for yahoo datafeed time format
# and convert the local time to utc time
def parse_dirtime_as_utc(s):
    try:
        return datetime.strptime(s, '%Y%m%d') - timedelta(hours=8)
    except Exception as error:
        return datetime.strptime(s, '%Y%m%d%H') - timedelta(hours=8)

# compatible for yahoo datafeed time format
def parse_dirtime(s):
    try:
        return datetime.strptime(s, '%Y%m%d')
    except Exception as error:
        return datetime.strptime(s, '%Y%m%d%H')

# FTP command "MDTM FILE_NAME" will convert the "last modify time"(utc+8) to utc time
def parse_filetime(s):
    return datetime.strptime(s, '%Y%m%d%H%M%S')

def parse_gmttime(s):
    return datetime.strptime(s, "%a, %d %b %Y %H:%M:%S GMT")

def format_filetime(t):
    return t.strftime('%Y%m%d%H%M%S')

# def get_datafeed_redis_key_yahoo(yahoo_property):
#     return 'yahoo_{}:queue'.format(yahoo_property)

def get_datafeed_redis_key(mall_name, mall_property=None):
    # 博客來不是真正的 datafeed，所以 redis-key 是 priqueue 不是 queue
    if mall_name == 'books':
        return '{}:priqueue'.format(mall_name)
    elif mall_property:
        return '{}_{}:queue'.format(mall_name, mall_property)
    else:
        return '{}:queue'.format(mall_name)

# def get_redis_key_for_status(mall_name):
#     return 'data:signature:{}'.format(mall_name)

# def get_redis_key_for_status_yahoo(yahoo_property, full_or_partial):
#     return 'data:signature:yahoo' + '_' + yahoo_property + '_' + full_or_partial

def get_redis_key_for_status(mall_name, mall_property=None, full_or_partial=None):
    # yahoo
    if mall_property and full_or_partial:
        return 'data:signature:' + mall_name + '_' + mall_property + '_' + full_or_partial
    # 
    elif mall_property:
        return 'data:signature:' + mall_name + '_' + mall_property
    # umall
    elif full_or_partial:
        return 'data:signature:' + mall_name + '_' + full_or_partial
    # general case
    else:
        return 'data:signature:{}'.format(mall_name)

def check_file_is_new(file_status, last_modified, file_size):
    if file_status:
        t = file_status.get('last_modified')
        s = file_status.get('size')
        return last_modified != t or s != file_size
    return True

def check_file_is_new_url(file_status, etag, last_modified):
    if file_status:
        e = file_status.get('etag')
        t = file_status.get('last_modified')
        # CDN may have different version, so we need to check time
        return parse_gmttime(last_modified) > parse_gmttime(t) and e != etag
    return True

def make_root_path(mall, mall_property=None, full_or_partial=None):
    if mall == "yahoo":
        return '/{}/{}'.format(mall_property, full_or_partial)
    elif mall == "umall":
        if full_or_partial == "full":
            return 'allproduct/'
        else:
            return 'partialproduct/'

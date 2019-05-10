# -*- coding: utf-8 -*-

from datetime import datetime
import json
from scrapy.http.request import Request
from .priority_queue_spider import PriorityQueueSpider
from redis import StrictRedis
from ..dupefilter_redis import RedisDupeFilter
from ..elastic import Elastic


class MallSpider(PriorityQueueSpider):

    crawler_type = 'spider'
    item_url_driver_download = False

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        inst = super(MallSpider, cls).from_crawler(crawler, *args, **kwargs)
        inst.max_result_page_items = 0
        
        settings = crawler.settings
        host = settings.get('DUPEFILTER_REDIS_HOST')
        port = settings.getint('DUPEFILTER_REDIS_PORT', 6379)
        db = settings.getint('DUPEFILTER_REDIS_DB', 0)

        inst.default_num_itemurl_needed = settings.get('MALLSPIDER_DEFAULT_NUM_ITEMURL_NEEDED', 50)
        inst.zero_query_result_ttl_secs = settings.getint('DUPEFILTER_ZERO_QUERY_RESULT_TTL_SECS', 86400*7)
        inst.ttl_redis_server = StrictRedis(host=host, port=port, db=db, encoding='utf-8')
        inst.elastic = Elastic.from_settings(settings)

        return inst

    def process_req_json(self, req_json):
        keyword = req_json.get('keyword')
        if keyword:
            del req_json['keyword']
            return self.process_query_request(keyword, req_json)
        elif self.item_url_driver_download:
            if req_json.get('callback') == 'parse_item':
                meta = req_json.setdefault('meta', {})
                meta['driver_download'] = 1     # driver download

        return req_json

    def process_query_request(self, keyword, req_json):
        req_json['url'] = self.make_query_url(keyword, 1)
        meta = req_json.setdefault('meta', {})
        meta['keyword'] = keyword
        meta['page_num'] = 1
        meta['num_itemurl_collected'] = 0
        return req_json

    def make_query_url(self, keyword, page_num):
        raise NotImplementedError        

    def next_query(self, request, count):
        meta = request.meta
        num_itemurl_collected = meta['num_itemurl_collected']
        next_page = meta['page_num'] + 1
        return request.replace(
                           url=self.make_query_url(meta['keyword'], next_page),
                           meta=dict(meta,
                                     page_num=next_page,
                                     num_itemurl_collected=num_itemurl_collected+count)
                           )

    def check_if_next_query_needed(self, request, count, total_count=10000):
        if count == 0 and request.meta['page_num'] == 1:
            fp = RedisDupeFilter.get_fingerprint(request)
            self.ttl_redis_server.setex(fp, self.zero_query_result_ttl_secs, '1')

        if count <= 0 or count < self.max_result_page_items:
            return False  # probably no next page since count < max_result_page_items

        self.max_result_page_items = max(count, self.max_result_page_items)

        num_itemurl_collected = request.meta['num_itemurl_collected']
        num_itemurl_needed = request.meta.get('num_itemurl_needed', self.default_num_itemurl_needed)
        max_itemurl_needed = min(total_count, getattr(self, 'max_itemurl_needed', 10000))
        if num_itemurl_needed > max_itemurl_needed:
            num_itemurl_needed = max_itemurl_needed
        return num_itemurl_collected + count < num_itemurl_needed

    def check_if_next_item_needed(self, count):
        num_itemurl_needed = min(getattr(self, 'max_itemurl_needed', 10000), self.default_num_itemurl_needed)
        return count <= num_itemurl_needed

    def elastic_post(self, response, count):
        if self.elastic and response.meta['page_num'] == 1:
            # only first page result to elasticsearch
            self.elastic.post(self.name, url=response.url, req_meta=response.meta, count=count)

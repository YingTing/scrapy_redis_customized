# -*- coding: utf-8 -*-

from urllib.parse import quote
from .priority_queue_spider import PriorityQueueSpider


class MallItemUrlDispatcher(PriorityQueueSpider):

    crawler_type = 'dispatcher'
  
    name = 'mall_item_url'
    redis_key = PriorityQueueSpider.get_redis_key(name)

    download_delay = 0

    custom_settings = {
        # disable dupefilter. Leave filtering to mallspider
        'DUPEFILTER_CLASS': 'product_spider.dupefilter_redis.MyBaseDupeFilter'
    }

    def process_req_json(self, req_json):
        url = req_json['url']
        
        req_json['url'] = "dispatch://" + quote(url)
        meta = req_json.setdefault('meta', {})
        meta['dispatch'] = 'item_url'
        meta['item_url'] = url

        mall_id = req_json.get('mall_id')
        if mall_id:
            meta['mall_id'] = mall_id
            del req_json['mall_id']

        source_type = req_json.get('source_type')
        if source_type:
            meta['source_type'] = source_type
            del req_json['source_type']

        return req_json

    def parse(self, response):
        pass

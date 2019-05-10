# -*- coding: utf-8 -*-

from .priority_queue_spider import PriorityQueueSpider
from scrapy.http.request import Request


class ItemGroupDispatcher(PriorityQueueSpider):

    crawler_type = 'dispatcher'
  
    name = 'item_group'
    redis_key = PriorityQueueSpider.get_redis_key(name)

    download_delay = 0

    custom_settings = {
        # disable dupefilter. Leave filtering to mallspider
        'DUPEFILTER_CLASS': 'product_spider.dupefilter_redis.MyBaseDupeFilter'
    }

    #def start_requests(self):
    #    yield Request(url="dispatch://group_id/51", meta={'dispatch':'item_group', 'group_id':'51'})

    def process_req_json(self, req_json):
        group_id = req_json['group_id']
        del req_json['group_id']
        
        req_json['url'] = "dispatch://group_id/" + str(group_id)

        meta = req_json.setdefault('meta', {})
        meta['dispatch'] = 'item_group'
        meta['group_id'] = group_id

        source_type = req_json.get('source_type')
        if source_type:
            meta['source_type'] = source_type
            del req_json['source_type']

        return req_json

    def parse(self, response):
        pass

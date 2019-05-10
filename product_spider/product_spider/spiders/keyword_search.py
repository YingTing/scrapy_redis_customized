# -*- coding: utf-8 -*-

import logging
from .priority_queue_spider import PriorityQueueSpider
from ..items import RawKeywordItem

class KeywordSearchDispatcher(PriorityQueueSpider):

    crawler_type = 'dispatcher'
    
    name = 'search_dispatcher'
    redis_key = PriorityQueueSpider.get_redis_key(name)
    
    download_delay = 0

    custom_settings = {
        # disable dupefilter. Leave filtering to mallspider
        'DUPEFILTER_CLASS': 'product_spider.dupefilter_redis.MyBaseDupeFilter',
        "ITEM_PIPELINES": {
            'product_spider.pipelines.GroupRawKeywordPipeline': 200
        }
    } 

    def process_req_json(self, req_json):
        keyword = req_json['keyword']
        del req_json['keyword']
        
        req_json['url'] = "dispatch://" + keyword
        
        meta = req_json.setdefault('meta', {})
        meta['dispatch'] = 'keyword'
        meta['keyword'] = keyword

        source_type = req_json.get('source_type')
        if source_type:
            meta['source_type'] = source_type
            del req_json['source_type']

        return req_json

    def parse(self, response):
        # keyword from gym api_search (gogomoney website search bar)
        gym_api_url = 'https://www.goyomoney.com.tw/search?queryText={}'
        metadata = response.meta
        keyword = metadata['keyword']

        # api_search brings meta['source_type']
        source_type = metadata.get('source_type')
        if source_type:
            if source_type == 'gym_query_kw':
                item = RawKeywordItem()
                item['word_type'] = 'keyword'
                item['word'] = keyword
                item['source_url'] = gym_api_url.format(keyword)
                item['source'] = 'api_search'

                yield item

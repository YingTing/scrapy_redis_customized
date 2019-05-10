import logging
import json
from scrapy.utils.misc import walk_modules
from scrapy.utils.spider import iter_spider_classes
from scrapy.http import Response
from product_spider.elastic import Elastic
from product_spider import get_timestamp_score
import requests

logger = logging.getLogger(__name__)

class DispatcherError(Exception):
    pass

class ItemGroupError(DispatcherError):
    pass

class DispatcherDownloadHandler(object):

    # for keyword search
    default_spiders = ['pchome', 'pcstore', 'books', 'epayless', \
        'ruten', 'momomall', 'eslite', 'myfone', 'tkec', \
        'senao', 'eclife', 'sanjing3c', '3c3c', 'rt-mart', 'etungo', 'biggo']

    # for item url search
    mapping = {
        10: "rakuten",
        15: "books",
        38: "pchome",
        39: "pchome",
        44: "epayless",
        47: "myfone",
        56: "ruten",
        57: "pcstore",
        63: "tkec",
        106: "momomall",
        108: "eslite",
        110: "senao",
        112: "eclife",
        113: "sanjing3c",
        114: "3c3c",
        116: "rt-mart",
        117: "etungo",
    }

    def __init__(self, settings):
        self.elastic = Elastic.from_settings(settings)

        # get spiders in /product_spider/spiders and /product_spider/datafeeds folder
        # and generate spider's redis_key into spiders_rediskey
        self.spiders_rediskey = {}
        spider_modules = settings.getlist('SPIDER_MODULES')
        for name in spider_modules:
            try:
                for module in walk_modules(name):
                    for spcls in iter_spider_classes(module):
                        if spcls.name and getattr(spcls, 'crawler_type', None) == 'spider' and \
                            hasattr(spcls, 'redis_key'):
                            self.spiders_rediskey[spcls.name] = spcls.redis_key
                            logger.info('search spider:%(name)s, key=%(key)s',
                                {'name':spcls.name, 'key':spcls.redis_key})
            except ImportError:
                pass
        self.req_session = requests.Session()
        self.group_items_api = settings.get('SEARCH_API') + '/se/spider-update/group/'
        self.group_items_cache_ttl = 86400

    def close(self):
        pass

    def download_request(self, request, spider):

        req_dict = dict()

        if request.dont_filter:
            req_dict['dont_filter'] = True

        meta = request.meta        

        score = meta.get('score')
        if score:
            del meta['score']
        else:
            score = 1

        dispatch_type = meta['dispatch']
        del meta['dispatch']
            
        flags = None

        # dispatch to keyword, item_url or item_group
        if dispatch_type == 'keyword':
            self.process_keyword(spider, req_dict, meta, score)
        elif dispatch_type == 'item_url':
            if not self.process_item_url(spider, req_dict, meta, score):
                flags = ['itemurl_ignored']
        elif dispatch_type == 'item_group':
            self.process_item_group(spider, req_dict, meta, score)
        else:
            raise DispatcherError("unknown dispatcher type: " + dispatch_type)

        return Response(url=request.url, request=request, flags = flags)

    def process_keyword(self, spider, req_dict, meta, score):
        keyword = meta.get('keyword')
        target_spiders = meta.get('spiders')
        if target_spiders is None:
            target_spiders = self.default_spiders
        else:
            del meta['spiders']

        if self.elastic:
            self.elastic.post(spider.name, req_meta=meta, count=0)

        req_dict['keyword'] = keyword
        # del meta['keyword']
        if len(meta) > 0:
            req_dict['meta'] = meta

        data = json.dumps(req_dict, sort_keys=True)
        score += get_timestamp_score()
        for k in target_spiders:
            spider.server.zincrby(self.spiders_rediskey[k], data, amount=score)

    def process_item_url(self, spider, req_dict, meta, score):        
        req_dict['meta'] = meta
        item_url = meta.get('item_url')
        if item_url:
            del meta['item_url']
            mall_id = meta['mall_id']
            del meta['mall_id']
            score += get_timestamp_score()
            self._add_item_url_to_queue(spider, mall_id, item_url, req_dict, score)

    def _add_item_url_to_queue(self, spider, mall_id, item_url, req_dict, score):
        spider_name = self.mapping.get(mall_id)
        if spider_name:
            redis_key = self.spiders_rediskey.get(spider_name)
            if redis_key:
                req_dict['url'] = item_url

                # NOTE: ['callback'] will be used to activate driver-download
                req_dict['callback'] = "parse_item"

                # 2018/4/19 make mall_item_url higher priority
                if score < 3:
                    score = 3
        
                data = json.dumps(req_dict, sort_keys=True)
                spider.server.zincrby(redis_key, data, amount=score)

                return True

    def process_item_group(self, spider, req_dict, meta, score):
        group_id = meta.get('group_id')
        assert group_id is not None

        group_redis_key = 'se:group_items:' + str(group_id)
        group_items_cache = spider.server.get(group_redis_key)
        
        if group_items_cache:
            group_items = json.loads(group_items_cache)
        else:
            r = self.req_session.get(url=self.group_items_api + str(group_id))
            if r.status_code != 200:
               raise ItemGroupError(r.status_code)
            group_items = r.json()
            spider.server.setex(group_redis_key, self.group_items_cache_ttl, r.text)

        score += get_timestamp_score()

        for item in group_items:
            self._add_item_url_to_queue(spider, item['mallId'], item['url'], req_dict, score)

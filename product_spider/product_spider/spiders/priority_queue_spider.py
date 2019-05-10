# -*- coding: utf-8 -*-

from time import sleep
from scrapy.spiders import Spider
from scrapy.http.request import Request
from scrapy_redis.spiders import RedisSpider
from scrapy_redis.utils import bytes_to_str
from scrapy_redis import connection, defaults
import json
from twisted.internet import task
from ..tsdb import TSDB
from product_spider import get_timestamp_score


class PriorityQueueSpider(RedisSpider):

    redis_batch_size = 30

    @staticmethod
    def get_redis_key(name):
        return name + ":priqueue"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        inst = super(PriorityQueueSpider, cls).from_crawler(crawler, *args, **kwargs)
        inst.json_input = crawler.settings.getbool('REDIS_START_URLS_JSON', False)
        inst.request_cls = Request        
        tsdb = TSDB.from_settings(crawler.settings)
        if tsdb:
            inst.tsdb = tsdb
            inst.loop_call = task.LoopingCall(inst._report_to_tsdb)
            inst.loop_call.start(30)  # 30 secs
        return inst

    def next_requests(self):
        found = 0
        # TODO: Use redis pipeline execution.
        while found < self.redis_batch_size:
            result = self.pop_with_highest_score()
            if not result:
                break
            data, score = result
            req = self.make_request_from_data(data)
            if req:
                req.meta['score'] = score
                yield req
                found += 1
        if found:
            self.logger.debug("Read %s requests from '%s'", found, self.redis_key)

    def pop_with_highest_score(self):
        pipe = self.server.pipeline()
        pipe.multi()

        # NOTE: sorted set is in ascending order, the highest score is at the end
        pipe.zrange(self.redis_key, -1, -1, withscores=True) \
            .zremrangebyrank(self.redis_key, -1, -1)

        results, count = pipe.execute()
        if results:
            return results[0]

    @classmethod
    def push_req_data(cls, server, data, score=1):
        # remember to use json.dumps(object, sort_keys=True) to ensure keys order!
        # print('{} {}'.format(cls.redis_key, data))
        server.zincrby(cls.redis_key, data, amount=score + get_timestamp_score())

    # override make_request_from_data to support json input
    def make_request_from_data(self, data):
        """
            json_input supports all members of Request
        """
        req_data = bytes_to_str(data, self.redis_encoding)
        if self.json_input:
            req_json = self.process_req_json(json.loads(req_data))
            return self.request_from_json(req_json)

        return self.request_cls(url=req_data)

    def process_req_json(self, req_json):
        return req_json

    def request_from_json(self, req_json, request_cls=Request):
        url = req_json['url']
        del req_json['url']
        self.set_callable_if_exists('callback', req_json)
        self.set_callable_if_exists('errback', req_json)
        
        # "[" and "]" will make ValueError("Invalid IPv6 URL")
        url.replace('[', '').replace(']', '')

        # interrupt search_dispatcher if ValueError("Invalid IPv6 URL")
        try:
            return self.request_cls(url, **req_json)
        except ValueError:
            return None
        except Exception as error:
            if self.elastic:
                raise self.elastic.post(self.name, url=url,
                                    error_status=1, error_type=error.__class__.__name__,
                                    error_msg=str(error))

    def set_callable_if_exists(self, name, req_json):
        cb = req_json.get(name)
        if cb is not None and not callable(cb):
            req_json[name] = getattr(self, cb)

    def _report_to_tsdb(self):
        self.tsdb.send('gym.crawler.redis_queue_size',
                        self.server.zcard(self.redis_key),
                        [('spider', self.name)])

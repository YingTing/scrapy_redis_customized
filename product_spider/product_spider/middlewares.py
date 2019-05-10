import logging
from scrapy import signals
from scrapy.exceptions import NotConfigured
from twisted.internet import task
import json
from .items import ProductSpiderItem
import socket
from datetime import datetime
import os
from .tsdb import TSDB, ThreadSafeCounter
from .elastic import Elastic


logger = logging.getLogger(__name__)

class DriverDownloadMiddleware(object):
    def process_request(self, request, spider):
        if getattr(spider, 'driver_download', 0) or request.meta.get('driver_download'):
            return spider.driver.get_response(request)


class TSDBMiddleware(object):
    """downloadermiddleware: send some statistics to tsdb.
    """

    def __init__(self, tsdb, interval):
        self.tsdb = tsdb        
        self.interval = interval
        self.requests_dropped = ThreadSafeCounter()
        self.requests_scheduled = ThreadSafeCounter()
        self.responses_downloaded = ThreadSafeCounter()
        self.downloads_retried = ThreadSafeCounter()
        self.download_errors = ThreadSafeCounter()
        self.download_exceptions = ThreadSafeCounter()
        self.spider_errors = ThreadSafeCounter()
        self.items_scraped = ThreadSafeCounter()
        self.items_dropped = ThreadSafeCounter()
        self.loop_call = task.LoopingCall(self._report_to_tsdb)
        self.loop_call.start(self.interval)

    @classmethod
    def from_crawler(cls, crawler):
        
        interval = crawler.settings.getint('SPIDERTSDB_INTERVAL', 10)
        tsdb = TSDB.from_settings(crawler.settings)
        # first check if the extension should be enabled and raise
        # NotConfigured otherwise
        if interval <= 0 or tsdb is None:
            raise NotConfigured

        # instantiate the extension object
        ext = cls(tsdb, interval)

        # connect the extension oimport potsdbbject to signals
        crawler.signals.connect(ext.request_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(ext.request_dropped, signal=signals.request_dropped)
        crawler.signals.connect(ext.response_downloaded, signal=signals.response_downloaded)
        crawler.signals.connect(ext.spider_error, signal=signals.spider_error)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.item_dropped, signal=signals.item_dropped)

        ext.elastic = Elastic.from_settings(crawler.settings)

        # return the extension object
        return ext

    def _make_tsdb_tags(self, spider, meta, *args):
        spider_label = meta.get('spider_label')
        spider_name = spider.name + '_' + spider_label if spider_label else spider.name
        crawler_type = spider.crawler_type
        source_type = meta.get('source_type', 'na')
        return (spider_name, crawler_type, source_type) + tuple(args)

    def process_response(self, request, response, spider):
        """ downloadermiddleware.process_response()
        """            
        retry_times = request.meta.get('retry_times', 0)

        if retry_times == 1:
            self.downloads_retried.increment(self._make_tsdb_tags(spider, request.meta, response.status))
                
        if response.status >= 400 and retry_times == 0:
            if self.elastic:
                self.elastic.post(spider.name, url=response.url, req_meta=request.meta,
                                    error_status=response.status, error_type='download error')
            
            self.download_errors.increment(self._make_tsdb_tags(spider, request.meta, response.status))

        return response

    def process_exception(self, request, exception, spider):
        """ downloadermiddleware.process_exception()
        """
        if self.elastic:
            self.elastic.post(spider.name, url=request.url, req_meta=request.meta,
                                error_status=1, error_type=exception.__class__.__name__,
                                error_msg=str(exception))

        if request.meta.get('retry_times', 0) == 0:
            k = self._make_tsdb_tags(spider, request.meta, exception.__class__.__name__)
            self.download_exceptions.increment(k)

    def request_scheduled(self, request, spider):
        k = self._make_tsdb_tags(spider, request.meta)
        self.requests_scheduled.increment(k)

    def request_dropped(self, request, spider):
        if self.elastic:
            self.elastic.post(spider.name, url=request.url,
                                    req_meta=request.meta, filtered=True)

        k = self._make_tsdb_tags(spider, request.meta)
        self.requests_dropped.increment(k)

    def response_downloaded(self, response, request, spider):
        k = self._make_tsdb_tags(spider, request.meta)
        self.responses_downloaded.increment(k)

    def item_scraped(self, item, spider):
        if isinstance(item, ProductSpiderItem):
            k = self._make_tsdb_tags(spider, item, item['status'])
            self.items_scraped.increment(k)

    def item_dropped(self, item, response, exception, spider):
        error_type = exception.args[0] if len(exception.args) > 0 else exception.__class__.__name__
        if self.elastic:
            self.elastic.post(spider.name, url=response.url,
                            req_meta=response.meta, error_status=response.status,
                            error_type=error_type,
                            error_msg=str(exception))

        k = self._make_tsdb_tags(spider, response.meta,
                        item.__class__.__name__, error_type)
        self.items_dropped.increment(k)

    def spider_error(self, failure, response, spider):
        error_type = failure.type.__name__
        if self.elastic:
            self.elastic.post(spider.name, url=response.url,
                            req_meta=response.meta, error_status=response.status,
                            error_type=error_type,
                            error_msg=str(failure))

        k = self._make_tsdb_tags(spider, response.meta, response.status, error_type)
        self.spider_errors.increment(k)

    def _report_to_tsdb(self):

        # requests enqueued
        table = self.requests_scheduled.reset()
        self._send('gym.crawler.requests_scheduled', table)

        # when requests are filtered
        table = self.requests_dropped.reset()
        self._send('gym.crawler.requests_dropped', table)

        # downloads retried (retry times == 1)
        # NOTE: Make sure RetryMiddleware runs after this middleware
        table = self.downloads_retried.reset()
        self._send('gym.crawler.downloads_retried', table, ['status'])

        # response status >= 400 and retry_times == 0
        table = self.download_errors.reset()
        self._send('gym.crawler.download_errors', table, ['status'])

        # exception raise during download
        table = self.download_exceptions.reset()
        self._send('gym.crawler.download_exceptions', table, ['exception'])

        # downloaded
        table = self.responses_downloaded.reset()
        self._send('gym.crawler.responses_downloaded', table)

        # number of items scraped
        table = self.items_scraped.reset()
        self._send('gym.crawler.items_scraped', table, ['status'])

        # number of DropItem exception raised
        table = self.items_dropped.reset()
        self._send('gym.crawler.items_dropped', table, ['item_type', 'exception'])

        # events for exception raised during any callback
        table = self.spider_errors.reset()
        self._send('gym.crawler.spider_errors', table, ['status', 'failure'])

    def _send(self, metric, table, extra_tags = None):
        for tagv, cnt in table.items():
            tagk = ['spider', 'crawler_type', 'source_type']
            if extra_tags:
                tagk = tagk + extra_tags
            self.tsdb.send(metric, float(cnt), zip(tagk, tagv))


class SpiderRegister(object):

    host = socket.gethostname()

    @classmethod
    def from_crawler(cls, crawler):

        inst = cls()
        inst.interval = crawler.settings.getint('SPIDERREGISTER_INTERVAL_SECS', 60)
        inst.spider_version = os.environ['SPIDER_VERSION']
        crawler.signals.connect(inst.spider_opened, signal=signals.spider_opened)
        return inst

    def spider_opened(self, spider):
        self.name = spider.name
        self.redis_server = spider.server
        self.register_looping_call = task.LoopingCall(self._register)
        self.register_looping_call.start(self.interval)
        logger.info("SpiderRegister looping call interval: {}".format(self.interval))

    def _register(self):
        key = '{}:{}'.format(self.name, self.host)
        value = {'version': self.spider_version, 'timestamp': datetime.utcnow().strftime('%Y%m%d%H%M%S')}
        self.redis_server.hset('spider:registry', key, json.dumps(value))

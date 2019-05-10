import logging
from redis import StrictRedis


logger = logging.getLogger(__name__)

class MyBaseDupeFilter:
    """No dupfilter"""

    def __init__(self, settings):
        pass

    @classmethod
    def from_settings(cls, settings):
        return cls(settings)

    @classmethod
    def from_spider(cls, spider):
        cls.filter_key = spider.settings.get('DUPEFILTER_REDIS_KEY', 'df_redis:%(fp)s')
        return cls(spider.settings)

    @classmethod
    def get_fingerprint(cls, request):
        return cls.filter_key % { u'fp': request.url }

    def request_seen(self, request):
        return False

    def open(self):
        pass

    def close(self, reason):
        pass

    def log(self, request, spider):
        pass


class RedisDupeFilter:
    """duplicates filter using Redis

    Default ttl will be one day. You can use different ttl for each url by setting
    request.meta['url_ttl_secs']
    """

    def __init__(self, settings):
        host = settings.get('DUPEFILTER_REDIS_HOST')
        port = settings.getint('DUPEFILTER_REDIS_PORT', 6379)
        db = settings.getint('DUPEFILTER_REDIS_DB', 0)
        self.redis_server = StrictRedis(host=host, port=port, db=db, encoding='utf-8')
        self.url_ttl_secs = settings.getint('DUPEFILTER_REDIS_URL_TTL_SECS', 86400)
        self.debug = settings.getbool('DUPEFILTER_DEBUG')
        self.logger = logging.getLogger(__name__)
        self.logger.info('host:{}, port:{}, db:{}, debug:{}, key:{}, ttl:{}'.format(
                    host, port, db, self.debug, self.filter_key, self.url_ttl_secs))

    @classmethod
    def from_settings(cls, settings):
        return cls(settings)

    @classmethod
    def from_spider(cls, spider):
        cls.filter_key = spider.settings.get('DUPEFILTER_REDIS_KEY', 'df_redis:%(fp)s')
        return cls(spider.settings)

    @classmethod
    def get_fingerprint(cls, request):
        return cls.filter_key % { u'fp': request.url }

    def request_seen(self, request):
        fp = self.get_fingerprint(request)
        if self.redis_server.get(fp):
            return True

        ttl = request.meta.get('url_ttl_secs')
        if ttl is None:
            ttl = self.url_ttl_secs
        self.redis_server.setex(fp, ttl, '1')

    def open(self):
        pass

    def close(self, reason):
        pass

    def log(self, request, spider):
        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)

# -*- coding: utf-8 -*-

# Scrapy settings for redismall project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'product_spider'

SPIDER_MODULES = ['product_spider.spiders', 'product_spider.datafeeds']
NEWSPIDER_MODULE = 'product_spider.spiders'

# Enables scheduling storing requests queue in redis.
SCHEDULER = "scrapy_redis.scheduler.Scheduler"

# Don't cleanup redis queues, allows to pause/resume crawls.
SCHEDULER_PERSIST = True

# Schedule requests using a priority queue. (default)
SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.SpiderPriorityQueue'
# Schedule requests using a queue (FIFO).
#SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.SpiderQueue'
# Schedule requests using a stack (LIFO).
#SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.SpiderStack'

# Specify the host and port to use when connecting to Redis (optional).
REDIS_HOST = 'crawler-redis'
REDIS_PORT = 6380
REDIS_START_URLS_JSON = True

# Redis server which holds a queue to process images for CDN
CDN_REDIS_HOST = 'crawler-redis'
CDN_REDIS_PORT = 6380
CDN_REDIS_KEY = 'img:priqueue'

DB_HOST = ''
DB_PORT = 3306
DB_NAME = ''
DB_USER = ''
DB_PWD = ''

# Ensure all spiders share same duplicates filter through redis.
DUPEFILTER_CLASS = "product_spider.dupefilter_redis.RedisDupeFilter"
DUPEFILTER_DEBUG = True
DUPEFILTER_REDIS_HOST = REDIS_HOST
DUPEFILTER_REDIS_PORT = REDIS_PORT
DUPEFILTER_REDIS_DB = 0
DUPEFILTER_REDIS_URL_TTL_SECS = 86400

# Max idle time to prevent the spider from being closed when distributed crawling.
# This only works if queue class is SpiderQueue or SpiderStack,
# and may also block the same time when your spider start at the first time (because the queue is empty).
SCHEDULER_IDLE_BEFORE_CLOSE = 10


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'redismall (+http://www.yourdomain.com)'

# Obey robots.txt rules
#ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 4

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs

# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# disable log
#LOG_ENABLED = False

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

DOWNLOAD_DELAY=0

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {

    'product_spider.middlewares.DriverDownloadMiddleware': 410,

    # DOWNLOADER_MIDDLEWARES_BASE
    #'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 500,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 500,
    
     # should run before RetryMiddleware because it will uses meta['retry_times']
    'product_spider.middlewares.TSDBMiddleware': 540

    # DOWNLOADER_MIDDLEWARES_BASE
    # 'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
    # 'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750
}
#HTTPERROR_ALLOWED_CODES = [404]
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
# extension for tsdb log
EXTENSIONS = {
    # register spider host in redis
    'product_spider.middlewares.SpiderRegister': 100,
    #'scrapy.extensions.telnet.TelnetConsole': None,
}

SPIDERTSDB_INTERVAL = 10
SPIDERTSDB_SERVER = ''
SPIDERTSBD_PORT = 4242

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'product_spider.pipelines.ProductSpiderPipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = True

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

DOWNLOAD_HANDLERS = {
    'dispatch': 'product_spider.downloader.handlers.dispatcher.DispatcherDownloadHandler',
}


ELASTICSEARCH_HOST = ''
ELASTICSEARCH_PORT = 9200

# search api
SEARCH_API = ''

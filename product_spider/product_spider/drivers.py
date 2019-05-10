from io import BytesIO
from urllib.parse import unquote
from selenium import webdriver
from scrapy.http import Response, HtmlResponse
from ftplib import FTP_TLS
from twisted.internet import defer
from urllib.parse import urlparse
import logging
import os
from fake_useragent import UserAgent
from selenium.common.exceptions import TimeoutException, UnexpectedAlertPresentException, NoSuchWindowException, WebDriverException

logger = logging.getLogger(__name__)


class BaseDriver(object):

    @classmethod
    def setup(cls, spider, *args, **kwargs):
        spider.driver = inst = cls(*args, **kwargs)
        inst.spider = spider

        # wrap spider.closed(reason) with driver.spider_closed
        inst.original_closed_event = getattr(spider, 'closed', None)
        spider.closed = inst.spider_closed
        return inst

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()        

    def get_response(self, request):
        raise NotImplemented

    def close(self):
        pass
        
    def spider_closed(self, reason):
        self.close()
        if self.original_closed_event:
            self.original_closed_event(reason)

class TestDriver(BaseDriver):
    def get_response(self, request):
        return Response(request.url, status=403)

class ChromeDriver(BaseDriver):
    
    def __init__(self, reset_freq=100, timeout=30):

        self.ua = UserAgent()
        self.reset_freq = reset_freq
        self.timeout = timeout
        self._setup_driver()

    def _reset_driver(self):
        logger.info('chrome driver reset: {}'.format(self.count))
        self.close()
        self._setup_driver()

    def _setup_driver(self):
        self.count = 0
        self._driver = webdriver.Chrome(chrome_options=self._get_options())
        #self._driver.implicitly_wait(10)

    def _get_options(self):
        
        options = webdriver.ChromeOptions()

        options.add_argument('--headless')

        # Note: --no-sandbox is not needed if you properly setup a user in the container.
        # https://developers.google.com/web/updates/2017/04/headless-chrome
        options.add_argument('--no-sandbox')

        options.add_argument('--disable-gpu')

        # Issues a stop after the specified number of milliseconds.
        # This cancels all navigation and causes the DOMContentLoaded event to fire.
        #if self.timeout > 0:
        #    options.add_argument('--timeout ' + str(self.timeout * 1000))
        
        # shared memory is not enough?
        # https://stackoverflow.com/questions/38065688/chromdriver2-21-2-failing-no-such-session/40827138
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-logging')

        # https://stackoverflow.com/questions/48773031/how-to-prevent-chrome-headless-from-loading-images
        options.add_argument('--blink-settings=imagesEnabled=false')

        #prefs = {"profile.default_content_settings.images":2}
        #options.add_experimental_option("prefs", prefs)
        # http://stackoverflow.com/questions/28070315/python-disable-images-in-selenium-google-chromedriver

        # https://intoli.com/blog/making-chrome-headless-undetectable/
        # random user-agent
        options.add_argument('--user-agent="{}"'.format(self.ua.random))

        http_proxy = os.getenv('http_proxy')
        if http_proxy:
            options.add_argument('--proxy-server={}'.format(http_proxy))

        return options

    def get_response(self, request):
        try:
            self._driver.set_script_timeout(self.timeout)
            self._driver.set_page_load_timeout(self.timeout)
            
            refresh = False
            try:
                self._driver.get(request.url)
                body = self._driver.page_source
            except TimeoutException:
                # https://stackoverflow.com/questions/40514022/chrome-webdriver-produces-timeout-in-selenium
                logger.error('Timeout Refresh: ' + request.url)
                refresh = True
            # sex product in 博客來 will raise UnexpectedAlertPresentException
            except UnexpectedAlertPresentException as e:
                return

            if refresh:
                self._driver.refresh()
                body = self._driver.page_source
                logger.info('Refresh Done: ' + request.url)

            self.count += 1
            if self.count >= self.reset_freq:
                self._reset_driver()

            return HtmlResponse(request.url, body=body,
                            encoding='utf-8', request=request)

        except ConnectionRefusedError as e:
            # why do we have connectionrefusederror? chrome is gone?!
            logger.error('Connection Refused: \"' + request.url + '\", ' + str(e))
            self._setup_driver()
            raise
        except Exception as e:
            # when UnexpectedAlertPresentException happens, we should close alert window before process next request!!!
            logger.error('Unexpected Error: \"' + request.url + '\", ' + str(e))
            self._reset_driver()
            raise

    def close(self):
        try:
            self._driver.close()
        except (NoSuchWindowException, WebDriverException) as e:
            # NoSuchWindowException: sometimes chrome hangs, don't know why
            # WebDriverException: Message: unknown error: failed to close window in 20 seconds
            logger.error('fail to close window: ' + str(e))
        self._driver.quit()

class ReceivedDataProtocol(object):
    def __init__(self, filename=None):
        self.__filename = filename
        self.body = open(filename, "wb") if filename else BytesIO()
        self.size = 0

    def dataReceived(self, data):
        self.body.write(data)
        self.size += len(data)

    @property
    def filename(self):
        return self.__filename

    def close(self):
        self.body.close() if self.filename else self.body.seek(0)

class FTPES_Driver(BaseDriver):
    def __init__(self):
        self.ftp = FTP_TLS()

    def connect(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        logger.info('ftpes to ' + self.host)

    def get_response(self, request):
                
        self.ftp.connect(self.host)
        self.ftp.login(self.user, self.password)
        self.ftp.prot_p()

        logger.info('download ' + request.url)
        
        protocol = ReceivedDataProtocol()
        path = unquote(urlparse(request.url).path)
        self.ftp.retrbinary('RETR ' + path, protocol.dataReceived)
        protocol.close()
        headers = {"size": protocol.size}
        request.meta['content']=protocol.body

        self.ftp.quit()

        return Response(url=request.url, status=200,
                        headers=headers, request=request)

    def close(self):
        self.ftp.close()

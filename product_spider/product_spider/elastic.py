from elasticsearch import Elasticsearch
import socket
from datetime import datetime
import json
import logging


logger = logging.getLogger(__name__)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.INFO)


class Elastic:
    
    @classmethod
    def from_settings(cls, settings):
        es_host = settings.get('ELASTICSEARCH_HOST', None)
        if es_host is None:
            return

        es_port = settings.getint('ELASTICSEARCH_PORT', 9200)

        inst = cls()
        inst.elasticsearch = Elasticsearch([{'host': es_host, 'port': es_port}])
        inst.host = socket.gethostname()
        return inst

    def post(self, spider_name, url='', keyword=None, req_meta=None, count=None,
                    filtered=False, error_status=0, error_type=None, error_msg=None):
        
        if keyword is None:
            keyword = req_meta.get('keyword', '')

        now = datetime.utcnow()
        index_name = 'gymcrawler_keywordspider-' + now.strftime('%Y-%m-%d')

        body = {
                'timestamp': int(now.timestamp()),
                'spider': spider_name,
                'host': self.host,
                'url': url,
                'keyword': keyword,
                'filtered': filtered,
                'error_status': error_status,
                }
        
        if count is not None:
            body['count'] = count

        if error_type is not None:
            body['error_type'] = error_type

        if error_msg is not None:
            body['error_msg'] = error_msg
        
        self.elasticsearch.index(index=index_name, doc_type='doc', body=json.dumps(body))

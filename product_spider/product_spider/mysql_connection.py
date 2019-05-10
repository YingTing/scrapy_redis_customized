# -*- coding: utf-8 -*-
import logging
import pymysql
from queue import Queue
from twisted.internet import task
from .tsdb import TSDB, ThreadSafeCounter
from product_spider.items import ProductSpiderItem
from product_spider.SpiderLogger import logger_tsdb
import time

logger = logging.getLogger(__name__)

class MySQLConnection(object):
    def __init__(self, db_host, db_port, db_name, db_user, db_pwd):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_pwd = db_pwd
        self._connect()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def _connect(self):
        logger.info('DB connect to {}:{}'.format(self.db_host, self.db_port))
        conn = pymysql.connect( \
                        user=self.db_user, \
                        passwd=self.db_pwd, \
                        db=self.db_name, \
                        host=self.db_host, \
                        port=self.db_port, \
                        charset="utf8mb4", \
                        use_unicode=True)
        cursor = conn.cursor()
        self.conn, self.cursor = conn, cursor

    def _try_connect(self):
        try:
            self.conn.ping()
        except Exception as e:
            self._connect()
            #self._log_db_error('reconnect', e)
            logger.error('reconnect db: ' + str(e))

    def execute(self, cmd):
        try:
            self._try_connect()
            self.cursor.execute(cmd)
            self.conn.commit()
        except Exception as e:
            logger_tsdb.error(e)
            logger_tsdb.error(cmd)
            logger.error(e)
            logger.error(cmd)

    def update_or_insert(self, data, table_name, bulk=False):
        if bulk:
            self.db_queue.put([table_name, data])
            return

        """If data exist, then update. else, insert"""
        keys, values = zip(*data.db_items())
        keys_str = ','.join(['`' + k + '`' for k in keys])
        values_str = ','.join(['%s'] * len(keys))

        # on duplicate key update, no need to update item_id
        on_dup_update_str = ','.join([k + '=VALUES(' + k + ')' for k in keys if k != 'item_id'])

        cmd = "INSERT INTO `" + table_name + "` (" + keys_str + ")" + " VALUES (" + values_str + ") ON DUPLICATE KEY UPDATE " + on_dup_update_str

        try:
            self._try_connect()
            self.cursor.execute(cmd, values)
            self.conn.commit()
        except Exception as e:
            logger_tsdb.error(e)
            logger_tsdb.error(cmd)
            self._log_db_error(data.__class__.__name__, e)
            logger.error(e)
            logger.error(cmd)
            logger.error(values)
    
    def update(self, data, compare_key, table_name):
        """Update function
           compare_key = ['key1',key2',...]
        """

        db_value = []
        db_set = []
        
        #Build update key,value
        for (key, value) in data.db_items():
            db_set.append(key + '=%s')
            db_value.append(value)
            
        #build where string
        if compare_key != '':
            where_string = ' WHERE '
            i = 0
            for key in compare_key:
                if i == 0:
                    where_string = where_string + key + '=%s'
                else:
                    where_string = where_string + ' and ' + key + '=%s'
                db_value.append(data[key])
                i = i+1

        cmd = "UPDATE `" + table_name + "` SET "+','.join(db_set)+ where_string

        try:
            self._try_connect()
            self.cursor.execute(cmd ,db_value)
            self.conn.commit()
        except Exception as e:
            logger_tsdb.error(e)
            logger_tsdb.error(cmd)
            self._log_db_error(data.__class__.__name__, e)
            logger.error(e)
            logger.error(cmd)

    def close(self):
        self.conn.close()

class CrawlerSQLConnection(MySQLConnection):

    def __init__(self, crawler):
        settings = crawler.settings
        self.db_host = settings.get('DB_HOST')
        self.db_port = settings.get('DB_PORT')
        self.db_name = settings.get('DB_NAME')
        self.db_user = settings.get('DB_USER')
        self.db_pwd = settings.get('DB_PWD')
        super().__init__(self.db_host, self.db_port, self.db_name, self.db_user, self.db_pwd)

        self.spider_name = crawler.spider.name

        self.bulk_insert_interval_secs = crawler.settings.getint('BULKINSERT_INTERVAL_SECS', 10)
        self.bulk_insert_batch_size = crawler.settings.getint('BULKINSERT_BATCH_SIZE', 300)

        logger.info("interval: {}, batch_size: {}".format(self.bulk_insert_interval_secs, self.bulk_insert_batch_size))

        # bulk insert
        self.db_queue = Queue()
        self.loop_db_call = task.LoopingCall(self.flush)
        self.loop_db_call.start(self.bulk_insert_interval_secs)
        
        tsdb = TSDB.from_settings(crawler.settings)
        if tsdb:
            self.tsdb = tsdb

            self.db_errors = ThreadSafeCounter()
            self.interval = 30

            self.loop_tsdb_call = task.LoopingCall(self._report_to_tsdb)
            self.loop_tsdb_call.start(self.interval)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def _report_to_tsdb(self):

        tagk = ['spider', 'item_type', 'exception', 'status']

        table = self.db_errors.reset()
        for tagv, cnt in table.items():
            self.tsdb.send('gym.crawler.db_errors', cnt, zip(tagk, tagv))

    def _log_db_error(self, item_type, err):
        err_name = err.__class__.__name__
        status = err.args[0] if len(err.args) and isinstance(err.args[0], int) else 'na'
        k = (self.spider_name, item_type, err_name, status)
        self.db_errors.increment(k)

    def close(self):
        self.flush()
        super().close()

    def flush(self):

        def _flush(table_dict, is_item):
            count = 0
            for table_name, items in table_dict.items():
                if isinstance(items[0], ProductSpiderItem) == is_item:
                    count += len(items)
                    self.bulk_insert(items, batch_size, table_name)
            if count:
                self.conn.commit()
            return count
        
        q = self.db_queue

        table_dict = dict()
        while not q.empty():
            table_name, item = q.get()
            items = table_dict.setdefault(table_name, [])
            items.append(item)

        batch_size = self.bulk_insert_batch_size

        # item first, for mysql error 1032: Can't find record....
        item_count = _flush(table_dict, True)
        other_count = _flush(table_dict, False)

        logger.debug('flushing db queue: items: {}, others: {}'.format(item_count, other_count))

        if item_count + other_count:
            self.conn.commit()

        
    def bulk_insert(self, data, batch_size, table_name):
        if len(data) == 0:
            return

        # data is a list of Item Class (in items.py)
        keys = list(data[0].db_keys())
        keys_str = ','.join(['`' + k + '`' for k in keys])
        value_tuple_str = '(' + ','.join(['%s'] * len(keys)) + ')'

        # on duplicate key update, no need to update item_id
        on_dup_update_str = ','.join([k + '=VALUES(' + k + ')' for k in keys if k != 'item_id'])

        for i in range(0, len(data), batch_size):
            items = data[i:i+batch_size]
            values = [item.get(k) for item in items for k in keys]
            values_str = ','.join([value_tuple_str] * len(items))

            # NOTE: if "keys" only has "item_id" field, upsert will cause error
            cmd = "INSERT INTO `" + table_name + "` (" + keys_str + ")" + " VALUES " + values_str + " ON DUPLICATE KEY UPDATE " + on_dup_update_str
            try:
                self._try_connect()
                self.cursor.execute(cmd, values)
            except Exception as e:
                logger_tsdb.error(e)
                logger_tsdb.error(cmd)
                logger.error(e)
                logger.error(cmd)
                logger.error(values)

                cmd = "INSERT INTO `" + table_name + "` (" + keys_str + ")" + " VALUES " + value_tuple_str + " ON DUPLICATE KEY UPDATE " + on_dup_update_str
                for item in items:
                    values = [item.get(k) for k in keys]
                    try:
                        self._try_connect()
                        self.cursor.execute(cmd, values)
                    except Exception as e:
                        self._log_db_error(item.__class__.__name__, e)
                        logger_tsdb.error(e)
                        logger_tsdb.error(cmd)
                        logger.error('NON-BULK: ' + str(e))
                        logger.error(cmd)
                        logger.error(values)

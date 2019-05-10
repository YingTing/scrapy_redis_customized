from redis import Redis


class RedisConnection:
    def __init__(self, host, port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db
        self._connect()

    def _connect(self):
        self.conn = Redis(host=self.host, port=self.port, db=self.db, encoding='utf-8')

    def zincrby(self, key, value, amount):
        return self.conn.zincrby(key, value, amount)

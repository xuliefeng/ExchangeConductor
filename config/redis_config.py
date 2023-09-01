from _decimal import Decimal

import redis
import json


class RedisConfig:
    def __init__(self, host='localhost', port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db
        self.client = self.connect()

    def connect(self):
        return redis.Redis(host=self.host, port=self.port, db=self.db)

    def set_data(self, key, data):
        data_json = json.dumps(data, default=self.handle_decimal)
        self.client.set(key, data_json)

    @staticmethod
    def handle_decimal(obj):
        if isinstance(obj, Decimal):
            return str(obj)
        raise TypeError

    def get_data(self, key):
        data_json = self.client.get(key)
        if data_json:
            return json.loads(data_json.decode('utf-8'))
        else:
            return None

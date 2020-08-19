import redis
import time
import hashlib
from typing import *


class WebCookie:
    def __init__(self):
        self._QUIT = False
        self._LIMIT = 10000000
        self._conn = redis.Redis()

    # 获取用户令牌
    def check_token(self, token):
        return self._conn.hget('login:', token)

    # 更新用户令牌
    # 保存用户最近浏览的25个商品
    def update_token(self, token, user, item=None):
        timestamp = time.time()
        self._conn.hset('login:', token, user)
        self._conn.zadd('recent:', token, timestamp)
        if item:
            self._conn.zadd('viewed:' + token, item, timestamp)
            self._conn.zremrangebyrank('viewed:' + token, 0, -26)

    def clean_sessions(self):
        while not self._QUIT:
            size = self._conn.zcard('recent:')
            if size <= self._LIMIT:
                time.sleep(1)
                continue

            end_index = min(size - self._LIMIT, 100)
            tokens = self._conn.zrange('recent:', 0, end_index - 1)

            session_keys = []
            for token in tokens:
                session_keys.append('viewed:' + token)
                session_keys.append('cart:' + token)

            self._conn.delete(*session_keys)
            self._conn.hdel('login:', *tokens)
            self._conn.zrem('recent:', *tokens)

    # 添加到购物车
    def add_to_cart(self, session, item, count):
        if count <= 0:
            self._conn.hdel('cart:' + session, item)
        else:
            self._conn.hset('cart:' + session, item, count)


class Cache:
    def __init__(self):
        self._QUIT = False
        self._conn = redis.Redis()

    def _can_cache(self, request):
        return True

    def _hash_request(self, request):
        return str(hash(request))

    # 缓存请求
    def cache_request(self, request: Any, callback: Callable):
        if not self._can_cache(request):
            return callback(request)

        page_key = 'cache:' + self._hash_request(request)
        content = self._conn.get(page_key)

        if not content:
            content = callback(request)
            self._conn.setex(page_key, 300, content)

        return content

    def schedule_row_cache(self, row_id, delay):
        self._conn.zadd('delay:', {row_id: delay})
        self._conn.zadd('schedule:', {row_id: time.time()})

    def cache_rows(self):
        while not self._QUIT:
            next = self._conn.zrange('schedule:', 0, 0, withscores=True)
            now = time.time()
            if not next or next[0][1] > now:
                time.sleep(0.05)
                continue

            row_id = next[0][0]
            delay = self._conn.zscore('delay:', row_id)
            if delay <= 0:
                self._conn.zrem('delay:', row_id)
                self._conn.zrem('schedule:', row_id)
                self._conn.delete('inv:' + row_id)
            else:
                pass


if __name__ == '__main__':
    cache = Cache()
    # response = cache.cache_request('123123', lambda x: x + 'cache')
    # print(response)
    cache.schedule_row_cache('1', 100)

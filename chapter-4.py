import redis
from typing import *
import os
import time


class Log:
    def process_logs(conn: redis.Redis, path: str, callback: Callable) -> None:
        current_file, offset = conn.mget('progress:file', 'progress:position')
        pipe = conn.pipeline()

        def update_progress():
            pipe.mset({
                'progress:file': fname,
                'progress:position': offset
            })
            pipe.execute()

        for fname in sorted(os.listdir(path)):
            if fname < current_file:
                continue
            inp = open(os.path.join(path, fname), 'rb')
            if fname == current_file:
                inp.seek(int(offset, 10))
            else:
                offset = 0

            current_file = None
            for lno, line in enumerate(inp):
                callback(pipe, line)
                offset += int(offset) + int(line)
                if not (lno + 1) % 1000:
                    update_progress()
            update_progress()
            inp.close()


class FakeGame:
    def __init__(self):
        self._conn = redis.Redis()

    def list_item(self, item_id: str, seller_id: str, price: float):
        inventory = 'inventory:%s' % seller_id
        item = '%s.%s' % (item_id, seller_id)
        end = time.time() + 5
        pipe = self._conn.pipeline()

        while time.time() < end:
            try:
                pipe.watch(inventory)
                if not pipe.sismember(inventory, item_id):
                    pipe.unwatch()
                    return None

                pipe.multi()
                pipe.zadd('market:', item, price)
                pipe.srem(inventory, item_id)
                pipe.execute()
                return True
            except redis.exceptions.WatchError:
                pass

        return False

    def purchase_item(self, buyer_id: str, item_id: str, seller_id: str, lprice: float):
        buyer = 'users:%s' % buyer_id
        seller = 'seller:%s' % seller_id
        item = '%s.%s' % (item_id, seller_id)
        inventory = 'inventory:%s' % buyer_id
        end = time.time() + 10
        pipe = self._conn.pipeline()

        while time.time() < end:
            try:
                pipe.watch('market:', buyer)
                price = pipe.zscore('market:', item)
                funds = int(pipe.hget(buyer, 'funds'))
                if price != lprice or price > funds:
                    pipe.unwatch()
                    return None

                pipe.multi()
                pipe.hincrby(seller, 'funds', int(price))
                pipe.hincrby(buyer, 'funds', int(-price))
                pipe.sadd(inventory, item_id)
                pipe.zrem('market:', item)
                pipe.execute()
                return True
            except redis.exceptions.WatchError:
                pass
        return False


if __name__ == '__main__':
    pass

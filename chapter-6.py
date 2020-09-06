import redis
import bisect
import uuid


class AddressBook:
    def __init__(self):
        self._conn = redis.Redis()
        self._valid_characters = '`abcdefghijklmnopqrstuvwxyz{'

    def _find_prefix_range(self, prefix):
        posn = bisect.bisect_left(self._valid_characters, prefix[-1:])
        suffix = self._valid_characters[(posn or 1) - 1]
        return prefix[:-1] + suffix + '{', prefix + '{'

    def autocomplete_on_prefix(self, guild, prefix):
        start, end = self._find_prefix_range(prefix)
        identifier = str(uuid.uuid4())
        start += identifier
        end += identifier
        zset_name = 'members:' + guild
        items = []

        self._conn.zadd(zset_name, {
            start: 0,
            end: 0,
        })
        pipeline = self._conn.pipeline(True)
        while True:
            try:
                pipeline.watch(zset_name)
                sindex = pipeline.zrank(zset_name, start)
                eindex = pipeline.zrank(zset_name, end)
                erange = min(sindex + 9, eindex - 2)
                pipeline.multi()
                pipeline.zrem(zset_name, start, end)
                pipeline.zrange(zset_name, sindex, erange)
                items = pipeline.execute()[-1]
                break
            except redis.exceptions.WatchError:
                continue

        return [item.decode() for item in items if '{' not in item.decode()]

    def join_guild(self, guild, user):
        self._conn.zadd('members:' + guild, {
            user: 0,
        })

    def leave_guild(self, guild, user):
        self._conn.zrem('members:' + guild, user)


def address_book_demo():
    address_book = AddressBook()
    guild1_name = 'guild1'

    address_book.join_guild(guild1_name, 'aaaass')
    address_book.join_guild(guild1_name, 'bbas')
    address_book.join_guild(guild1_name, 'acsc')
    items = address_book.autocomplete_on_prefix(guild1_name, 'a')
    print(items)


if __name__ == '__main__':
    address_book_demo()

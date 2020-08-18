import redis
import time


class ArticleVote:
    def __init__(self):
        self._ONE_WEEK_IN_SECOND = 7 * 24 * 60 * 60
        self._VOTE_SCORE = 432
        self._pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)

    def clear(self):
        self._get_conn().flushall()

    def _get_conn(self):
        return redis.Redis(connection_pool=self._pool)

    # 对文章进行投票
    def article_vote(self, user: str, article: str):
        r = self._get_conn()
        cutoff = time.time() - self._ONE_WEEK_IN_SECOND
        # 检查是否还可以对文章进行投票
        if r.zscore('time:', article) < cutoff:
            return

        # 从article:id标识符里面取出文章的ID
        article_id = article.partition(':')[-1]
        # 如果用户是第一次投票，则增加文章的投票数量和评分
        if r.sadd('voted:' + article_id, user):
            r.zincrby('score:', article, self._VOTE_SCORE)
            r.hincrby(article, 'votes', 1)

    # 发布并获取文章
    def post_article(self, user: str, title: str, link: str):
        r = self._get_conn()
        article_id = str(r.incr('article:'))

        # 将发布文章的用户添加到文章的已投票用户名单里，
        # 然后将这个名单的过期时间设置为一周
        voted = 'voted:' + article_id
        r.sadd(voted, user)
        r.expire(voted, self._ONE_WEEK_IN_SECOND)

        now = time.time()
        # 文章信息存储到hash中
        article = 'article:' + article_id
        r.hset(article, mapping={
            'title': title,
            'link': link,
            'poster': user,
            'time': now,
            'votes': 1,
        })

        # 发布时间和评分存到zset中
        r.zadd('score:', {article: now + self._VOTE_SCORE})
        r.zadd('time:', {article: now})

        return article_id

    # 获取文章
    def get_articles(self, page_num: int, page_size=25, order='score:'):
        r = self._get_conn()
        start = (page_num - 1) * page_size
        end = start + page_size - 1

        ids = r.zrevrange(order, start, end)
        articles = []
        for article_id in ids:
            article_data = r.hgetall(article_id)
            article_data['id'] = article_id
            articles.append(article_data)

        return articles

    # 对文章进行分组
    def add_remove_groups(self, article_id: str, to_add=[], to_remove=[]):
        r = self._get_conn()
        article = 'article:' + article_id
        for group in to_add:
            r.sadd('group:' + group, article)
        for group in to_remove:
            r.srem('group:' + group, article)

    def get_group_articles(self, group: str, page_num: int, page_size=25, order='score:'):
        r = self._get_conn()
        key = order + group
        if not r.exists(key):
            r.zinterstore(key, ['group:' + group, order], aggregate='max')
            r.expire(key, 60)
        return self.get_articles(page_num, page_size, key)


if __name__ == '__main__':
    a_v = ArticleVote()
    a_v.clear()

    user1, user2, user3 = 'user1', 'user2', 'user3'
    a_v.post_article(user1, 'python', 'link')
    a_v.post_article(user2, 'C#', 'link')
    a_v.post_article(user3, 'eat', 'link')
    a_v.post_article(user3, 'van', 'link')

    a_v.add_remove_groups('article:1', ['programming'])
    a_v.add_remove_groups('article:2', ['programming'])

    print(a_v.get_articles(1))
    print(a_v.get_group_articles('programming', 1))

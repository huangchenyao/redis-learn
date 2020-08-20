import redis
from typing import *
import os
import time
import logging
import datetime
import csv
from itertools import islice
from tqdm import tqdm


class RedisLog:
    def __init__(self):
        self._conn = redis.Redis()
        self._SEVERITY = {
            logging.DEBUG: 'debug',
            logging.INFO: 'info',
            logging.WARNING: 'warning',
            logging.ERROR: 'error',
            logging.CRITICAL: 'critical',
        }
        # self._SEVERITY.update((name, name) for name in self._SEVERITY.values())

    def log_recent(self, name: str, message: str, severity=logging.INFO):
        severity = self._SEVERITY[severity]
        destination = 'recent:%s:%s' % (name, severity)
        message = time.asctime() + ' ' + message
        pipe = self._conn.pipeline()
        pipe.lpush(destination, message)
        pipe.ltrim(destination, 0, 99)
        pipe.execute()

    def log_common(self, name: str, message: str, severity=logging.INFO, timeout=5):
        severity = self._SEVERITY[severity]
        destination = 'recent:%s:%s' % (name, severity)
        start_key = destination + ':start'
        pipe = self._conn.pipeline()
        end = time.time() + timeout
        while time.time() < end:
            try:
                pipe.watch(start_key)
                now = datetime.datetime.utcnow().timetuple()
                hour_start = datetime.datetime(*now[:4]).isoformat()
                existing = pipe.get(start_key)
                pipe.multi()
                if existing and existing < hour_start:
                    pipe.rename(destination, destination + ':last')
                    pipe.rename(start_key, destination + ':pstart')
                    pipe.set(start_key, hour_start)
                pipe.zincrby(destination, message)
                self.log_recent(name, message, severity)
            except redis.exceptions.WatchError:
                continue


class GeoIP:
    def __init__(self, path):
        self._conn = redis.Redis()
        self._path = path
        # self._conn.flushall()

    def _ip_to_score(self, ip):
        score = 0
        for v in ip.split('.'):
            score = score * 256 + int(v, 10)
        return score

    def _ip_parse(self, ip):
        res = []
        if '/' in ip:
            ip_prefix, mask = ip.split('/')
            score = self._ip_to_score(ip_prefix)
            mask = 32 - int(mask, 10)
            score &= 2 ** 32 - 2 ** mask
            for i in range(2 ** mask):
                res.append(score + i)
        else:
            res.append(self._ip_to_score(ip))
        return res

    def import_ips(self, file_name: str):
        with open(os.path.join(self._path, file_name), 'r') as csv_file:
            pipe = self._conn.pipeline(transaction=False)
            for count, row in tqdm(enumerate(islice(csv.reader(csv_file), 1, None))):
                score_list = self._ip_parse(row[0])
                city_id = row[1] + '_' + str(count)
                for score in score_list:
                    try:
                        pipe.zadd('ip2cityid:', city_id, score)
                    except Exception as e:
                        print(e)
                        print(row)

                if count % 2000 == 0:
                    pipe.execute()

            pipe.execute()

    def import_cities(self, file_prefix: str):
        for file_name in os.listdir(self._path):
            if file_name.startswith(file_prefix):
                pipe = self._conn.pipeline(transaction=False)
                with open(os.path.join(self._path, file_name), 'r') as csv_file:
                    for count, row in tqdm(enumerate(islice(csv.reader(csv_file), 1, None))):
                        pipe.hset('cityid2city:' + row[0], mapping={
                            'city_id': row[0],
                            'continent_name': row[3],
                            'country_name': row[5],
                            'city_name': row[10],
                        })

                        if count % 50000 == 0:
                            pipe.execute()

                pipe.execute()


if __name__ == '__main__':
    data_path = '/Users/huangchenyao/downloads/GeoLite2-City-CSV_20200818'
    geo_ip = GeoIP(data_path)
    geo_ip.import_cities('GeoLite2-City-Locations')
    geo_ip.import_ips('GeoLite2-City-Blocks-IPv4.csv')
    conn = redis.Redis()
    print(conn.hgetall('cityid2city:2077456'))

# ['network', 'geoname_id', 'registered_country_geoname_id', 'represented_country_geoname_id', 'is_anonymous_proxy',
# 'is_satellite_provider', 'postal_code', 'latitude', 'longitude', 'accuracy_radius']
# ['1.0.0.0/24', '2077456', '2077456', '', '0', '0', '', '-33.4940', '143.2104', '1000']
# ['1.0.1.0/24', '1814991', '1814991', '', '0', '0', '', '34.7725', '113.7266', '50']
# ['1.0.2.0/23', '1814991', '1814991', '', '0', '0', '', '34.7725', '113.7266', '50']
#
# ['geoname_id', 'locale_code', 'continent_code', 'continent_name', 'country_iso_code', 'country_name',
# 'subdivision_1_iso_code', 'subdivision_1_name', 'subdivision_2_iso_code', 'subdivision_2_name', 'city_name',
# 'metro_code', 'time_zone', 'is_in_european_union']
# ['5819', 'en', 'EU', 'Europe', 'CY', 'Cyprus', '02', 'Limassol District', '', '', 'Souni', '', 'Asia/Nicosia', '1']
# ['49518', 'en', 'AF', 'Africa', 'RW', 'Rwanda', '', '', '', '', '', '', 'Africa/Kigali', '0']
# ['49747', 'en', 'AF', 'Africa', 'SO', 'Somalia', 'BK', 'Bakool', '', '', 'Oddur', '', 'Africa/Mogadishu', '0']
# ['51537', 'en', 'AF', 'Africa', 'SO', 'Somalia', '', '', '', '', '', '', 'Africa/Mogadishu', '0']
# ['53654', 'en', 'AF', 'Africa', 'SO', 'Somalia', 'BN', 'Banaadir', '', '', 'Mogadishu', '', 'Africa/Mogadishu', '0']

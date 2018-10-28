# -*- coding: utf-8 -*-
from __future__ import absolute_import, division

import hashlib
import logging
import phoenixdb
import phoenixdb.cursor
import re
import six
import socket
import sys
import traceback

from frontera import DistributedBackend
from frontera.core.components import Metadata, Queue, States, Seed
from frontera.core.models import Request
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.utils.fingerprint import hostname_local_fingerprint
from frontera.utils.misc import chunks, get_crc32, time_elapsed
from frontera.utils.url import parse_domain_from_url_fast, norm_url
from frontera.contrib.backends.remote.codecs.msgpack import Decoder, Encoder
from frontera.contrib.backends.phoenix.domaincache import DomainCache
from frontera.contrib.backends.phoenix.utils import connect
from frontera.contrib.backends.hbase.utils import connect as hconnect

from msgpack import Unpacker, Packer, packb
from six.moves import range
from w3lib.util import to_bytes
from cachetools import LRUCache

from struct import pack, unpack
from datetime import datetime
from calendar import timegm
from time import time, sleep
from binascii import hexlify, unhexlify
from io import BytesIO
from random import choice
from collections import defaultdict, Iterable
from thriftpy.thrift import TException
from crontab import CronTab


_pack_functions = {
    'url': to_bytes,
    'depth': lambda x: pack('>I', 0),
    'created_at': lambda x: pack('>Q', x),
    'status_code': lambda x: pack('>H', x),
    'state': lambda x: pack('>B', x),
    'error': to_bytes,
    'domain_fprint': to_bytes,
    'score': lambda x: pack('>f', x),
    'content': to_bytes,
    'headers': packb,
    'dest_fprint': to_bytes
}


def unpack_score(blob):
    return unpack(">d", blob)[0]


def prepare_hbase_object(obj=None, **kwargs):
    if not obj:
        obj = dict()
    for k, v in six.iteritems(kwargs):
        if k in ['score', 'state']:
            cf = 's'
        elif k == 'content':
            cf = 'c'
        else:
            cf = 'm'
        func = _pack_functions[k]
        obj[cf + ':' + k] = func(v)
    return obj


def utcnow_timestamp():
    d = datetime.utcnow()
    return timegm(d.timetuple())


class LRUCacheWithStats(LRUCache):
    """Extended version of standard LRUCache with counting stats."""

    EVICTED_STATNAME = 'states.cache.evicted'

    def __init__(self, stats=None, *args, **kwargs):
        super(LRUCacheWithStats, self).__init__(*args, **kwargs)
        self._stats = stats
        if self._stats is not None:
            self._stats.setdefault(self.EVICTED_STATNAME, 0)

    def popitem(self):
        key, val = super(LRUCacheWithStats, self).popitem()
        if self._stats:
            self._stats[self.EVICTED_STATNAME] += 1
        return key, val


class HBaseQueue(Queue):
    GET_RETRIES = 3

    def __init__(self, host, port, namespace, partitions, table_name, drop=False, use_snappy=False, use_framed_compact=False):
        self.logger = logging.getLogger("hbase.queue")
        self._host = host
        self._port = port
        self._namespace = namespace
        self._use_framed_compact = use_framed_compact
        connection = hconnect(host, port, namespace, use_framed_compact=use_framed_compact)
        self.logger.info("Connecting to %s:%d thrift server.", host, port)
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.table_name = to_bytes(table_name)

        tables = set(connection.tables())
        if drop and self.table_name in tables:
            connection.delete_table(self.table_name, disable=True)
            tables.remove(self.table_name)

        if self.table_name not in tables:
            schema = {'f': {'max_versions': 1}}
            if use_snappy:
                schema['f']['compression'] = 'SNAPPY'
            try:
                connection.create_table(self.table_name, schema)
            except:
                err, msg, _ = sys.exc_info()
                self.logger.error("{} {}\n".format(err, msg))

        class DumbResponse:
            pass

        self.decoder = Decoder(Request, DumbResponse)
        self.encoder = Encoder(Request)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def schedule(self, batch):
        to_schedule = dict()
        now = int(time())
        for fprint, score, request, schedule in batch:
            if schedule:
                assert b'domain' in request.meta
                timestamp = request.meta[b'crawl_at'] if b'crawl_at' in request.meta else now
                to_schedule.setdefault(timestamp, []).append((request, score))
        for timestamp, batch in six.iteritems(to_schedule):
            self._schedule(batch, timestamp)

    def _schedule(self, batch, timestamp):
        """
        Row - portion of the queue for each partition id created at some point in time
        Row Key - partition id + score interval + random_str
        Column Qualifier - discrete score (first three digits after dot, e.g. 0.001_0.002, 0.002_0.003, ...)
        Value - QueueCell msgpack blob

        Where score is mapped from 0.0 to 1.0
        score intervals are
          [0.01-0.02)
          [0.02-0.03)
          [0.03-0.04)
         ...
          [0.99-1.00]
        random_str - the time when links was scheduled for retrieval, microsecs

        :param batch: iterable of Request objects
        :return:
        """

        def get_interval(score, resolution):
            if score < 0.0 or score > 1.0:
                raise OverflowError

            i = int(score / resolution)
            if i % 10 == 0 and i > 0:
                i = i - 1  # last interval is inclusive from right
            return (i * resolution, (i + 1) * resolution)

        random_str = int(time() * 1E+6)
        data = dict()
        for request, score in batch:
            domain = request.meta[b'domain']
            fingerprint = request.meta[b'fingerprint']
            slot = request.meta.get(b'slot')
            if slot is not None:
                partition_id = self.partitioner.partition(slot, self.partitions)
                key_crc32 = get_crc32(slot)
            elif type(domain) == dict:
                partition_id = self.partitioner.partition(domain[b'name'], self.partitions)
                key_crc32 = get_crc32(domain[b'name'])
            elif type(domain) == int:
                partition_id = self.partitioner.partition_by_hash(domain, self.partitions)
                key_crc32 = domain
            else:
                raise TypeError("partitioning key and info isn't provided")
            item = (unhexlify(fingerprint), key_crc32, self.encoder.encode_request(request), score)
            score = 1 - score  # because of lexicographical sort in HBase
            rk = "%d_%s_%d" % (partition_id, "%0.2f_%0.2f" % get_interval(score, 0.01), random_str)
            data.setdefault(rk, []).append((score, item))

        def batch_put(connection, table_name, data):
            table = connection.table(table_name)
            with table.batch(transaction=True) as b:
                for rk, tuples in six.iteritems(data):
                    obj = dict()
                    for score, item in tuples:
                        column = 'f:%0.3f_%0.3f' % get_interval(score, 0.001)
                        obj.setdefault(column, []).append(item)

                    final = dict()
                    packer = Packer()
                    for column, items in six.iteritems(obj):
                        stream = BytesIO()
                        for item in items:
                            stream.write(packer.pack(item))
                        final[column] = stream.getvalue()
                    final[b'f:t'] = str(timestamp)
                    b.put(rk, final)

        connection = hconnect(self._host, self._port, self._namespace, use_framed_compact=self._use_framed_compact)
        self._op(3, batch_put, connection, self.table_name, data)

    def _op(self, max_attempt, f, *args):
        attempt = self._attempt(max_attempt, f, *args)
        # reconnect for non-transient error.
        if attempt > max_attempt - 1:
            tmp_args = list(args)
            tmp_args[0] = hconnect(self._host, self._port, self._namespace, use_framed_compact=self._use_framed_compact)
            args = tuple(tmp_args)
            self.logger.info("Reconnecting to %s:%d thrift server.", self._host, self._port)
            self._attempt(int(max_attempt/2), f, *args)

    def _attempt(self, max_attempt, f, *args):
        attempt = 0
        while attempt < max_attempt:
            try:
                f(*args)
                break
            except (TException, socket.error):
                err, msg, _ = sys.exc_info()
                self.logger.error("{} {}\n".format(err, msg))
                self.logger.error(traceback.format_exc())
                attempt += 1
                sleep(.3)
        return attempt

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Tries to get new batch from priority queue. It makes self.GET_RETRIES tries and stops, trying to fit all
        parameters. Every new iteration evaluates a deeper batch. After batch is requested it is removed from the queue.

        :param max_n_requests: maximum number of requests
        :param partition_id: partition id to get batch from
        :param min_requests: minimum number of requests
        :param min_hosts: minimum number of hosts
        :param max_requests_per_host: maximum number of requests per host
        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        min_requests = kwargs.pop('min_requests')
        min_hosts = kwargs.pop('min_hosts', None)
        max_requests_per_host = kwargs.pop('max_requests_per_host', None)
        assert (max_n_requests > min_requests)
        # TODO: Workaround. Connecting each time may avoid ProtocolError: No protocol version header
        connection = hconnect(self._host, self._port, self._namespace, use_framed_compact=self._use_framed_compact)
        try:
            table = connection.table(self.table_name)

            meta_map = {}
            queue = {}
            limit = min_requests
            tries = 0
            count = 0
            prefix = to_bytes('%d_' % partition_id)
            # now_ts = int(time())
            # TODO: figure out how to use filter here, Thrift filter above causes full scan
            # filter = "PrefixFilter ('%s') AND SingleColumnValueFilter ('f', 't', <=, 'binary:%d')" % (prefix, now_ts)
            while tries < self.GET_RETRIES:
                tries += 1
                limit *= 5.5 if tries > 1 else 1.0
                self.logger.debug("Try %d, limit %d, last attempt: requests %d, hosts %d",
                                  tries, limit, count, len(queue.keys()))
                meta_map.clear()
                queue.clear()
                count = 0
                # XXX pypy hot-fix: non-exhausted generator must be closed manually
                # otherwise "finally" piece in table.scan() method won't be executed
                # immediately to properly close scanner (http://pypy.org/compat.html)
                scan_gen = table.scan(limit=int(limit), batch_size=256, row_prefix=prefix, sorted_columns=True)
                try:
                    for rk, data in scan_gen:
                        for cq, buf in six.iteritems(data):
                            if cq == b'f:t':
                                continue
                            stream = BytesIO(buf)
                            unpacker = Unpacker(stream)
                            for item in unpacker:
                                fprint, key_crc32, _, _ = item
                                if key_crc32 not in queue:
                                    queue[key_crc32] = []
                                if max_requests_per_host is not None and len(queue[key_crc32]) > max_requests_per_host:
                                    continue
                                queue[key_crc32].append(fprint)
                                count += 1

                                if fprint not in meta_map:
                                    meta_map[fprint] = []
                                meta_map[fprint].append((rk, item))
                        if count > max_n_requests:
                            break
                finally:
                    scan_gen.close()

                if min_hosts is not None and len(queue.keys()) < min_hosts:
                    continue

                if count < min_requests:
                    continue
                break

            self.logger.debug("Finished: tries %d, hosts %d, requests %d", tries, len(queue.keys()), count)

            # For every fingerprint collect it's row keys and return all fingerprints from them
            fprint_map = {}
            for fprint, meta_list in six.iteritems(meta_map):
                for rk, _ in meta_list:
                    fprint_map.setdefault(rk, []).append(fprint)

            results = []
            trash_can = set()

            for _, fprints in six.iteritems(queue):
                for fprint in fprints:
                    for rk, _ in meta_map[fprint]:
                        if rk in trash_can:
                            continue
                        for rk_fprint in fprint_map[rk]:
                            _, item = meta_map[rk_fprint][0]
                            _, _, encoded, score = item
                            request = self.decoder.decode_request(encoded)
                            request.meta[b'score'] = score
                            results.append(request)
                        trash_can.add(rk)

            with table.batch(transaction=True) as b:
                for rk in trash_can:
                    b.delete(rk)
            self.logger.debug("%d row keys removed", len(trash_can))
            return results
        finally:
            connection.close()

    def count(self):
        raise NotImplementedError


class PhoenixState(States):
    def __init__(self, host, port, schema, table_name, cache_size_limit,
                 drop_all_tables=False):
        self.logger = logging.getLogger("phoenix.states")
        self._host = host
        self._port = port
        self._schema = schema.upper()
        self._table_name = table_name.upper()
        self._state_stats = defaultdict(int)
        self._state_cache = LRUCacheWithStats(maxsize=cache_size_limit,
                                              stats=self._state_stats)
        self._state_last_updates = 0

        # TODO
        # Cannot find corresponding table options to 'block_cache_enabled', 'bloom_filter_type', 'in_memory'
        self._DDL = """
            CREATE TABLE {table} (
                URL_FPRINT VARCHAR(40) PRIMARY KEY,
                "s:state" UNSIGNED_TINYINT
            ) VERSIONS={versions}
        """.format(table=self._table_name, versions=1)

        self._SQL_UPDATE_STATE = """
            UPSERT INTO {table}
                (URL_FPRINT, "s:state")
            VALUES
                (?, ?)
        """.format(table=self._table_name)

        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()

            if self._schema:
                cursor.execute("SELECT DISTINCT(TABLE_NAME) FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = ?", (self._schema,))
            else:
                cursor.execute("SELECT DISTINCT(TABLE_NAME) FROM SYSTEM.CATALOG")

            tables = [e[0] for e in cursor.fetchall()]

            if drop_all_tables and self._table_name in tables:
                cursor.execute("DROP TABLE IF EXISTS {:s}".format(self._table_name))
                tables.remove(self._table_name)

            if self._table_name not in tables:
                try:
                    cursor.execute(self._DDL)
                except:
                    err, msg, _ = sys.exc_info()
                    self.logger.error("{} {}\n".format(err, msg))
        finally:
            conn.close()

    def update_cache(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]
        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()
            for obj in objs:
                fingerprint, state = obj.meta[b'fingerprint'], obj.meta[b'state']
                # prepare & write state change to happybase batch
                self._op(2, cursor.execute, self._SQL_UPDATE_STATE, (fingerprint, state))
                # update LRU cache with the state update
                self._state_cache[fingerprint] = state
                self._state_last_updates += 1
            self._update_batch_stats()
        finally:
            conn.close()

    def set_states(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]
        for obj in objs:
            obj.meta[b'state'] = self._state_cache.get(obj.meta[b'fingerprint'], States.DEFAULT)

    def flush(self):
        pass

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._state_cache]
        self._update_cache_stats(hits=len(fingerprints) - len(to_fetch), misses=len(to_fetch))
        if not to_fetch:
            return
        self.logger.debug('Fetching %d/%d elements from Phoenix (cache size %d)',
                          len(to_fetch), len(fingerprints), len(self._state_cache))
        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()
            for chunk in chunks(to_fetch, 65536):
                keys = [fprint for fprint in chunk]
                sql = 'SELECT "s:state" FROM {table} WHERE url_fprint IN (? ' + ',? ' * (len(keys)-1) + ')'
                sql = sql.format(table=self._table_name)
                self._op(2, cursor.execute, sql, keys)
                rows = [row[0] for row in cursor.fetchall() if len(row) > 1]
                for fprint, state in rows:
                    self._state_cache[fprint] = state
        finally:
            conn.close()

    def _update_batch_stats(self):
        self._state_stats['states.batches.sent'] += self._state_last_updates

    def _update_cache_stats(self, hits, misses):
        total_hits = self._state_stats['states.cache.hits'] + hits
        total_misses = self._state_stats['states.cache.misses'] + misses
        total = total_hits + total_misses
        self._state_stats['states.cache.hits'] = total_hits
        self._state_stats['states.cache.misses'] = total_misses
        self._state_stats['states.cache.ratio'] = total_hits / total if total else 0

    def get_stats(self):
        stats = self._state_stats.copy()
        self._state_stats.clear()
        return stats

    def _op(self, max_attempt, f, *args):
        attempt = self._attempt(max_attempt, f, *args)
        # reconnect for non-transient error.
        if attempt > max_attempt - 1:
            conn = connect(self._host, self._port, self._schema)
            try:
                cursor = conn.cursor()
                self.logger.info("Reconnecting to %s:%d phoenix query server.", self._host, self._port)
                self._attempt(int(max_attempt/2), cursor.execute, *args)
            finally:
                conn.close()

    def _attempt(self, max_attempt, f, *args):
        attempt = 0
        while attempt < max_attempt:
            try:
                f(*args)
                break
            except:
                err, msg, _ = sys.exc_info()
                self.logger.error("{} {}\n".format(err, msg))
                self.logger.error(traceback.format_exc())
                attempt += 1
                sleep(.3)
        return attempt


class PhoenixMetadata(Metadata):
    r_ctype = re.compile(rb'([0-9a-z._+-]+/[0-9a-z._+-]+);?', flags=re.IGNORECASE)
    r_charset = re.compile(rb'charset="?([0-9a-z-_]+)"?;?', flags=re.IGNORECASE)

    def __init__(self, host, port, schema, table_name,
                 drop_all_tables=False, data_block_encoding='DIFF'):

        self.logger = logging.getLogger("phoenix.metadata")
        self._host = host
        self._port = port
        self._schema = schema.upper()
        self._table_name = table_name.upper()
        self._DDL = """
            CREATE TABLE {table} (
                URL_FPRINT VARCHAR(40) PRIMARY KEY,
                "m:url" VARCHAR,
                "m:domain" VARCHAR(100),
                "m:netloc" VARCHAR(100),
                "m:domain_fprint" VARCHAR(40),
                "m:netloc_fprint" VARCHAR(40),
                "m:dest_fprint" VARCHAR(40),
                "m:seed_fprint" VARCHAR(40),
                "m:title" VARCHAR,
                "m:content_type" VARCHAR,
                "m:charset" VARCHAR,
                "m:headers" VARBINARY,
                "m:status_code" UNSIGNED_SMALLINT,
                "m:signature" VARCHAR(40),
                "m:depth" UNSIGNED_SMALLINT,
                "m:error" VARCHAR(100),
                "m:created_at" UNSIGNED_LONG,
                "c:content" VARBINARY
            ) DATA_BLOCK_ENCODING='{data_block_encoding}', VERSIONS={versions}
        """.format(table=self._table_name,
                   data_block_encoding=data_block_encoding,
                   versions=2147483647)

        self.SQL_ADD_SEED = """
            UPSERT INTO {table} (
                URL_FPRINT,
                "m:url",
                "m:domain",
                "m:netloc",
                "m:created_at",
                "m:domain_fprint",
                "m:netloc_fprint")
            VALUES
                (?, ?, ?, ?, ?, ?, ?)
        """.format(table=self._table_name)

        self.SQL_ADD_REDIRECT = """
            UPSERT INTO {table}
                (URL_FPRINT, "m:url", "m:created_at", "m:dest_fprint")
            VALUES
                (?, ?, ?, ?)
        """.format(table=self._table_name)

        self.SQL_PAGE_CRAWLED = """
            UPSERT INTO {table} (
                 URL_FPRINT,
                 "m:url",
                 "m:domain",
                 "m:netloc",
                 "m:created_at",
                 "m:domain_fprint",
                 "m:netloc_fprint",
                 "m:status_code",
                 "m:content_type",
                 "m:charset",
                 "m:headers",
                 "m:signature",
                 "m:title",
                 "m:seed_fprint",
                 "m:depth",
                 "c:content")
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.format(table=self._table_name)

        self.SQL_REQUEST_ERROR = """
            UPSERT INTO {table}
                (url_fprint, "m:url", "m:created_at", "m:error", "m:domain_fprint")
            VALUES
                (?, ?, ?, ?, ?)
        """.format(table=self._table_name)

        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()

            if self._schema:
                cursor.execute("SELECT DISTINCT(TABLE_NAME) FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = ?", (self._schema,))
            else:
                cursor.execute("SELECT DISTINCT(TABLE_NAME) FROM SYSTEM.CATALOG")

            tables = [e[0] for e in cursor.fetchall()]

            if drop_all_tables and self._table_name in tables:
                cursor.execute("DROP TABLE IF EXISTS {:s}".format(self._table_name))
                tables.remove(self._table_name)

            if self._table_name not in tables:
                self.logger.info(self._DDL)
                try:
                    cursor.execute(self._DDL)
                except:
                    err, msg, _ = sys.exc_info()
                    self.logger.error("{} {}\n".format(err, msg))
        finally:
            conn.close()

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def flush(self):
        pass

    def add_seeds(self, seeds):
        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()
            for seed in seeds:
                self._op(2, cursor.execute, self.SQL_ADD_SEED,
                         (seed.meta[b'fingerprint'],
                          norm_url(seed.url),
                          seed.meta[b'domain'][b'name'],
                          seed.meta[b'domain'][b'netloc'],
                          int(time()),
                          seed.meta[b'domain'][b'fingerprint'],
                          seed.meta[b'domain'][b'netloc_fingerprint'],
                          seed.meta[b'fingerprint'],
                          seed.meta[b'depth']
                          ))
        finally:
            conn.close()

    def page_crawled(self, response):
        fprint = response.meta[b'fingerprint']
        url = norm_url(response.url)
        domain = response.meta[b'domain'][b'name']
        netloc = response.meta[b'domain'][b'netloc']
        created_at = int(time())
        domain_fprint = response.meta[b'domain'][b'fingerprint']
        netloc_fprint = response.meta[b'domain'][b'netloc_fingerprint']
        headers = response.headers
        content_type = headers[b'Content-Type'][0] if b'Content-Type' in headers else None
        ctype = None
        charset = None
        if content_type:
            ctypes = self.r_ctype.findall(content_type)
            charsets = self.r_charset.findall(content_type)
            if ctypes:
                ctype = ctypes[0].lower()
            if charsets:
                charset = charsets[0].lower()

        title = response.meta[b'title'] if b'title' in response.meta else None
        #body = response.body
        #if any(e in content_type.lower() for e in ('utf8', 'utf-8')):
        #    body = body.decode('utf8')
        #elif any(e in content_type.lower() for e in ('sjis', 'shift-jis', 'shift_jis'))::
        #    body = body.decode('sjis')
        signature = self.md5(response.body)
        redirect_urls = response.request.meta.get(b'redirect_urls')
        redirect_fprints = response.request.meta.get(b'redirect_fingerprints')

        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()
            if redirect_urls:
                for url, fprint in zip(redirect_urls, redirect_fprints):
                    self._op(2, cursor.execute, self.SQL_ADD_REDIRECT,
                             (fprint,
                              norm_url(url),
                              int(time()),
                              redirect_fprints[-1]))

            try:
                self._op(2, cursor.execute, self.SQL_PAGE_CRAWLED,
                         (fprint,
                          url,
                          domain,
                          netloc,
                          created_at,
                          domain_fprint,
                          netloc_fprint,
                          int(response.status_code),
                          ctype,
                          charset,
                          packb(headers),
                          signature,
                          title,
                          response.meta[b'seed_fingerprint'],
                          response.meta[b'depth'],
                          response.body))
            except ValueError:
                self.logger.error("Failed to persist fetched data. fprint={}, url={}".format(fprint, url))
                err, msg, _ = sys.exc_info()
                self.logger.error("{} {}\n".format(err, msg))
        finally:
            conn.close()

    def links_extracted(self, request, links):
        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()
            links_dict = dict()
            for link in links:
                links_dict[link.meta[b'fingerprint']] = (link, link.url, link.meta[b'domain'])
            for link_fingerprint, (link, link_url, link_domain) in six.iteritems(links_dict):
                self._op(2, cursor.execute, self.SQL_ADD_SEED,
                         (link_fingerprint,
                          norm_url(link_url),
                          link_domain[b'name'],
                          link_domain[b'netloc'],
                          int(time()),
                          link_domain[b'fingerprint'],
                          link_domain[b'netloc_fingerprint'],
                          link.meta.get(b'seed_fingerprint', None),
                          link.meta.get(b'depth', None)))
        finally:
            conn.close()

    def request_error(self, request, error):
        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()
            self._op(2, cursor.execute, self.SQL_REQUEST_ERROR,
                     (request.meta[b'fingerprint'],
                      norm_url(request.url),
                      int(time()),
                      error,
                      request.meta[b'domain'][b'fingerprint']))
            if b'redirect_urls' in request.meta:
                for url, fprint in zip(request.meta[b'redirect_urls'], request.meta[b'redirect_fingerprints']):
                    self._op(2, cursor.execute, self.SQL_ADD_REDIRECT,
                             (fprint,
                              norm_url(url),
                              int(time()),
                              request.meta[b'redirect_fingerprints'][-1]))
        finally:
            conn.close()

    def update_score(self, batch):
        # never called ?
        pass

    def md5(self, body):
        return to_bytes(hashlib.md5(body).hexdigest())

    def _op(self, max_attempt, f, *args):
        attempt = self._attempt(max_attempt, f, *args)
        # reconnect for non-transient error.
        if attempt > max_attempt - 1:
            conn = connect(self._host, self._port, self._schema)
            try:
                cursor = conn.cursor()
                self.logger.info("Reconnecting to %s:%d phoenix query server.", self._host, self._port)
                self._attempt(int(max_attempt/2), cursor.execute, *args)
            finally:
                conn.close()

    def _attempt(self, max_attempt, f, *args):
        attempt = 0
        while attempt < max_attempt:
            try:
                f(*args)
                break
            except:
                err, msg, _ = sys.exc_info()
                self.logger.error("{} {}\n".format(err, msg))
                self.logger.error(traceback.format_exc())
                attempt += 1
                sleep(.3)
        return attempt


class PhoenixSeed(Seed):

    def __init__(self, host, port, schema, seed_partitions, table_name,
                 drop_all_tables=False, data_block_encoding='DIFF'):

        self.logger = logging.getLogger("phoenix.seed")
        self._host = host
        self._port = port
        self._schema = schema.upper()
        self._table_name = table_name.upper()
        self._DDL = """
            CREATE TABLE {table} (
                URL_FPRINT VARCHAR(40) PRIMARY KEY,
                "s:url" VARCHAR,
                "s:domain" VARCHAR(100),
                "s:netloc" VARCHAR(100),
                "s:strategy" VARCHAR(30),
                "s:depth_limit" UNSIGNED_SMALLINT,
                "s:partition_id" UNSIGNED_TINYINT,
                "s:token" VARCHAR(64),
                "s:created_at" UNSIGNED_LONG
            ) DATA_BLOCK_ENCODING='{data_block_encoding}', VERSIONS={versions}
        """.format(table=self._table_name,
                   data_block_encoding=data_block_encoding,
                   versions=1)

        self._SQL_ADD_SEED = """
            UPSERT INTO {table}
                (url_fprint, "s:url", "s:domain", "s:netloc", "s:strategy", "s:depth_limit", "s:partition_id", "s:token", "s:created_at")
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.format(table=self._table_name)

        self.seed_partitions = [i for i in range(0, seed_partitions)]
        self.seed_partitioner = Crc32NamePartitioner(self.seed_partitions)

        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()

            if self._schema:
                cursor.execute("SELECT DISTINCT(TABLE_NAME) FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = ?", (self._schema,))
            else:
                cursor.execute("SELECT DISTINCT(TABLE_NAME) FROM SYSTEM.CATALOG")

            tables = [e[0] for e in cursor.fetchall()]

            if drop_all_tables and self._table_name in tables:
                cursor.execute("DROP TABLE IF EXISTS {:s}".format(self._table_name))
                tables.remove(self._table_name)

            if self._table_name not in tables:
                self.logger.info(self._DDL)
                try:
                    cursor.execute(self._DDL)
                except:
                    err, msg, _ = sys.exc_info()
                    self.logger.error("{} {}\n".format(err, msg))
        finally:
            conn.close()

    def add_seeds(self, batch):
        if not batch:
            return

        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()

            now = int(time())
            for fprint, _, request, _ in batch:
                url = norm_url(request.url)
                domain = request.meta[b'domain']
                strategy = request.meta[b'strategy'][b'name']
                depth_limit = request.meta[b'strategy'][b'depth_limit']
                token = request.meta[b'token']
                slot = request.meta.get(b'slot')
                if slot is not None:
                    partition_id = self.seed_partitioner.partition(slot, self.seed_partitions)
                elif type(domain) == dict:
                    partition_id = self.seed_partitioner.partition(domain[b'name'], self.seed_partitions)
                elif type(domain) == int:
                    partition_id = self.seed_partitioner.partition_by_hash(domain, self.seed_partitions)
                else:
                    raise TypeError("partitioning key and info isn't provided")

                self._op(2, cursor.execute, self._SQL_ADD_SEED,
                         (fprint,
                          url,
                          domain[b'name'],
                          domain[b'netloc'],
                          strategy,
                          depth_limit,
                          partition_id,
                          token,
                          now))
        finally:
            conn.close()

    def _op(self, max_attempt, f, *args):
        attempt = self._attempt(max_attempt, f, *args)
        # reconnect for non-transient error.
        if attempt > max_attempt - 1:
            conn = connect(self._host, self._port, self._schema)
            try:
                cursor = conn.cursor()
                self.logger.info("Reconnecting to %s:%d phoenix query server.", self._host, self._port)
                self._attempt(int(max_attempt/2), cursor.execute, *args)
            finally:
                conn.close()

    def _attempt(self, max_attempt, f, *args):
        attempt = 0
        while attempt < max_attempt:
            try:
                f(*args)
                break
            except:
                err, msg, _ = sys.exc_info()
                self.logger.error("{} {}\n".format(err, msg))
                self.logger.error(traceback.format_exc())
                attempt += 1
                sleep(.3)
        return attempt


class PhoenixBackend(DistributedBackend):
    component_name = 'Phoenix Backend'

    def __init__(self, manager):
        self.manager = manager
        self.logger = logging.getLogger("phoenix.backend")
        settings = manager.settings

        self._port = int(settings.get('PHOENIX_PORT'))
        hosts = settings.get('PHOENIX_HOST')
        self._host = choice(hosts) if type(hosts) in [list, tuple] else hosts
        self._schema = settings.get('PHOENIX_SCHEMA')

        self._hbase_port = int(settings.get('HBASE_THRIFT_PORT'))
        thrift_hosts = settings.get('HBASE_THRIFT_HOST')
        self._hbase_host = choice(thrift_hosts) if type(thrift_hosts) in [list, tuple] else thrift_hosts
        self._hbase_namespace = settings.get('HBASE_NAMESPACE')

        self._min_requests = settings.get('BC_MIN_REQUESTS')
        self._min_hosts = settings.get('BC_MIN_HOSTS')
        self._max_requests_per_host = settings.get('BC_MAX_REQUESTS_PER_HOST')

        self._queue_partitions = settings.get('SPIDER_FEED_PARTITIONS')
        self._metadata = None
        self._queue = None
        self._states = None
        self._domain_metadata = None
        self._seed = None

    def _init_states(self, settings):
        self._states = PhoenixState(self._host, self._port, self._schema,
                                    settings.get('PHOENIX_STATE_TABLE'),
                                    settings.get('PHOENIX_STATE_CACHE_SIZE_LIMIT'),
                                    settings.get('PHOENIX_DROP_ALL_TABLES'))

    def _init_queue(self, settings):
        self._queue = HBaseQueue(self._hbase_host, self._hbase_port, self._hbase_namespace, self._queue_partitions,
                                 settings.get('HBASE_QUEUE_TABLE'),
                                 settings.get('HBASE_DROP_ALL_TABLES'),
                                 settings.get('HBASE_USE_SNAPPY'),
                                 settings.get('HBASE_USE_FRAMED_COMPACT'))

    def _init_metadata(self, settings):
        self._metadata = PhoenixMetadata(self._host, self._port, self._schema,
                                         settings.get('PHOENIX_METADATA_TABLE'),
                                         settings.get('PHOENIX_DROP_ALL_TABLES'),
                                         settings.get('PHOENIX_DATA_BLOCK_ENCODING'))

    def _init_domain_metadata(self, settings):
        self._domain_metadata = DomainCache(self._host, self._port, self._namespace,
                                            settings.get('HBASE_DOMAIN_METADATA_CACHE_SIZE'),
                                            settings.get('HBASE_DOMAIN_METADATA_TABLE'),
                                            settings.get('HBASE_DOMAIN_METADATA_BATCH_SIZE'),
                                            settings.get('HBASE_USE_FRAMED_COMPACT'))

    def _init_seed(self, settings):
        self._seed = PhoenixSeed(self._host, self._port, self._schema, self._queue_partitions,
                                 settings.get('PHOENIX_SEED_TABLE'),
                                 settings.get('PHOENIX_DROP_ALL_TABLES'),
                                 settings.get('PHOENIX_DATA_BLOCK_ENCODING'))

    @classmethod
    def strategy_worker(cls, manager):
        o = cls(manager)
        o._init_states(manager.settings)
        #o._init_domain_metadata(manager.settings)
        return o

    @classmethod
    def db_worker(cls, manager):
        o = cls(manager)
        o._init_queue(manager.settings)
        o._init_metadata(manager.settings)
        return o

    @classmethod
    def local(cls, manager):
        o = cls(manager)
        o._init_queue(manager.settings)
        o._init_states(manager.settings)
        o._init_seed(manager.settings)
        return o

    @property
    def metadata(self):
        return self._metadata

    @property
    def queue(self):
        return self._queue

    @property
    def states(self):
        return self._states

    @property
    def domain_metadata(self):
        return self._domain_metadata

    @property
    def seed(self):
        return self._seed

    def frontier_start(self):
        for component in [self.metadata, self.queue, self.states, self.domain_metadata]:
            if component is not None:
                component.frontier_start()

    def frontier_stop(self):
        for component in [self.metadata, self.queue, self.states, self.domain_metadata]:
            if component is not None:
                component.frontier_stop()

    def add_seeds(self, seeds):
        self.metadata.add_seeds(seeds)

    def page_crawled(self, response):
        self.metadata.page_crawled(response)

    def links_extracted(self, request, links):
        self.metadata.links_extracted(request, links)

    def request_error(self, page, error):
        self.metadata.request_error(page, error)

    def finished(self):
        raise NotImplementedError

    def get_next_requests(self, max_next_requests, **kwargs):
        self.logger.debug("Querying queue table.")
        results = []
        for partition_id in set(kwargs.pop('partitions', [i for i in range(self._queue_partitions)])):
            requests = self.queue.get_next_requests(
                max_next_requests, partition_id,
                min_requests=self._min_requests,
                min_hosts=self._min_hosts,
                max_requests_per_host=self._max_requests_per_host)
            results.extend(requests)
            self.logger.debug("Got %d requests for partition id %d", len(requests), partition_id)
        return results

    def get_stats(self):
        """Helper to get stats dictionary for the backend.

        For now it provides only HBase client stats.
        """
        stats = {}
        # No workaround fround. Seems to be missing Thrift I/F.
        # with time_elapsed('Call HBase backend get_stats()'):
        #     stats.update(self.connection.client.get_stats())
        if self._states:
            stats.update(self._states.get_stats())
        return stats


class PhoenixFeed(Queue):

    def __init__(self, phx_host, phx_port, phx_schema, feed_partitions, feed_table_name,
                 hbase_host, hbase_port, hbase_namespace, queue_partitions, queue_table_name,
                 drop_all_tables=False, use_snappy=False, use_framed_compact=False):

        self.logger = logging.getLogger("phoenix.feed")
        self._host = phx_host
        self._port = phx_port
        self._schema = phx_schema.upper()
        self._table_name = feed_table_name.upper()

        self._DDL = """
            CREATE TABLE {table} (
                URL_FPRINT VARCHAR(40) PRIMARY KEY,
                "f:partition_id" UNSIGNED_TINYINT,
                "f:url" VARCHAR,
                "f:created_at" UNSIGNED_LONG,
                "f:fetched_at" UNSIGNED_LONG,
                "f:next" UNSIGNED_LONG,
                "f:crontab" VARCHAR,
                "f:depth_limit" UNSIGNED_SMALLINT
            ) VERSIONS={versions}
        """.format(table=self._table_name, versions=1)

        self._SQL_SCHEDULE = """
            UPSERT INTO {table}
                (URL_FPRINT, "f:partition_id", "f:url", "f:created_at", "f:next", "f:crontab", "f:depth_limit")
            VALUES
                (?, ?, ?, ?, ?, ?, ?)
        """.format(table=self._table_name)

        self._SQL_GET_FEEDS = """
            SELECT
                URL_FPRINT,
                "f:url",
                "f:next",
                "f:crontab",
                "f:depth_limit"
            FROM
                {table}
            WHERE
                "f:partition_id" = ?
        """.format(table=self._table_name)

        self._SQL_UPDATE_SCHEDULE = """
            UPSERT INTO {table}
                (URL_FPRINT, "f:fetched_at", "f:next")
            VALUES
                (?, ?, ?)
        """.format(table=self._table_name)

        self.feed_partitions = [i for i in range(0, feed_partitions)]
        self.feed_partitioner = Crc32NamePartitioner(self.feed_partitions)

        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()

            if self._schema:
                cursor.execute("SELECT DISTINCT(TABLE_NAME) FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = ?", (self._schema,))
            else:
                cursor.execute("SELECT DISTINCT(TABLE_NAME) FROM SYSTEM.CATALOG")

            tables = [e[0] for e in cursor.fetchall()]

            if drop_all_tables and self._table_name in tables:
                cursor.execute("DROP TABLE IF EXISTS {:s}".format(self._table_name))
                tables.remove(self._table_name)

            if self._table_name not in tables:
                self.logger.info(self._DDL)
                try:
                    cursor.execute(self._DDL)
                except:
                    err, msg, _ = sys.exc_info()
                    self.logger.error("{} {}\n".format(err, msg))
        finally:
            conn.close()

        self._queue_delegate = HBaseQueue(hbase_host,
                                          hbase_port,
                                          hbase_namespace,
                                          queue_partitions,
                                          queue_table_name,
                                          drop_all_tables,
                                          use_snappy,
                                          use_framed_compact)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def schedule(self, batch):
        if not batch:
            return

        if batch[0][2].meta.get(b'feed', False):
            to_schedule = dict()
            now = int(time())
            for fprint, score, request, schedule in batch:
                if schedule:
                    assert b'domain' in request.meta
                    timestamp = request.meta[b'crawl_at'] if b'crawl_at' in request.meta else now
                    to_schedule.setdefault(timestamp, []).append((request, score))
            for timestamp, batch in six.iteritems(to_schedule):
                self._schedule(batch, timestamp)
        else:
            self._queue_delegate.schedule(batch)

    def _schedule(self, batch, timestamp):
        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()
            for request, score in batch:
                domain = request.meta[b'domain']
                fingerprint = request.meta[b'fingerprint']
                crontab = request.meta[b'strategy'][b'crontab']
                depth_limit = request.meta[b'strategy'][b'depth_limit']
                next_time = timestamp + int(CronTab(crontab).next())
                slot = request.meta.get(b'slot')
                if slot is not None:
                    partition_id = self.feed_partitioner.partition(slot, self.feed_partitions)
                elif type(domain) == dict:
                    partition_id = self.feed_partitioner.partition(domain[b'name'], self.feed_partitions)
                elif type(domain) == int:
                    partition_id = self.feed_partitioner.partition_by_hash(domain, self.feed_partitions)
                else:
                    raise TypeError("partitioning key and info isn't provided")

                self._op(3, cursor.execute, self._SQL_SCHEDULE,
                         (fingerprint,
                          partition_id,
                          norm_url(request.url),
                          timestamp,
                          next_time,
                          crontab,
                          depth_limit))
        finally:
            conn.close()

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        conn = connect(self._host, self._port, self._schema)
        try:
            cursor = conn.cursor()
            self._op(3, cursor.execute, self._SQL_GET_FEEDS, (partition_id,))
            feed_requests = []
            for row in cursor.fetchall():
                fprint = row[0]
                feed_url = row[1]
                next_time = row[2]
                crontab = row[3]
                depth_limit = row[4]
                now = int(time())
                if next_time <= now:
                    request = Request(feed_url)
                    request.meta[b'fingerprint'] = fprint
                    request.meta[b'seed_fingerprint'] = fprint
                    netloc, name, scheme, sld, tld, subdomain = parse_domain_from_url_fast(feed_url)
                    request.meta[b'domain'] = {
                        b'netloc': to_bytes(netloc),
                        b'name': to_bytes(name),
                        b'scheme': to_bytes(scheme),
                        b'sld': to_bytes(sld),
                        b'tld': to_bytes(tld),
                        b'subdomain': to_bytes(subdomain)
                    }
                    request.meta[b'strategy'] = {
                        b'crontab': crontab,
                        b'depth_limit': depth_limit
                    }
                    request.meta[b'score'] = 1.0
                    feed_requests.append(request)
                    next_time = now + int(CronTab(crontab).next())
                    self._op(3,
                             cursor.execute,
                             self._SQL_UPDATE_SCHEDULE,
                             (fprint, now, next_time))
        finally:
            conn.close()

        if len(feed_requests):
            return feed_requests
        else:
            return self._queue_delegate.get_next_requests(max_n_requests, partition_id, **kwargs)

    def count(self):
        raise NotImplementedError

    def _op(self, max_attempt, f, *args):
        attempt = self._attempt(max_attempt, f, *args)
        # reconnect for non-transient error.
        if attempt > max_attempt - 1:
            conn = connect(self._host, self._port, self._schema)
            try:
                cursor = conn.cursor()
                self.logger.info("Reconnecting to %s:%d phoenix query server.", self._host, self._port)
                self._attempt(int(max_attempt/2), cursor.execute, *args)
            finally:
                conn.close()

    def _attempt(self, max_attempt, f, *args):
        attempt = 0
        while attempt < max_attempt:
            try:
                f(*args)
                break
            except:
                err, msg, _ = sys.exc_info()
                self.logger.error("{} {}\n".format(err, msg))
                self.logger.error(traceback.format_exc())
                attempt += 1
                sleep(.3)
        return attempt


class PhoenixFeedBackend(PhoenixBackend):
    component_name = 'Phoenix Feed Backend'

    def __init__(self, manager):
        super(PhoenixFeedBackend, self).__init__(manager)

    def _init_queue(self, manager):
        settings = manager.settings
        self._queue = PhoenixFeed(self._host,
                                  self._port,
                                  self._schema,
                                  self._queue_partitions,
                                  settings.get('PHOENIX_FEED_TABLE'),
                                  self._hbase_host,
                                  self._hbase_port,
                                  self._hbase_namespace,
                                  self._queue_partitions,
                                  settings.get('HBASE_QUEUE_TABLE'),
                                  settings.get('HBASE_DROP_ALL_TABLES'),
                                  settings.get('HBASE_USE_SNAPPY'),
                                  settings.get('HBASE_USE_FRAMED_COMPACT'))

    @classmethod
    def strategy_worker(cls, manager):
        o = cls(manager)
        o._init_states(manager.settings)
        return o

    @classmethod
    def db_worker(cls, manager):
        o = cls(manager)
        o._init_queue(manager)
        o._init_metadata(manager.settings)
        return o

    @classmethod
    def local(cls, manager):
        o = cls(manager)
        o._init_queue(manager)
        o._init_states(manager.settings)
        o._init_seed(manager.settings)
        return o

    @property
    def queue(self):
        return self._queue

    def get_next_requests(self, max_next_requests, **kwargs):
        self.logger.debug("Querying feed table.")
        results = []
        for partition_id in set(kwargs.pop('partitions', [i for i in range(self._queue_partitions)])):
            requests = self.queue.get_next_requests(
                max_next_requests, partition_id,
                min_requests=self._min_requests,
                min_hosts=self._min_hosts,
                max_requests_per_host=self._max_requests_per_host)
            results.extend(requests)
            self.logger.debug("Got %d requests for partition id %d", len(requests), partition_id)
        return results


# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import re

from urllib.parse import urlparse
from frontera.core.components import States
from frontera.strategy import BaseCrawlingStrategy


class FeedCrawlingStrategy(BaseCrawlingStrategy):

    def __init__(self, manager, args, scheduled_stream, states_context):
        super(FeedCrawlingStrategy, self).__init__(manager, args, scheduled_stream, states_context)
        self.logger = logging.getLogger('strategy.feed')

    def read_seeds(self, fh):
        for url in fh:
            url = url.strip()
            req = self.create_request(url)
            #self.refresh_states(req)
            #if req.meta[b'state'] is States.NOT_CRAWLED:
            #    req.meta[b'state'] = States.QUEUED
            #    req.meta[b'depth'] = 0
            #    req.meta[b'feed'] = True
            #    req.meta[b'strategy'] = {
            #        b'crontab': '0 * * * *',  # every 0 minute
            #        b'depth_limit': 0
            #    }
            #    self.schedule(req)
            req.meta[b'depth'] = 0
            req.meta[b'token'] = '0'
            req.meta[b'feed'] = True
            req.meta[b'strategy'] = {
                b'name': 'feed',
                b'crontab': '0 * * * *',  # every 0 minute
                b'depth_limit': 0
            }
            self.schedule(req)

    def read_seeds_dict(self, seeds):
        for url in seeds['feed_urls']:
            url = url.strip()
            req = self.create_request(url)
            # self.refresh_states(req)
            #if req.meta[b'state'] is States.NOT_CRAWLED:
            #    req.meta[b'state'] = States.QUEUED
            #    req.meta[b'depth'] = 0
            #    req.meta[b'feed'] = True
            #    req.meta[b'feed_type'] = seeds['feed_type']
            #    req.meta[b'strategy'] = {
            #        b'crontab': seeds['crontab'],
            #        b'depth_limit': seeds.get('depth_limit', 0)
            #    }
            #    self.schedule(req)
            req.meta[b'uid'] = seeds.get('uid')
            req.meta[b'seed_fingerprint'] = req.meta[b'fingerprint']
            req.meta[b'depth'] = 0
            req.meta[b'token'] = seeds.get('token', '0')
            req.meta[b'feed'] = True
            req.meta[b'strategy'] = {
                b'name': 'feed',
                b'crontab': seeds['crontab'],
                b'depth_limit': seeds.get('depth_limit', 0)
            }
            self.schedule(req)

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED

    def filter_extracted_links(self, request, links):
        depth_limit = request.meta[b'strategy'][b'depth_limit']
        if depth_limit >= 0 and depth_limit < request.meta[b'depth'] + 1:
            self.logger.info('Depth limit exceeded. {:s} (depth: {:d} > limit: {:d})'
                             .format(request.url,
                                     request.meta[b'depth'] + 1,
                                     request.meta[b'strategy'][b'depth_limit']))
            return []

        def update(link):
            link.meta[b'depth'] = request.meta[b'depth'] + 1
            link.meta[b'strategy'] = request.meta[b'strategy']
            if b'seed_fingerprint' in request.meta:
                link.meta[b'seed_fingerprint'] = request.meta[b'seed_fingerprint']
            else:
                link.meta[b'seed_fingerprint'] = request.meta[b'fingerprint']
            return True

        return list(filter(update, links))

    def links_extracted(self, request, links):
        for link in links:
            if link.meta[b'state'] is States.NOT_CRAWLED:
                link.meta[b'state'] = States.QUEUED
                self.schedule(link, self.get_score(link))

    def request_error(self, request, error):
        request.meta[b'state'] = States.ERROR
        self.schedule(request, score=0.0, dont_queue=True)

    def get_score(self, link):
        depth = float(link.meta[b'depth'])
        score = 1.0 - (depth / (depth + 1.0))
        self.logger.info('Link extracted. {:s} (depth: {:s} score: {:s})'
                         .format(link.url, str(depth), str(score)))
        return score

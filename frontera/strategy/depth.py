# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

from frontera.core.components import States
from frontera.strategy import BaseCrawlingStrategy


class BreadthFirstCrawlingStrategy(BaseCrawlingStrategy):

    def __init__(self, manager, args, scheduled_stream, states_context):
        super(BreadthFirstCrawlingStrategy, self).__init__(manager, args, scheduled_stream, states_context)
        self.logger = logging.getLogger('strategy.bfs')

    def read_seeds(self, fh):
        for url in fh:
            url = url.strip()
            req = self.create_request(url)
            self.refresh_states(req)
            if req.meta[b'state'] is States.NOT_CRAWLED:
                req.meta[b'state'] = States.QUEUED
                req.meta[b'depth'] = 0
                req.meta[b'depth_limit'] = 0
                self.schedule(req)

    def read_seeds_dict(self, seeds):
        for url in seeds['seed_urls']:
            url = url.strip()
            req = self.create_request(url)
            self.refresh_states(req)
            if req.meta[b'state'] is States.NOT_CRAWLED:
                req.meta[b'state'] = States.QUEUED
                req.meta[b'depth'] = 0
                if 'depth_limit' in seeds:
                    req.meta[b'depth_limit'] = seeds['depth_limit']
                else:
                    req.meta[b'depth_limit'] = 0
                self.schedule(req)

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED

    def filter_extracted_links(self, request, links):
        return links

    def links_extracted(self, request, links):
        for link in links:
            link.meta[b'depth'] = request.meta[b'depth'] + 1
            link.meta[b'depth_limit'] = request.meta[b'depth_limit']
            if link.meta[b'state'] is States.NOT_CRAWLED:
                link.meta[b'state'] = States.QUEUED
                if link.meta[b'depth_limit'] == 0 or link.meta[b'depth'] <= link.meta[b'depth_limit']:
                    self.schedule(link, self.get_score(link))
                else:
                    self.logger.info('Depth limit exceeded. {:s} (depth: {:d} > limit: {:d})'
                                     .format(link.url, link.meta[b'depth'], link.meta[b'depth_limit']))

    def request_error(self, request, error):
        request.meta[b'state'] = States.ERROR
        self.schedule(request, score=0.0, dont_queue=True)

    def get_score(self, link):
        depth = float(link.meta[b'depth'])
        score = 1.0 - (depth / (depth + 1.0))
        self.logger.info('Link extracted. {:s} (depth: {:s} score: {:s})'
                         .format(link.url, str(depth), str(score)))
        return score


class DepthFirstCrawlingStrategy(BreadthFirstCrawlingStrategy):
    def __init__(self, manager, args, scheduled_stream, states_context):
        super(DepthFirstCrawlingStrategy, self).__init__(manager, args, scheduled_stream, states_context)
        self.logger = logging.getLogger('strategy.dfs')

    def get_score(self, link):
        depth = float(link.meta[b'depth'])
        score = depth / (depth + 1.0)
        self.logger.info('Link extracted. {:s} (depth: {:s} score: {:s})'
                         .format(link.url, str(depth), str(score)))
        return score

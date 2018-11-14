# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import re

from urllib.parse import urlparse
from frontera.core.components import States
from frontera.strategy import BaseCrawlingStrategy


class DomainCrawlingStrategy(BaseCrawlingStrategy):

    def __init__(self, manager, args, scheduled_stream, states_context):
        super(DomainCrawlingStrategy, self).__init__(manager, args, scheduled_stream, states_context)
        self.logger = logging.getLogger('strategy.domain')
        self.fastpass_score_threshold = manager.settings.get('QUEUE_FASTPASS_SCORE_THRESHOLD')

    def read_seeds(self, fh):
        for url in fh:
            url = url.strip()
            req = self.create_request(url)
            self.refresh_states(req)
            if req.meta[b'state'] is States.NOT_CRAWLED:
                req.meta[b'state'] = States.QUEUED
                req.meta[b'depth'] = 0
                req.meta[b'token'] = '0'
                req.meta[b'strategy'] = {
                    b'name': 'domain',
                    b'depth_limit': 0,
                    b'subdomain': True,
                    b'path_aware': False
                }
                self.schedule(req)

    def read_seeds_dict(self, seeds):
        # Priority: url_pattern > path_aware > subdomain
        url_patterns = seeds.get('url_patterns', [])
        path_aware = seeds.get('path_aware', False)
        subdomain = seeds.get('subdomain', True)
        if url_patterns:
            subdomain = path_aware = False
        elif path_aware:
            subdomain = True

        for url in seeds['seed_urls']:
            url = url.strip()
            path = urlparse(url).path
            req = self.create_request(url)
            self.refresh_states(req)
            if req.meta[b'state'] is States.NOT_CRAWLED:
                req.meta[b'uid'] = seeds.get('uid')
                req.meta[b'seed_fingerprint'] = req.meta[b'fingerprint']
                req.meta[b'state'] = States.QUEUED
                req.meta[b'depth'] = 0
                req.meta[b'token'] = seeds.get('token', '0')
                req.meta[b'strategy'] = {
                    b'name': 'domain',
                    b'depth_limit': seeds.get('depth_limit', 0),
                    b'subdomain': subdomain,
                    b'path_aware': path_aware,
                    b'seed_path': path,
                    b'url_patterns': url_patterns
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

        strategy = request.meta[b'strategy']
        req_domain = request.meta[b'domain']

        url_patterns = []
        for url_pattern in strategy.get(b'url_patterns', []):
            url_patterns.append(re.compile(url_pattern))

        def matches(link):
            link.meta[b'depth'] = request.meta[b'depth'] + 1
            link.meta[b'strategy'] = request.meta[b'strategy']
            if b'seed_fingerprint' in request.meta:
                link.meta[b'seed_fingerprint'] = request.meta[b'seed_fingerprint']
            else:
                link.meta[b'seed_fingerprint'] = request.meta[b'fingerprint']

            if url_patterns:
                if any(url_pattern.fullmatch(link.url) for url_pattern in url_patterns):
                    return True
                else:
                    return False
            else:
                link_domain = link.meta[b'domain']
                link_path = urlparse(link.url).path
                if ((req_domain[b'name'] != link_domain[b'name'])
                        or (strategy[b'path_aware'] and link_path.startswith(strategy[b'seed_path']) is False)
                        or (strategy[b'subdomain'] and req_domain[b'subdomain'] != link_domain[b'subdomain'])):
                    return False
                else:
                    return True

        return list(filter(matches, links))

    def links_extracted(self, request, links):
        for link in links:
            if link.meta[b'state'] is States.NOT_CRAWLED:
                link.meta[b'state'] = States.QUEUED
                score = self.get_score(link)
                if self.fastpass_score_threshold > 0.0 and score >= self.fastpass_score_threshold:
                    self.logger.info('Assign slot 0 {:s} (score: {:s})'.format(link.url, str(score)))
                    link.meta[b'slot'] = 0
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

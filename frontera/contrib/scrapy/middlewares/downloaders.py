import logging

from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.utils.url import url_is_from_spider
from scrapy.http import HtmlResponse

logger = logging.getLogger(__name__)


class RelCanonicalMiddleware(object):
    """
    downloader middleware automatically to redirect pages containing a rel=canonical in their contents to the canonical url (if the page itself is not the canonical one),
    """
    _extractor = LxmlLinkExtractor(restrict_xpaths=['//head/link[@rel="canonical"]'], tags=['link'], attrs=['href'])

    def process_response(self, request, response, spider):
        if isinstance(response, HtmlResponse) and response.body and getattr(spider, 'follow_canonical_links', False):
            rel_canonical = self._extractor.extract_links(response)
            if rel_canonical:
                rel_canonical = rel_canonical[0].url
                if rel_canonical != request.url:
                    logger.info("Redirecting (rel=\"canonical\") to %(rel_canonical)s from %(request)s", {'rel_canonical': rel_canonical, 'request': request}, extra={'spider': spider})
                    # return request.replace(url=rel_canonical, callback=lambda r: r if r.status == 200 else response)
                    return request.replace(url=rel_canonical)
        return response

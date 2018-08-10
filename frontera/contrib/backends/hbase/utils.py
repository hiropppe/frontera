from __future__ import absolute_import
from happybase import Batch
from happybase import Connection

from thriftpy.transport import TTransportException
import logging


def connect(host, port, namespace, use_framed_compact=False):
    kwargs = {
        'host': host,
        'port': port,
        'table_prefix': namespace,
        'table_prefix_separator': ':'
    }
    if use_framed_compact:
        kwargs.update({
            'protocol': 'compact',
            'transport': 'framed'
        })
    return Connection(**kwargs)


class HardenedBatch(Batch):
    def __init__(self, table, timestamp=None, batch_size=None,
                 transaction=False, wal=True):
        super(HardenedBatch, self).__init__(table, timestamp=timestamp, batch_size=batch_size, transaction=transaction,
                                            wal=wal)
        self.logger = logging.getLogger("happybase.batch")

    def send(self):
        try:
            super(HardenedBatch, self).send()
        except TTransportException:
            self.logger.exception("Exception happened during batch persistence")
            self.logger.warning("Cleaning up the batch")
            self._reset_mutations()
            pass

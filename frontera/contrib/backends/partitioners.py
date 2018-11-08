# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

from frontera.core.components import Partitioner
from cityhash import CityHash64
from frontera.utils.misc import get_crc32


class Crc32NamePartitioner(Partitioner):
    def partition(self, key, partitions=None):
        if not partitions:
            partitions = self.partitions
        idx = self.get_partition_idx(key, partitions)
        return partitions[idx]

    def partition_by_hash(self, value, partitions):
        size = len(partitions)
        idx = value % size
        return partitions[idx]

    def get_partition_idx(self, key, partitions):
        if key is None:
            return 0
        value = get_crc32(key)
        return value % len(partitions)

    def __call__(self, key, all_partitions, available):
        return self.partition(key, all_partitions)


class FingerprintPartitioner(Partitioner):
    def partition(self, key, partitions=None):
        if not partitions:
            partitions = self.partitions
        idx = self.get_partition_idx(key, partitions)
        return partitions[idx]

    def get_partition_idx(self, key, partitions):
        value = CityHash64(key)
        return value % len(partitions)

    def __call__(self, key, all_partitions, available):
        return self.partition(key, all_partitions)


class FastPassPartitioner(Partitioner):

    def __init__(self, delegate, fastpass_score_threshold):
        self.delegate = delegate
        self.fastpass_score_threshold = fastpass_score_threshold
        self.logger = logging.getLogger("partitioner")

    def partition(self, key, partitions=None):
        if not partitions:
            partitions = self.partitions
        try:
            score, key = key.split(b'_')
        except ValueError:
            self.logger.error('Illegal key ' + key.decode())
            return partitions[0]

        if float(score) >= self.fastpass_score_threshold or len(partitions) == 1:
            return partitions[0]
        idx = self.delegate.get_partition_idx(key, partitions[:-1])
        return partitions[idx + 1]

    def __call__(self, key, all_partitions, available):
        return self.partition(key, all_partitions)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from math import ceil, floor
from random import randrange
from random import shuffle


class BatcherFull(Exception):
    pass


class ShuffleBatcher(object):
    """


    Attributes:
        batch_size: The size of each minibatch
        min_size: This is the minimum size of the queue before
         it will release a batch.  The higher this number, the
         more overhead in the beginning of a job, yet the
         better the randomization of data
        capacity: This is the maximum amount of entries allowed
         in the queue.  The higher the number, the better the
         randomization, but the higher the memory usage and the
         lower the potential data throughput
    """
    def __init__(self, batch_size, min_size, capacity=None):
        self.batch_size = batch_size
        self.current_size = 0
        self.num_batch = int(ceil(min_size/batch_size))
        self.queue_size = self.num_batch * batch_size
        self.queue = [[] for _ in range(self.num_batch)]
        self.capacity = floor(capacity/batch_size) if \
            capacity else min_size * 10
        self.ready_queues = []

    def append(self, val):
        try:
            q_i = randrange(self.num_batch)
        except ValueError:
            raise BatcherFull(
                "Shuffle Queue is at capacity"
            )
        self.queue[q_i].append(val)
        if len(self.queue[q_i]) == self.batch_size:
            self.ready_queues.append(self.queue.pop(q_i))

            if (len(self.queue) + len(self.ready_queues)) \
                    >= self.capacity:
                self.num_batch -= 1
            else:
                self.queue.append([])

        self.current_size += 1

    def next(self):
        batch = self.ready_queues.pop()
        self.current_size -= self.batch_size
        self.queue.append([])
        self.num_batch += 1
        shuffle(batch)
        return batch

    def can_write(self):
        return self.current_size < \
               self.capacity * self.batch_size

    def can_read(self):
        return self.current_size >= \
               self.num_batch * self.batch_size

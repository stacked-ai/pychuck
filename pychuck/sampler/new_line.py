#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from pychuck.sampler.sampler import (Sampler, EndOfEpoch, EndOfMap,
                                     EndOfFile)


class NewLineSampler(Sampler):

    def next(self):
        if not self.opened:
            self.open('r')
        line = self.reader.readline()
        if line == '' or self.bytes_offset >= self.current_file.end:
            try:
                self.current_file = self.file_map.next()
            except EndOfMap:
                if self.stop_on_epoch:
                    raise EndOfEpoch
                self.current_file = self.file_map.next()
        self.bytes_offset = self.reader.tell()

        return line.strip('\n')

    @staticmethod
    def _get_next_pos(filename, bytes_offset):
        with open(filename, 'r') as line_file:
            line_file.seek(bytes_offset)
            while True:
                byte = line_file.read(1)
                if byte == '\n':
                    return line_file.tell() + 1
                elif byte == '':
                    raise EndOfFile

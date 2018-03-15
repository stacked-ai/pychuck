#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from pychuck.sampler.sampler import (Sampler, EndOfEpoch, EndOfMap,  # noqa
                                     EndOfFile)


class InvalidTypeMapError(Exception):
    pass


class EmptyLine(Exception):
    pass


class NewLineSampler(Sampler):

    def open(self, mode='rb'):
        super(NewLineSampler, self).open(mode='r')

    def __init__(self, file_map, stop_on_epoch=True, **kwargs):
        super(NewLineSampler, self).__init__(file_map=file_map,
                                             stop_on_epoch=stop_on_epoch)
        self.type_map = kwargs.get('type_map', str)
        self.skip_empty_lines = True

    def next(self):
        if not self.opened:
            self.open('r')
            self.reader.seek(self.current_file.start)
            self.bytes_offset = self.current_file.start

        line = self.reader.readline().strip()

        if line == '' or self.bytes_offset >= self.current_file.end:
            self.current_file = self.file_map.next()
            self._close_reader()
            return self.next()

        else:
            self.bytes_offset = self.reader.tell()
        return self._unpack_line(line)

    def _unpack_line(self, line):
        # apply type map, or other transformation of data here
        return line

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

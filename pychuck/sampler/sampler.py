#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from abc import ABCMeta, abstractmethod
from random import shuffle
from argparse import Namespace

import os


class EndOfMap(Exception):
    pass


class EndOfFile(Exception):
    pass


class EndOfEpoch(Exception):
    pass


class FileMap(object):
    """
    The file map is the struct that describes the partition of files for this
    dataset.  The file map will keep and provide meta data to the Sampler to
    know where to begin serial reads in a file, when to end, and where to go
    after an EOF
    Attributes:
        map: A list of tuples for a particular sampler with format:
          [(fullpath, start_offset, end_offset),...]
    """

    def __init__(self):
        self.map = []
        self.index = -1
        self.length = 0

    def add_file(self, fullpath, start=0, end=None):
        """
        This method appends a new file partition to the file map
        Args:
            file: the fullpath of the file
            start: the starting bytes offset (0)
            end: the ending bytes offset (None) - if None, the partition ends
              at the EOF
        """
        filemap = Namespace(**{
            'fullpath': fullpath,
            'start': start,
            'end': end
        })
        self.map.append(filemap)
        self.length += 1

    def next(self):
        """
        This method will return the next file map tuple in the list of maps.
        Returns:
             Namespace: fields fullpath, start, end)
        Raises:
            EndOfMap: When the end of the map is reached (epoch), the EndofMap
              exception is raised and the index is reset
        """
        try:
            next_map = self.map[self.index + 1]
            self.index += 1
            return next_map
        except IndexError:
            self.index = -1
            raise EndOfMap("EOM Reached")

    @classmethod
    def verbose_from_list(cls, file_list):
        """
        This method will define a complete file map with all sizes in tact.
        That is, the map will have every file listed as (fullpath, 0,
        total_number_of_bytes)

        Returns:
            FileMap: completely defined (verbose)

        """
        new_map = cls()
        for fullpath in file_list:
            try:
                size = os.path.getsize(fullpath)
            except FileNotFoundError:
                raise ValueError("No file named {} could be found from your "
                                 "file_list".format(file_list))
            new_map.add_file(fullpath, 0, size)

        return new_map


class Sampler(object):
    """
    Attributes:
        file_map: a FileMap object for this sampler defining
          where to read data from
    """
    __metaclass__ = ABCMeta

    def __init__(self, file_map, stop_on_epoch=False):
        self.file_map = file_map
        self.current_file = self.file_map.next()
        self.reader = None
        self.opened = False
        self.bytes_offset = self.current_file.start
        self.stop_on_epoch = stop_on_epoch

    def _open_reader(self, mode='rb'):
        self.reader = open(self.current_file.fullpath,
                           mode=mode)
        self.reader.seek(self.current_file.start)
        self.opened = True

    def _close_reader(self):
        self.reader.close()
        self.opened = False

    def open(self, mode='rb'):
        self._open_reader(mode)

    @staticmethod
    @abstractmethod
    def _get_next_pos(filename, bytes_offset):
        """
        This function will allow a sampler to return
        a logical position within a file to partition
        that file.  To be used when partitioning large
        files amongst various workers.

        Args:
            filename: the name of the file to inspect
            bytes_offset: the position in the file, in bytes
              from which to inspect
        Returns:
            int: position, the offset of bytes to the
              next position in the file
        """
        pass

    @abstractmethod
    def next(self):
        """
        This function will grab the next sample in the
        dataset
        Returns:
             sample: the next sample in the dataset
        """
        pass

    @classmethod
    def partition(cls, filenames, no_workers, shuffled=False):
        """
        This method will partion the file(s) evenly,
        in number of bytes, and return a list of samplers
        for each worker
        Args:
            filenames: [string], a list of absolute paths to the files
            no_workers: int, the number of worker
            shuffled: bool, whether or not to shuffle the files into
              the partitions

        Returns:
            [Sampler]: a list of samplers, length no_workers
        """
        big_file_map = FileMap.verbose_from_list(filenames).map
        total_bytes = 0
        for each_file in big_file_map:
            total_bytes += int(each_file.end)
        target_offset = int(total_bytes/no_workers)
        split_map = FileMap()
        bytes_processed = 0
        current_offset = 0
        current_file_offset = 0
        for file_map in big_file_map:
            while True:
                # if the current file ends before the next target
                if file_map.end + bytes_processed < \
                        current_offset + target_offset:
                    # add the file
                    split_map.add_file(file_map.fullpath,
                                       current_file_offset,
                                       file_map.end)
                    # update current offset
                    current_offset += file_map.end - current_file_offset
                    # reset the file offset
                    current_file_offset = 0
                    # add the file's bytes to the processed count
                    bytes_processed += file_map.end
                    # next file
                    break
                # if the next target is within this file
                else:
                    # stash current file offset
                    tmp_offset = current_file_offset
                    # update current file offset with nearest sample offset
                    try:
                        current_file_offset = cls._get_next_pos(
                            file_map.fullpath,
                            tmp_offset + target_offset) - 1
                    except EndOfFile as e:
                        print("SOMETHING WAY WRONG BRO")
                        raise e
                    # add the range of bytes to the file offset
                    split_map.add_file(file_map.fullpath,
                                       tmp_offset,
                                       current_file_offset)
                    # update current offset
                    current_offset += current_file_offset - tmp_offset

        partition = [FileMap() for _ in range(no_workers)]

        map = split_map.map
        print("MAP ", map)
        if shuffled:
            shuffle(map)

        while map:
            for file_map in partition:
                try:
                    next_add = map.pop()
                except IndexError:
                    break
                print(next_add)
                file_map.add_file(next_add.fullpath,
                                  next_add.start,
                                  next_add.end)

        return [cls(file_map) for file_map in partition]

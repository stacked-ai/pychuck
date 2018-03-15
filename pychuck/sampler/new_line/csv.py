#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from pychuck.sampler.new_line import (NewLineSampler, InvalidTypeMapError)


class TabularCsvSampler(NewLineSampler):

    def _unpack_line(self, line):
        output = []
        results = [x.strip() for x in line.split(',')]
        if isinstance(self.type_map, list):
            try:
                assert len(results) == len(self.type_map)
            except AssertionError:
                raise InvalidTypeMapError(
                    "Type map must be the same length of the results: {}"
                    "".format(results)
                )
            for cast, result in zip(self.type_map, results):
                try:
                    output.append(cast(result))
                except NameError:
                    raise InvalidTypeMapError(
                        "Type map must be callable valid python or package "
                        "data types.  Cannot cast {} with type {}"
                        "".format(result, cast)
                    )
                except ValueError:
                    raise InvalidTypeMapError(
                        "Type map unable to cast {} into type {}"
                        "".format(result, cast)
                    )
        return output

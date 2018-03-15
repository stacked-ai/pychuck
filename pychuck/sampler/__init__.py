#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
The sampler defines how to slice files
into data entries and provides helpers
to partition files
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from pychuck.sampler.new_line import NewLineSampler  # noqa
from pychuck.sampler.new_line.csv import TabularCsvSampler  # noqa

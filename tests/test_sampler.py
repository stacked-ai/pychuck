#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import pytest
from mock import MagicMock, patch, file_spec
from pychuck.sampler.sampler import Sampler

parametrize = pytest.mark.parametrize


class TestSampler(object):

    @parametrize('num_wkr', [1, 2])
    def test_partition(self, num_wkr):
        with patch('builtins.open') as mock_open:
            with patch('os.path.getsize') as mock_size:
                with patch(
                    'pychuck.sampler.sampler.Sampler._get_next_pos'
                ) as mock_pos:
                    mock_pos.return_value = 8
                    mock_size.return_value = 8
                    mock_open.return_value = MagicMock(file_spec)
                    mock_open.read.return_value = '1,2,3\n4,5,6'
                    s = Sampler.partition(['any.file'], num_wkr)
                    if num_wkr == 1:
                        try:
                            len(s)
                        except TypeError:
                            pass
                    else:
                        assert len(s) == 2

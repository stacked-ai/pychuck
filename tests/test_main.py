# -*- coding: utf-8 -*-
from pytest import raises

# The parametrize function is generated, so this doesn't work:
#
#     from pytest.mark import parametrize
#
import pytest
parametrize = pytest.mark.parametrize

from pychuck import metadata  # noqa
from pychuck.main import main  # noqa


class TestMain(object):
    @parametrize('helparg', ['-h', '--help'])
    def test_help(self, helparg, capsys):
        with raises(SystemExit) as exc_info:
            main(['progname', helparg])
        out, err = capsys.readouterr()
        # Should have printed some sort of usage message. We don't
        # need to explicitly test the content of the message.
        assert 'usage' in out
        # Should have used the program name from the argument
        # vector.
        assert 'progname' in out
        # Should exit with zero return code.
        assert exc_info.value.code == 0

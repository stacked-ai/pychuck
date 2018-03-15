#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
from argparse import Namespace

###############
# Event Flags #
###############

FLAG_EV_Q_PAUSE = b"QUEUE_PAUSE"
FLAG_EV_END = b"END_TASK"
FLAG_EV_EOF = b"EOF"
FLAG_EV_READY = b"QUEUE_READY"
FLAG_EV_START = b"START"

##############
# COMM Flags #
##############

FLAG_WKR_ID_EV = b"WORKER_"
FLAG_WKR_WAIT = b"WORKER_WAITING_"
FLAG_WKR_RST = b"WORKER_RESET_"
FLAG_WKR = b"WORKER"

##############
# State enum #
##############

SYSTEM_STATE = Namespace(
    **{
        "producing": 0,
        # Workers are producing and app is
        # consuming
        "eof_buffer": 1,
        # Workers are no longer but have
        # buffer producing app is consuming
        "consuming": 2,
        # Data queue is empty, workers paused,
        # consuming only
        "idle": 3
    }
)

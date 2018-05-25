#!/usr/bin/env python
# coding=UTF-8

import datetime

from lib.task import Task


class TestTask(Task):
    def __init__(self, cluster_name, pool_name, rbd_name):
        super(TestTask, self).__init__()

        self.cluster_name = cluster_name
        self.pool_name = pool_name
        self.rbd_name = rbd_name

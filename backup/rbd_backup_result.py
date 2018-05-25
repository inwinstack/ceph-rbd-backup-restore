#!/usr/bin/env python
# coding=UTF-8

import datetime, time

from lib.common_func import *


class RBD_Backup_Task_Result(object):

    def __init__(self, log):
        self.log = log

        self.snapshot_create_task_result = []
        self.snapshot_delete_task_result = []
        self.export_task_result = []

    def add_snapshort_create_result(self, result):
        self.snapshot_create_task_result.append(result)
        return

    def add_snapshort_delete_result(self, result):
        self.snapshot_delete_task_result.append(result)
        return

    def add_export_task_result(self, result):
        self.export_task_result.append(result)
        return

    def write_result_to_file(self, filepath):

        return

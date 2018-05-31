#!/usr/bin/env python
# coding=UTF-8
# Author: Yu-Jung Cheng

import os

from backup.rbd_backup_const import RBD_Backup_Const as const

from lib.metafile import MetaFile


class RBD_Backup_Task_Result(object):

    def __init__(self, log):
        self.log = log

        self.snapshot_create_task_result = []
        self.snapshot_delete_task_result = []
        self.export_task_result = []
        self.backup_circle_delete_result = []

    def add_snapshort_create_result(self, result):
        self.snapshot_create_task_result.append(dict(result))
        return True

    def add_snapshort_delete_result(self, result):
        self.snapshot_delete_task_result.append(dict(result))
        return True

    def add_export_task_result(self, result):
        self.export_task_result.append(dict(result))
        return True

    def add_backup_circle_delete_result(self, result):
        self.backup_circle_delete_result.append(result)
        return True

    def write_to_file(self, backup_name):
        try:
            result_dir = "%s/%s" % (const.TMP_DIR, backup_name)
            os.system("mkdir -p %s" % result_dir)
            self.log.debug("Created RBD backup result directory '%s'." % result_dir)

            snap_create_file = "%s/snapshot_create.result" % result_dir
            export_file = "%s/export.result" % result_dir
            snap_delete_file = "%s/snapshot_delete.result" % result_dir
            circle_delete_file = "%s/backup_circle_delete.result" % result_dir

            self.log.debug("Write RBD result to '%s'." % result_dir)
            metafile = MetaFile(self.log)
            metafile.write(self.snapshot_create_task_result, filepath=snap_create_file)
            metafile.write(self.snapshot_delete_task_result, filepath=snap_delete_file)
            metafile.write(self.export_task_result, filepath=export_file)
            metafile.write(self.backup_circle_delete_result, filepath=circle_delete_file)
            return True
        except Exception as e:
            self.log.error("Fail to write backup results to files. %s" % e)
            return False

#!/usr/bin/env python
# coding=UTF-8
from lib.common_func import *

from backup.rbd_backup_const import RBD_Backup_Const as const

from task.snapshot_create import SnapshotCreateTask
from task.snapshot_delete import SnapshotDeleteTask
from task.snapshot_purge import SnapshotPurgeTask
from task.export_full import ExportFullTask
from task.export_diff import ExportDiffTask


class RBD_Backup_Task_Maker(object):

    def __init__(self, log, cluster_name, conf_file, keyring_file):
        self.log = log
        self.cluster_name = cluster_name
        self.conf_file = conf_file
        self.keyring_file = keyring_file

        self.export_type_full = 'full'
        self.export_type_diff = 'diff'

        self.snap_datetime_fmt = const.SNAPSHOT_NAME_DATETIME_FMT
        self.export_datetime_fmt = const.BACKUP_NAME_DATETIME_FMT

    def set_export_full_type(self, value):
        self.export_type_full = value

    def set_export_diff_type(self, value):
        self.export_type_diff = value

    def get_rbd_snapshot_create_task(self, pool_name, rbd_name):
        try:
            snap_task = SnapshotCreateTask(self.conf_file,
                                           self.keyring_file,
                                           self.cluster_name,
                                           pool_name,
                                           rbd_name,
                                           snap_datetime_fmt=self.snap_datetime_fmt)
            self.log.debug("SnapshotCreateTask: ",
                           "cluster name = %s" % self.cluster_name,
                           "pool name = %s" % pool_name,
                           "rbd name = %s" % rbd_name)
            return snap_task
        except Exception as e:
            self.log.error('Fail to create RBD snapshot create task. %s' % e)
            return False

    def get_rbd_snapshot_delete_task(self, pool_name, rbd_name, snap_name):
        try:
            snap_task = SnapshotDeleteTask(self.conf_file,
                                           self.keyring_file,
                                           self.cluster_name,
                                           pool_name,
                                           rbd_name,
                                           snap_name)
            self.log.debug("SnapshotDeleteTask: ",
                           "cluster name = %s" % self.cluster_name,
                           "pool name = %s" % pool_name,
                           "rbd name = %s" % rbd_name,
                           "snapshot name = %s" % snap_name)
            return snap_task
        except Exception as e:
            self.log.error('Fail to create RBD snapshot delete task. %s' % e)
            return False

    def get_rbd_export_full_task(self, pool_name, rbd_name, snap_name, dest_path):
        try:
            export_task = ExportFullTask(self.conf_file,
                                         self.keyring_file,
                                         self.cluster_name,
                                         pool_name,
                                         rbd_name,
                                         snap_name=snap_name,
                                         dest_path=dest_path,
                                         export_datetime_fmt=self.export_datetime_fmt)
            export_task.export_type = self.export_type_full
            self.log.debug("ExportFullTask: ",
                           "cluster name = %s" % self.cluster_name,
                           "pool name = %s" % pool_name,
                           "rbd_name = %s" % rbd_name,
                           "snapshot name = %s" % snap_name,
                           "destination path = %s" % dest_path)
            return export_task
        except Exception as e:
            self.log.error('Fail to create RBD export full task. %s' % e)
            return False

    def get_rbd_export_diff_task(self, pool_name, rbd_name, snap_name, from_snap, dest_path):
        try:
            export_task = ExportDiffTask(self.conf_file,
                                         self.keyring_file,
                                         self.cluster_name,
                                         pool_name,
                                         rbd_name,
                                         snap_name=snap_name,
                                         dest_path=dest_path,
                                         from_snap=from_snap,
                                         export_datetime_fmt=self.export_datetime_fmt)
            export_task.export_type = self.export_type_diff
            self.log.debug("ExportFullTask: ",
                           "cluster name = %s" % self.cluster_name,
                           "pool name = %s" % pool_name,
                           "rbd_name = %s" % rbd_name,
                           "snapshot name = %s" % snap_name,
                           "destination path = %s" % dest_path)
            return export_task
        except Exception as e:
            self.log.error('Fail to create RBD export diff task. %s' % e)
            return False

#!/usr/bin/env python
# coding=UTF-8
# Author: Yu-Jung Cheng

from lib.common_func import *

from task.import_full import ImportFullTask
from task.import_diff import ImportDiffTask
from task.rbd_delete import RBDDeleteTask
from task.snapshot_create import SnapshotCreateTask
from task.snapshot_purge import SnapshotPurgeTask
#from task.merge_diff import MergeDiffTask


class RBD_Restore_Task_Maker(object):

    def __init__(self, log, cluster_name, conf_file, keyring_file):
        self.log = log
        self.cluster_name = cluster_name
        self.conf_file = conf_file
        self.keyring_file = keyring_file

        self.import_type_full = 'full'
        self.import_type_diff = 'diff'

    def get_rbd_snapshot_purge_task(self, pool_name, rbd_name):
        try:
            purge_task = SnapshotPurgeTask(self.conf_file,
                                           self.keyring_file,
                                           self.cluster_name,
                                           pool_name,
                                           rbd_name)
            self.log.debug("SnapshotPurgeTask: ",
                           "cluster name = %s" % self.cluster_name,
                           "pool name = %s" % pool_name,
                           "rbd name = %s" % rbd_name)
            return purge_task
        except Exception as e:
            self.log.error('Fail to create RBD snapshot purge task. %s' % e)
            return False

    def get_rbd_snapshot_create_task(self, pool_name, rbd_name, snap_name):
        try:
            snap_task = SnapshotCreateTask(self.conf_file,
                                           self.keyring_file,
                                           self.cluster_name,
                                           pool_name,
                                           rbd_name,
                                           snap_name=snap_name)
            self.log.debug("SnapshotCreateTask: ",
                           "cluster name = %s" % self.cluster_name,
                           "pool name = %s" % pool_name,
                           "rbd name = %s" % rbd_name,
                           "snapshot name = %s" % snap_name)
            return snap_task
        except Exception as e:
            self.log.error('Fail to create RBD snapshot create task. %s' % e)
            return False

    def get_rbd_import_full_task(self, pool_name, rbd_name, src_path, src_file):
        try:
            import_task = ImportFullTask(self.conf_file,
                                         self.keyring_file,
                                         self.cluster_name,
                                         pool_name,
                                         rbd_name,
                                         src_path,
                                         src_file)
            import_task.import_type = self.import_type_full
            self.log.debug("ImportFullTask: ",
                           "cluster name = %s" % self.cluster_name,
                           "pool name = %s" % pool_name,
                           "rbd_name = %s" % rbd_name,
                           "source path = %s" % src_path,
                           "source file = %s" % src_file)

            return import_task
        except Exception as e:
            self.log.error('Fail to create RBD import full task. %s' % e)
            return False

    # to import diff, you have to have previous full or diff import.
    def get_rbd_import_diff_task(self, pool_name, rbd_name, src_path, src_file):
        try:
            import_task = ImportDiffTask(self.conf_file,
                                         self.keyring_file,
                                         self.cluster_name,
                                         pool_name,
                                         rbd_name,
                                         src_path,
                                         src_file)
            import_task.import_type = self.import_type_diff
            self.log.debug("ImportDiffTask: ",
                           "cluster name = %s" % self.cluster_name,
                           "pool name = %s" % pool_name,
                           "rbd_name = %s" % rbd_name,
                           "source path = %s" % src_path,
                           "source file = %s" % src_file)
            return import_task
        except Exception as e:
            self.log.error('Fail to create RBD import diff task. %s' % e)
            return False

    def get_rbd_delete_task(self, pool_name, rbd_name):
        try:
            delete_task = RBDDeleteTask(self.conf_file,
                                        self.keyring_file,
                                        self.cluster_name,
                                        pool_name,
                                        rbd_name)
            self.log.debug("RBDDeleteTask: ",
                           "cluster name = %s" % self.cluster_name,
                           "pool name = %s" % pool_name,
                           "rbd_name = %s" % rbd_name)
            return delete_task
        except Exception as e:
            self.log.error('Fail to create RBD delete task. %s' % e)
            return False

    def get_rbd_merge_diff(self):
        return

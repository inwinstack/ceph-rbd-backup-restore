#!/usr/bin/env python
# coding=UTF-8
# Author: Yu-Jung Cheng


import os

from lib.common_func import *
from lib.metafile import MetaFile
from lib.directory import Directory

from backup.rbd_backup_const import RBD_Backup_Const as const


class RBD_Backup_Metadata(object):

    def __init__(self, log, path, meta_dir=None):
        self.log = log

        if meta_dir == None:
            meta_dir = const.META_DIRNAME
        self.path = "%s/%s" % (path, meta_dir)

        self.metafile = MetaFile(log)
        self.directory = Directory(log)

        self.CLUSTER_INFO_META = const.META_CLUSTER_INFO
        self.CLUSTER_RBD_INFO_META = const.META_CLUSTER_RBD_INFO
        self.CLUSTER_RBD_SNAPSHOT_INFO_META = const.META_CLUSTER_RBD_SNAPSHOT_INFO

        self.BACKUP_EXPORT_INFO_META = const.META_BACKUP_EXPORT_INFO
        self.BACKUP_CIRCLE_INFO_META = const.META_BACKUP_CIRCLE_INFO
        self.BACKUP_INCREMENTAL_INFO_META = const.META_BACKUP_INCREMENTAL_INFO

        self.LIST_COUNTER_KEY = const.META_LIST_COUNTER_KEY
        self.MAX_BACKUP_INFO_RETAIN_COUNT = const.META_ROTATION_LENGTH

    def set_cluster_info(self, meta):
        filepath = os.path.join(self.path, self.CLUSTER_INFO_META)
        return self.metafile.write(meta, filepath=filepath)

    def set_backup_export_info(self, export_name, meta):
        filepath = os.path.join(self.path,
                                self.BACKUP_EXPORT_INFO_META,
                                export_name)
        return self.metafile.write(meta, filepath=filepath)

    def set_rbd_info(self, pool_name, rbd_name, meta):
        filepath = os.path.join(self.path,
                                pool_name,
                                rbd_name,
                                self.CLUSTER_RBD_INFO_META)
        return self.metafile.write(meta, filepath=filepath)

    def set_backup_circle_list(self, pool_name, rbd_name, meta):
        filepath = os.path.join(self.path,
                                pool_name,
                                rbd_name,
                                self.BACKUP_CIRCLE_INFO_META)
        return self.metafile.write(meta, filepath=filepath)

    def set_backup_snapshot_list(self, pool_name, rbd_name, meta):
        filepath = os.path.join(self.path,
                                pool_name,
                                rbd_name,
                                self.CLUSTER_RBD_SNAPSHOT_INFO_META)
        return self.metafile.write(meta, filepath=filepath)

    def set_backup_incremental_list(self, pool_name, rbd_name, circle_name, meta):
        filepath = os.path.join(self.path,
                                pool_name,
                                rbd_name,
                                self.BACKUP_INCREMENTAL_INFO_META,
                                circle_name)
        return self.metafile.write(meta, filepath=filepath)

    def get_cluster_info(self):
        self.log.debug("Metadata - Get backup cluster info.")
        filepath = os.path.join(self.path, self.CLUSTER_INFO_META)
        cluster_info = self.metafile.read(filepath=filepath)
        if isinstance(cluster_info, dict):
            return cluster_info
        self.log.debug("Metadata - Invalid backup cluster info.")
        return {}

    def get_rbd_info(self, pool_name, rbd_name):
        self.log.debug("Metadata - Get backup RBD info.")
        filepath = os.path.join(self.path,
                                pool_name,
                                rbd_name,
                                self.CLUSTER_RBD_INFO_META)
        rbd_info = self.metafile.read(filepath=filepath)
        if isinstance(rbd_info, dict):
            return rbd_info
        self.log.debug("Metadata - Invalid backup RBD info.")
        return {}

    def get_backup_name_list(self):
        self.log.debug("Metadata - Get backup name list.")
        directory = Directory(self.log)
        file_list = directory.get_file_list(self.path,
                                            self.BACKUP_EXPORT_INFO_META)
        if isinstance(file_list, list):
            return sorted(file_list)
        self.log.debug("Metadata - Invalid backup name list.")
        return []

    def get_backup_rbd_list(self, backup_name):
        self.log.debug("Metadata - Get backup rbd list.")
        filepath = os.path.join(self.path,
                                self.BACKUP_EXPORT_INFO_META,
                                backup_name)
        rbd_list = self.metafile.read(filepath=filepath)
        if isinstance(rbd_list, list):
            return rbd_list
        self.log.debug("Metadata - Invalid backup rbd list.")
        return []

    def get_backup_rbd_info_list(self):
        self.log.debug("Metadata - Get backup rbd info list.")
        directory = Directory(self.log)
        dir_name_list = directory.get_dir_list(self.path)
        rbd_info_list = []
        for dir_name in dir_name_list:
            if dir_name != self.BACKUP_EXPORT_INFO_META:
                rbd_name_list = directory.get_dir_list(self.path, dir_name)
                for rbd_name in rbd_name_list:
                    rbd_info = self.get_rbd_info(dir_name, rbd_name)
                    rbd_info_list.append((dir_name, rbd_name, rbd_info))
        return rbd_info_list

    def get_backup_circle_list(self, pool_name, rbd_name):
        self.log.debug("Metadata - Get backup circle list.")
        filepath = os.path.join(self.path,
                                pool_name,
                                rbd_name,
                                self.BACKUP_CIRCLE_INFO_META)
        circle_list = self.metafile.read(filepath=filepath)
        if isinstance(circle_list, list):
            sorted_circle_list = sort_dict_list(circle_list,
                                                self.LIST_COUNTER_KEY,
                                                reverse=False)
            if sorted_circle_list == False:
                self.log.debug("Metadata - Unable to sort backup circle list.")
            else:
                return sorted_circle_list
        self.log.debug("Metadata - Invalid backup circle list.")
        return []

    def get_backup_snapshot_list(self, pool_name, rbd_name):
        self.log.debug("Metadata - Get backup snapshot list.")
        filepath = os.path.join(self.path,
                                pool_name,
                                rbd_name,
                                self.CLUSTER_RBD_SNAPSHOT_INFO_META)
        snapshot_list = self.metafile.read(filepath=filepath)
        if isinstance(snapshot_list, list):
            sorted_snapshot_list = sort_dict_list(snapshot_list,
                                                  self.LIST_COUNTER_KEY,
                                                  reverse=False)
            if sorted_snapshot_list == False:
                self.log.debug("Metadata - Unable to sort backup snapshot list.")
            else:
                return sorted_snapshot_list
        self.log.debug("Metadata - Invalid backup snapshot list.")
        return []

    def get_backup_incremental_list(self, pool_name, rbd_name, circle_name):
        self.log.debug("Metadata - Get backup incremental list.")
        filepath = os.path.join(self.path,
                                pool_name,
                                rbd_name,
                                self.BACKUP_INCREMENTAL_INFO_META,
                                circle_name)
        incremental_list = self.metafile.read(filepath=filepath)
        if isinstance(incremental_list, list):
            sorted_incremental_list = sort_dict_list(incremental_list,
                                                     self.LIST_COUNTER_KEY,
                                                     reverse=False)
            if sorted_incremental_list == False:
                self.log.debug("Metadata - Unable to sort backup incremental list.")
            else:
                return sorted_incremental_list
        self.log.debug("Metadata - Invalid backup incremental list.")
        return []

    def del_backup_circle_info(self, pool_name, rbd_name, delete_circle_info, key=None):
        try:
            self.log.debug("Metadata - Delete a backup circle.")
            if key == None:
                d_key = self.LIST_COUNTER_KEY
            else:
                d_key = key

            circle_list = self.get_backup_circle_list(pool_name, rbd_name)
            new_circle_list = []
            for circle_info in circle_list:
                if delete_circle_info[d_key] != circle_info[d_key]:
                    new_circle_list.append(circle_info)
            return self.set_backup_circle_list(pool_name, rbd_name, new_circle_list)
        except Exception as e:
            self.log.error("Metadata - Unable to delete a backup circle. %s" % e)
            return False

    def del_backup_snapshot_info(self, pool_name, rbd_name, delete_snapshot_info, key=None):
        try:
            self.log.debug("Metadata - Delete a backup snapshot.")
            if key == None:
                d_key = self.LIST_COUNTER_KEY
            else:
                d_key = key

            snapshot_list = self.get_backup_snapshot_list(pool_name, rbd_name)
            new_snapshot_list = []
            for snapshot_info in snapshot_list:
                if delete_snapshot_info[d_key] != snapshot_info[d_key]:
                    new_snapshot_list.append(snapshot_info)
            return self.set_backup_snapshot_list(pool_name, rbd_name, new_snapshot_list)
        except Exception as e:
            self.log.error("Metadata - Unable to delete a backup snapshot. %s" % e)
            return False

    def del_backup_incremental_info(self, pool_name, rbd_name, circle_name,
                                          incr_info=None, key=None):
        try:
            self.log.debug("Metadata - Delete a backup incremental.")
            filepath = os.path.join(self.path,
                                    pool_name,
                                    rbd_name,
                                    self.BACKUP_INCREMENTAL_INFO_META,
                                    circle_name)

            if incr_info == None:
                self.log.debug("Metadata - Delete backup incremental file '%s'." % filepath)
                self.directory.delete(filepath)
                return True

            incremental_list = self.metafile.read(filepath=filepath)
            new_incremental_list = []
            if isinstance(incremental_list, list):
                for incremental_name in incremental_list:
                    if incremental_name != incr_info:
                        new_incremental_list.append(incremental_name)
            return self.set_backup_incremental_list(pool_name,
                                                    rbd_name,
                                                    circle_name,
                                                    new_incremental_list)
        except Exception as e:
            self.log.error("Metadata - Unable to delete a backup incremental. %s" % e)
            return False

    def del_backup_info(self, backup_name):
        try:
            self.log.debug("Metadata - Delete a backup info.")
            filepath = os.path.join(self.path, self.BACKUP_EXPORT_INFO_META, backup_name)
            self.directory.delete(filepath)
            return True
        except Exception as e:
            self.log.error("Metadata - Unable to delete a backup info. %s" % e)
            return False

    def del_rbd_info(self, pool_name, rbd_name):
        try:
            self.log.debug("Metadata - Delete a rbd meta.")
            filepath = os.path.join(self.path, pool_name, rbd_name)
            self.directory.delete(filepath)
            return True
        except Exception as e:
            self.log.error("Metadata - Unable to delete a rbd meta. %s" % e)
            return False

    def add_backup_circle_info(self, pool_name, rbd_name, new_circle_info):
        try:
            self.log.debug("Metadata - Add new backup circle.")
            circle_list = self.get_backup_circle_list(pool_name, rbd_name)
            add_circle_info = new_circle_info
            if len(circle_list) == 0:
                add_circle_info[self.LIST_COUNTER_KEY] = 1
            else:
                last_circle_info = circle_list[-1]
                add_circle_info[self.LIST_COUNTER_KEY] = int(last_circle_info[self.LIST_COUNTER_KEY]) + 1
            circle_list.append(add_circle_info)
            return self.set_backup_circle_list(pool_name, rbd_name, circle_list)
        except Exception as e:
            self.log.error("Metadata - Unable to add new backup circle. %s" % e)
            return False

    def add_backup_info(self, backup_name, meta):
        try:
            self.log.debug("Metadata - Add new backup export.")
            self.set_backup_export_info(backup_name, meta)

            rotation_length = self.MAX_BACKUP_INFO_RETAIN_COUNT

            backup_name_list = self.get_backup_name_list()
            diff_count = len(backup_name_list) - int(rotation_length)
            if diff_count > 0:
                sorted_export_list = sorted(backup_name_list)
                for i in range(0, diff_count):
                    self.del_backup_info(backup_name_list[i])
        except Exception as e:
            self.log.error("Metadata - Unable to add new backup export info. %s" % e)
            return False

    def add_backup_snapshot_info(self, pool_name, rbd_name, new_snapshot_info):
        try:
            self.log.debug("Metadata - Add new backup snapshot.")
            snapshot_list = self.get_backup_snapshot_list(pool_name, rbd_name)
            add_snapshot_info = new_snapshot_info
            if len(snapshot_list) == 0:
                add_snapshot_info[self.LIST_COUNTER_KEY] = 1
            else:
                last_snapshot_info = snapshot_list[-1]
                new_counter = int(last_snapshot_info[self.LIST_COUNTER_KEY]) + 1
                add_snapshot_info[self.LIST_COUNTER_KEY] = new_counter
            snapshot_list.append(add_snapshot_info)
            return self.set_backup_snapshot_list(pool_name, rbd_name, snapshot_list)
        except Exception as e:
            self.log.error("Metadata - Unable to add new backup snapshot. %s" % e)
            return False

    def add_backup_incremental_info(self, pool_name, rbd_name, circle_name, new_incr_info):
        try:
            self.log.debug("Metadata - Add new backup incremental.")
            incremental_list = self.get_backup_incremental_list(pool_name, rbd_name, circle_name)
            add_incremental_info = new_incr_info
            if len(incremental_list) == 0:
                add_incremental_info[self.LIST_COUNTER_KEY] = 1
            else:
                last_incremental_info = incremental_list[-1]
                new_counter = int(last_incremental_info[self.LIST_COUNTER_KEY]) + 1
                add_incremental_info[self.LIST_COUNTER_KEY] = new_counter
            incremental_list.append(add_incremental_info)
            return self.set_backup_incremental_list(pool_name, rbd_name, circle_name, incremental_list)
        except Exception as e:
            self.log.error("Metadata - Unable to add new backup incremental. %s" % e)
            return False

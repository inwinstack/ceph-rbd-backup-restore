#!/usr/bin/env python
# coding=UTF-8

import yaml

from backup.rbd_backup_metadata import RBD_Backup_Metadata


class RBD_Restore_List(object):

    def __init__(self, log):
        self.log = log
        self.rbd_list_data = []
        self.backup_meta = None
        self.backup_yaml = None
        self.cluster_info = None

    def read_meta(self, backup_dirpath):
        try:
            self.log.debug("Read metadata in '%s'." % backup_dirpath)
            self.backup_dirpath = backup_dirpath
            self.backup_meta = RBD_Backup_Metadata(self.log, backup_dirpath)
            cluster_info = self.backup_meta.get_cluster_info()
            if all (k in cluster_info for k in ('name', 'fsid', 'conf', 'keyr')):
                self.cluster_info = cluster_info
                return True
            self.log.error("Metadata is not valid.")
            return False
        except Exception as e:
            self.log.error('Unable to read metadata. %s' % e)
            return False

    # unused
    def read_yaml(self, yaml_filepath):
        try:
            self.log.debug("Read Yaml file '%s'." % yaml_filepath)
            if os.path.isfile(use_filepath):
                yaml_file = open(use_filepath, 'r')
                yaml_data = yaml.load(yaml_file, Loader=yaml.CLoader)
                yaml_file.close()
                if yaml_data.has_key('cluster_info'):
                    cluster_info = yaml_data['cluster_info']
                    if all (k in cluster_info for k in ('name', 'fsid', 'conf', 'keyr')):
                        self.log.debug("Read yaml file:", yaml_data)
                        self.cluster_info = cluster_info
                        self.backup_yaml = yaml_data
                        return True
                else:
                    self.log.error("Yaml file is not valid.")
            else:
                self.log.error("Yaml file is not found.")

            return False
        except Exception as e:
            self.log.error('Unable to read YAML file. %s' % e)
            return False

    def get_cluster_info(self):
        return self.cluster_info

    def get_full_backup_time_list(self, pool_name, rbd_name):
        try:
            return self.backup_meta.get_backup_circle_list(pool_name, rbd_name)
        except Exception as e:
            self.log.error('Unable to get full backup time list. %s' % e)
            return False

    def get_rbd_backup_time_list(self, pool_name, rbd_name):
        backup_circle_list = self.backup_meta.get_backup_circle_list(pool_name, rbd_name)
        backup_time_list = []
        for circle_info in backup_circle_list:
            circle_name = circle_info['name']
            backup_time_list.append(circle_info['datetime'])
            backup_incr_list = self.backup_meta.get_backup_incremental_list(pool_name,
                                                                            rbd_name,
                                                                            circle_name)
            for incr_info in backup_incr_list:
                backup_time_list.append(incr_info['datetime'])
        return backup_time_list

    def get_rbd_backup_info_list(self, pool_name, rbd_name):
        backup_circle_list = self.backup_meta.get_backup_circle_list(pool_name, rbd_name)
        backup_info_list = []
        for circle_info in backup_circle_list:
            circle_name = circle_info['name']
            circle_datetime = circle_info['datetime']
            backup_name = circle_info['backup_name']
            backup_info_list.append((circle_name, circle_datetime, backup_name, circle_name))
            backup_incr_list = self.backup_meta.get_backup_incremental_list(pool_name,
                                                                            rbd_name,
                                                                            circle_name)
            for incr_info in backup_incr_list:
                incr_name = incr_info['name']
                incr_datetime = incr_info['datetime']
                backup_name = incr_info['backup_name']
                backup_info_list.append((incr_name, incr_datetime, backup_name, circle_name))

        # (backup filename, backup datetime, backup name, backup circle name)
        return backup_info_list

    def get_backup_circle_info(self, pool_name, rbd_name, restore_datetime):
        circle_list = self.backup_meta.get_backup_circle_list(pool_name, rbd_name)
        for circle_info in circle_list:
            if circle_info['name'] == restore_datetime:
                return circle_info
        for circle_info in circle_list:
            circle_name = circle_info['name']
            incr_list = self.backup_meta.get_backup_incremental_list(pool_name,
                                                                     rbd_name,
                                                                     circle_name)
            for incr_info in incr_list:
                if incr_info['datetime'] == restore_datetime:
                    return circle_info
        self.log.error('Unable to get backup circle info. %s' % e)
        return False

    def get_rbd_incr_list(self, pool_name, rbd_name, circle_name):
        return self.backup_meta.get_backup_incremental_list(pool_name,
                                                            rbd_name,
                                                            circle_name)

    def get_latest_backup_time(self, pool_name, rbd_name):
        circle_list = self.backup_meta.get_backup_circle_list(pool_name, rbd_name)
        circle_name = circle_list[-1]
        incr_list = self.backup_meta.get_backup_incremental_list(pool_name, rbd_name, circle_name)
        return incr_list[-1]

    def get_earliest_time(self, pool_name, rbd_name):
        circle_list = self.backup_meta.get_backup_circle_list(pool_name, rbd_name)
        circle_name = circle_list[0]
        incr_list = self.backup_meta.get_backup_incremental_list(pool_name, rbd_name, circle_name)
        return incr_list[0]

    def get_backup_rbd_list(self, backup_name=None):
        if backup_name != None:
            return self.backup_meta.get_backup_rbd_list(backup_name)
        return self.backup_meta.get_backup_rbd_info_list()

    def get_backup_name_list(self):
        return self.backup_meta.get_backup_name_list()

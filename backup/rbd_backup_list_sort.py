#!/usr/bin/env python
# coding=UTF-8

import os

from lib.common_func import *


# todo: add other sorting method selection
# a. sort by last backup file size
# b. sort by rbd used size
# c. sort by last modified time of RBD. how?
# d. sort by priority configured in backup list yaml file
# e. sort by last recorded metafile info ... eg. elapsed time of backup or snapshot size
class RBD_Backup_List_Sort(object):

    def __init__(self, log):
        self.log = log

    def sort_by_rbd_size(self, rbd_list, key='rbd_size'):
        try:
            sorted_rbd_backup_list = rbd_list
            sorted_list = sort_dict_list(rbd_list, key, reverse=True)

            if sorted_list != False:
                self.log.debug("Sorted rbd backup list by rbd size.")
                sorted_rbd_backup_list = sorted_list
            else:
                self.log.warning("Unable to sort rbd backup list by rbd size.")

            self._log_priority(sorted_rbd_backup_list)

            return backup_rbd_info_list
        except Exception as e:
            self.log.error("error occur when sorting RBD backup list. %s" % e)
            return rbd_list

    def sort_by_snap_size(self, rbd_list):
        """ sort by size of last snapshot

        get size of last snapshot from snapshot list and sort by size
        """
        try:
            return rbd_list
        except Exception as e:
            self.log.error("error occur when sorting RBD backup list. %s" % e)
            return rbd_list

    def sort_by_rbd_used_size(self):
        return

    def sort_by_config_order(self):
        return

    def sort_by_last_backup_time(self):
        return

#!/usr/bin/env python
# coding=UTF-8

from lib.common_func import *


class RBD_Backup_Option(object):
    '''/
        Verify backup options and convert to relate type
    /'''
    def __init__(self, log):
        self.log = log
        self.backup_rbd_option = {}

    def _isdigit(self, value):
        if isinstance(value, str):
            return opt_str.isdigit()
        elif isinstance(value, int):
            return True
        return False

    def _is_list(self, value):
        opt_list = value.split(', ')
        if len(opt_list) != 0:
            return False
        return False

    def _is_bool_str(self, value):
        if value in ['True', 'False']:
            return True
        return False

    def _is_abs_path(self, value):
        if os.path.isabs(value):
            return True

    def _get_rbd_option(self, pool_name, rbd_name, option_name):
        rbd_id = convert_rbd_id(pool_name, rbd_name)
        if self.backup_rbd_option.has_key(rbd_id):
            rbd_option = self.backup_rbd_option[rbd_id]
            if rbd_option.has_key(option_name):
                return rbd_option[option_name]
        self.log.debug("No option is set for the RBD.")
        return False

    def add_option(self, pool_name, rbd_name, options):
        rbd_id = convert_rbd_id(pool_name, rbd_name)
        self.log.debug("Add RBD '%s' backup options" % rbd_id)
        self.backup_rbd_option[rbd_id] = options

    def get_backup_type(self, pool_name, rbd_name):
        option_value = self._get_rbd_option(pool_name, rbd_name, 'backup_type')
        if option_value != False:
            if option_value in ['full', 'incr']:
                return option_value
            self.log.error("Backup type is not either 'incr' or 'full'.")
        return False

    # unused
    def get_backup_path(self, pool_name, rbd_name):
        option_value = self._get_rbd_option(pool_name, rbd_name, 'backup_path')
        if option_value != False:
            if os.path.isabs(option_value):
                return option_value
            else:
                self.log.error("Backup path is not an absolute path.")
        return False

    def get_backup_circle_retain_count(self, pool_name, rbd_name):
        option_value = self._get_rbd_option(pool_name, rbd_name, 'max_backup_circle_count')
        if option_value != False:
            if self._isdigit(option_value):
                return option_value
            else:
                self.log.error("Circle retain count is not an integer value.")
        return False

    def get_backup_max_incr_count(self, pool_name, rbd_name):
        option_value = self._get_rbd_option(pool_name, rbd_name, 'max_incremental_backup_count')
        if option_value != False:
            if self._isdigit(option_value):
                return option_value
            else:
                self.log.error("Incremental backup count is not an integer value.")
        return False

    def get_snapshot_retain_count(self, pool_name, rbd_name):
        option_value = self._get_rbd_option(pool_name, rbd_name, 'backup_snapshot_retain_count')
        if option_value != False:
            if self._isdigit(option_value):
                return option_value
            else:
                self.log.error("Snapshot retain count is not an integer value.")
        return False

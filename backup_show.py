#!/usr/bin/env python
# coding=UTF-8

import sys
import os
import datetime
import traceback
from argparse import ArgumentParser

from lib.common_func import *
from lib.config import Config
from lib.logger import Logger
from lib.ceph import Ceph
from lib.directory import Directory

from backup.rbd_backup_const import RBD_Backup_Const as const
from backup.rbd_backup_list import RBD_Backup_List
from backup.rbd_backup_option import RBD_Backup_Option

from restore.rbd_restore_list import RBD_Restore_List


def main(argument_list):

    DEFAULT_CONFIG_PATH = const.CONFIG_PATH
    DEFAULT_CONFIG_SECTION = const.CONFIG_SECTION

    try:
        parser = ArgumentParser(add_help=False)

        parser.add_argument('--config-file')
        parser.add_argument('--config-section')
        parser.add_argument('--cluster-name')
        parser.add_argument('--backup-directory')
        parser.add_argument('--show-details', action='store_true')

        parser.add_argument('options', nargs='+')

        #args = vars(parser.parse_args(argument_list[1:]))
        known, unknown = parser.parse_known_args(argument_list[1:])
        args = vars(known)

        # get config file path and config section name
        restore_config_file = DEFAULT_CONFIG_PATH
        restore_config_section = DEFAULT_CONFIG_SECTION

        if args['config_file'] is not None:
            restore_config_file = args['config_file']

        if args['config_section'] is not None:
            restore_config_section = args['config_section']

        # create config obj and read config file data
        cfg = Config(restore_config_file, restore_config_section)
        if not cfg.is_valid():
            print("Error, fail to initialize config.")
            sys.exit(2)
        if not cfg.set_options(print_options=False):
            print("Error, fail to set config.")
            sys.exit(2)

        # get backup directory path and cluster name
        backup_directory = cfg.backup_destination_path
        cluster_name = cfg.ceph_cluster_name
        if args['backup_directory'] is not None:
            backup_directory = args['backup_directory']
        if args['cluster_name'] is not None:
            cluster_name = args['cluster_name']

        show_details = False
        if args['show_details']:
            show_details = True

        opts = args['options']

        # initial backup logging
        log = Logger(cfg.log_file_path,
                     cfg.log_level,
                     cfg.log_max_bytes,
                     cfg.log_backup_count,
                     cfg.log_delay,
                     name=const.LOG_SHOW_LOGGER_NAME)
        if not log.set_log(log_module=cfg.log_module_name):
            print("Error, unable to set logger.")
            sys.exit(2)

        if opts[0] != 'config':
            rbd_restore_list = RBD_Restore_List(log)
            backup_cluster_directory = os.path.join(backup_directory, cluster_name)
            if not rbd_restore_list.read_meta(backup_cluster_directory):
                print("Error, unable to get cluster info.")
                sys.exit(2)

        print("")
        if opts[0] == 'backup':

            if len(opts) == 1:
                print("*Show all backup name.\n")
                backup_name_list = rbd_restore_list.get_backup_name_list()
                if show_details:
                    print("[Backup Name]       [RBD Count]")
                    print("-"*31)
                else:
                    print("[Backup Name List]")
                    print("-"*20)

                for backup_name in backup_name_list:
                    if show_details:
                        rbd_list = rbd_restore_list.get_backup_rbd_list(backup_name)
                        print("%s %s" % (backup_name, len(rbd_list)))
                    else:
                        print("%s" % backup_name)

            elif len(opts) >= 2:
                backup_name = normalize_datetime(' '.join(opts[1:]))
                rbd_list = rbd_restore_list.get_backup_rbd_list(backup_name)

                print("*Show RBD list in backup name '%s'.\n" % backup_name)
                if show_details:
                    print("[Backup Time]       [Circle name]       " \
                          "[Pool name/RBD name]  ... [Status]")
                    print("-"*74)
                else:
                    print("[Backup RBD List]")
                    print("-"*20)

                for rbd_info in rbd_list:
                    if show_details:
                        if rbd_info[0] == 0:
                            status = 'OK'
                        else:
                            status = 'FAIL'
                        print("%s %s %s/%s ... %s" % (rbd_info[3],
                                                      rbd_info[4],
                                                      rbd_info[1],
                                                      rbd_info[2],
                                                      status))
                    else:
                        print("%s/%s" % (rbd_info[1], rbd_info[2]))

        elif opts[0] == 'rbd':
            if len(opts) == 1:
                print("*Show all backuped RBD name.\n")
                rbd_list = rbd_restore_list.get_backup_rbd_list()
                if len(rbd_list) == 0:
                    return 0

                arranged_rbd_info = {}

                for rbd_info in rbd_list:
                    pool_name = rbd_info[0]
                    rbd_name = rbd_info[1]
                    info = rbd_info[2]

                    if not arranged_rbd_info.has_key(pool_name):
                        rbd_info_list = []
                    else:
                        rbd_info_list = arranged_rbd_info[pool_name]
                    info['name'] = rbd_name
                    rbd_info_list.append(info)
                    arranged_rbd_info[pool_name] = rbd_info_list

                if show_details:
                    print("[Pool name]")
                    print("  [RBD name] [block name prefix] [Num objects] [size (bytes)]")
                    print("-"*74)
                else:
                    print("[Pool name]")
                    print("  [RBD name]")
                    print("-"*20)
                for pool_name, rbd_list in arranged_rbd_info.iteritems():
                    print("%s" % pool_name)
                    if show_details:
                        for rbd_info in rbd_list:
                            rbd_name = rbd_info['name']
                            rbd_size = rbd_info['size']
                            rbd_objs = rbd_info['num_objs']
                            rbd_prix = rbd_info['block_name_prefix']
                            print("  %s %s %s %s" % (rbd_name,
                                                     rbd_prix,
                                                     rbd_objs,
                                                     rbd_size))
                    else:
                        for rbd_info in rbd_list:
                            print("  %s" % rbd_info['name'])

            if len(opts) == 3:
                pool_name = opts[1]
                rbd_name = opts[2]

                print("*Show backup time of RBD '%s/%s'\n." % (pool_name, rbd_name))

                if show_details:
                    directory = Directory(log)
                    backup_info_list = rbd_restore_list.get_rbd_backup_info_list(pool_name, rbd_name)
                    if len(backup_info_list) == 0:
                        return 0
                    print("[Backup time]       [Backup name]       " \
                          "[Backup circle]     [Backup size]")
                    print("-"*74)
                    for backup_info in backup_info_list:
                        backup_file = backup_info[0]
                        backup_time = backup_info[1]
                        backup_name = backup_info[2]
                        backup_circ = backup_info[3]
                        backup_size = directory.get_used_size(backup_cluster_directory,
                                                              pool_name,
                                                              rbd_name,
                                                              backup_circ,
                                                              backup_file)
                        print("%s %s %s %s" % (backup_time,
                                               backup_name,
                                               backup_circ,
                                               backup_size))
                else:
                    backup_time_list = rbd_restore_list.get_rbd_backup_time_list(pool_name, rbd_name)
                    if len(backup_time_list) == 0:
                        return 0

                    print("[Backup time]")
                    print("-"*20)
                    for backup_time in backup_time_list:
                        print("%s" % backup_time)

        elif opts[0] == 'cluster':
            print("*Show backuped cluster info.\n")
            cluster_info = rbd_restore_list.get_cluster_info()
            for key, value in cluster_info.iteritems():
                print("%s: %s" % (key, value))

        elif opts[0] == 'config':
            if len(opts) == 1:
                print("*Show backup config.\n")

                cfg_opts = cfg.get_option()
                for key, value in cfg_opts.iteritems():
                    print("%s = %s" % (key, value))

            elif len(opts) == 2:
                if opts[1] == 'openstack':
                    print("*Show openstack yaml. (constructing)\n")

            elif len(opts) == 3:
                if opts[1] == 'rbd' and opts[2] == 'list':
                    print("*Show RBD backup list.")
                    print("*Yaml file: %s" % cfg.backup_list_file_path)
                    print("*Cluster name: %s" % cluster_name)
                    print("")

                    rbd_backup_list = RBD_Backup_List(log)
                    rbd_backup_list.read_yaml(cfg.backup_list_file_path)
                    rbd_name_list = rbd_backup_list.get_rbd_name_list(cluster_name)

                    if show_details:
                        print("[Pool name]")
                        print("  [RBD name] [backup_type] [max_incr] [max_circ] [max_snap]")
                        print("-"*74)
                        backup_option = RBD_Backup_Option(log)
                        for pool_name, rbd_name_list in rbd_name_list.iteritems():
                            print("%s" % pool_name)
                            for rbd_name in rbd_name_list:
                                options = rbd_backup_list.get_rbd_options(cluster_name,
                                                                          pool_name,
                                                                          rbd_name)
                                backup_option.add_option(pool_name,
                                                         rbd_name,
                                                         options)
                                backup_type = backup_option.get_backup_type(pool_name,
                                                                            rbd_name)
                                max_incr = backup_option.get_backup_max_incr_count(pool_name,
                                                                                   rbd_name)
                                max_circ = backup_option.get_backup_circle_retain_count(pool_name,
                                                                                        rbd_name)
                                max_snap = backup_option.get_snapshot_retain_count(pool_name, rbd_name)
                                print("  %s %s %s %s %s" % (rbd_name,
                                                            backup_type,
                                                            max_incr,
                                                            max_circ,
                                                            max_snap))
                    else:
                        print("[Pool name]")
                        print("  [RBD name]")
                        print("-"*24)
                        for pool_name, rbd_name_list in rbd_name_list.iteritems():
                            print("%s" % pool_name)
                            for rbd_name in rbd_name_list:
                                print("  %s" % rbd_name)

        print("")
    except Exception as e:
        print("Error, %s" % e)
        sys.exit(2)


if "__main__" == __name__:
    return_code = main(sys.argv)
    sys.exit(return_code)

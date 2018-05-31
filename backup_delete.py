#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Yu-Jung Cheng

import sys
import os
import datetime
import traceback
from argparse import ArgumentParser

from lib.directory import Directory
from lib.config import Config
from lib.logger import Logger

from backup.rbd_backup_const import RBD_Backup_Const as const
from backup.rbd_backup_metadata import RBD_Backup_Metadata


def main(argument_list):

    DEFAULT_CONFIG_PATH = const.CONFIG_PATH
    DEFAULT_CONFIG_SECTION = const.CONFIG_SECTION

    try:
        parser = ArgumentParser(add_help=False)

        parser.add_argument('--config-file')
        parser.add_argument('--config-section')
        parser.add_argument('--cluster-name')
        parser.add_argument('--backup-directory')
        parser.add_argument('--yes', action='store_true')
        parser.add_argument('options', nargs='+')
        args = vars(parser.parse_args(argument_list[1:]))

        backup_config_file = DEFAULT_CONFIG_PATH
        backup_config_section = DEFAULT_CONFIG_SECTION
        if args['config_file'] is not None:
            backup_config_file = args['config_file']
        if args['config_section'] is not None:
            backup_config_section = args['config_section']

        # create config obj and read config file data
        cfg = Config(backup_config_file, backup_config_section)
        if not cfg.is_valid():
            print("Error, fail to initialize config.")
            sys.exit(2)
        if not cfg.set_options(print_options=False):
            print("Error, fail to set config.")
            sys.exit(2)

        backup_directory = cfg.backup_destination_path
        cluster_name = cfg.ceph_cluster_name
        if args['backup_directory'] is not None:
            backup_directory = args['backup_directory']
        if args['cluster_name'] is not None:
            cluster_name = args['cluster_name']

        ask_yes_no = True
        if args['yes']:
            ask_yes_no = False

        opts = args['options']

        # initial backup logging
        log = Logger(cfg.log_file_path,
                     cfg.log_level,
                     cfg.log_max_bytes,
                     cfg.log_backup_count,
                     cfg.log_delay,
                     name=const.LOG_DELETE_LOGGER_NAME)
        if not log.set_log(log_module=cfg.log_module_name):
            print("Error, unable to set logger.")
            sys.exit(2)

        backup_cluster_directory = os.path.join(backup_directory, cluster_name)
        backup_meta = RBD_Backup_Metadata(log, backup_cluster_directory)
        directory = Directory(log)

        if opts[0] == 'rbd':
            if len(opts) == 3:
                pool_name = opts[1]
                rbd_name = opts[2]

                log.info("Delete backuped RBD.",
                         "pool name: %s" % pool_name,
                         "rbd name: %s" % rbd_name)

                print("- delete backuped RBD:")
                print("  pool name: %s" % pool_name)
                print("  rbd name: %s" % rbd_name)

                if ask_yes_no:
                    response = raw_input("Are you sure to delete this RBD backup "\
                                         "(yes or no)? : ")
                    response = response.lower()
                    if response != 'yes':
                        sys.exit(0)

                dir_ret = directory.delete(backup_cluster_directory, pool_name, rbd_name)
                meta_ret = backup_meta.del_rbd_info(pool_name, rbd_name)

                if dir_ret != False and meta_ret != False:
                    log.info("The Backuped RBD is deleted successfully.")
                    print("- the backuped RBD is deleted successfully.")
                else:
                    print("Error, error occur while deleting backuped RBD.")

    except KeyboardInterrupt as ke:
        print("- Operation canceled.")
        sys.exit(2)

    except Exception as e:

        exc_type,exc_value,exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)

        sys.exit(2)

if "__main__" == __name__:
    return_code = main(sys.argv)
    sys.exit(return_code)

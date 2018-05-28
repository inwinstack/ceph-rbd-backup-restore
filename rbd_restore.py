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
from lib.manager import Manager

from backup.rbd_backup_const import RBD_Backup_Const as const

from restore.rbd_restore_list import RBD_Restore_List
from restore.rbd_restore_task_maker import RBD_Restore_Task_Maker


def main(argument_list):

    DEFAULT_CONFIG_PATH = const.CONFIG_PATH
    DEFAULT_CONFIG_SECTION = const.CONFIG_SECTION

    backup_directory = None
    cluster_name = None
    dest_pool_name = None
    dest_rbd_name = None

    src_pool_name = None
    src_rbd_name = None
    restore_datetime = None

    force_import = False
    rbd_exist = False

    try:
        # parse arguments
        parser = ArgumentParser(add_help=False)

        parser.add_argument('--config-file')
        parser.add_argument('--config-section')
        parser.add_argument('--cluster-name')
        parser.add_argument('--backup-directory')
        parser.add_argument('--dest-pool-name')
        parser.add_argument('--dest-rbd-name')
        parser.add_argument('--force-restore')
        parser.add_argument('src_pool_name')
        parser.add_argument('src_rbd_name')
        parser.add_argument('restore_datetime', nargs='+')
        args = vars(parser.parse_args(argument_list[1:]))

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

        if args['src_pool_name'] is None:
            print("Error, missing pool name.")
            sys.exit(2)
        if args['src_rbd_name'] is None:
            print("Error, missing rbd name.")
            sys.exit(2)
        if args['restore_datetime'] is None:
            print("Error, missing datetime to restore.")
            sys.exit(2)

        src_pool_name = args['src_pool_name']
        src_rbd_name = args['src_rbd_name']

        dest_pool_name = args['src_pool_name']
        dest_rbd_name = args['src_rbd_name']
        if args['dest_pool_name'] is not None:
            dest_pool_name = args['dest_pool_name']
        if args['dest_rbd_name'] is not None:
            dest_rbd_name = args['dest_rbd_name']
        if args['force_restore'] == 'True':
            force_import = True

        restore_datetime = normalize_datetime(' '.join(args['restore_datetime']))

        # initial backup logging
        log = Logger(cfg.log_file_path,
                     cfg.log_level,
                     cfg.log_max_bytes,
                     cfg.log_backup_count,
                     cfg.log_delay,
                     name='restore')
        if not log.set_log(log_module=cfg.log_module_name):
            print("Error, unable to set logger.")
            sys.exit(2)

        # start RBD restore
        begin_restore_timestamp = int(time.time())
        log.blank(line_count=4)
        log.info("******** Start Ceph RBD restore ********",
                 "pid = %s" % os.getpid(),
                 "config file = %s" % restore_config_file,
                 "section = %s" % restore_config_section)

        print("- from backup directory: %s" % backup_directory)
        print("- from cluster name: %s" % cluster_name)
        print("- restore to datetime: %s" % restore_datetime)
        print("- source pool name: %s" % src_pool_name)
        print("- source RBD name: %s" % src_rbd_name)
        print("- destionation pool name: %s" % dest_pool_name)
        print("- destionation RBD name: %s" % dest_rbd_name)

        # get backup circle info form backup directory.
        rbd_restore_list = RBD_Restore_List(log)
        backup_cluster_directory = os.path.join(backup_directory, cluster_name)

        if not rbd_restore_list.read_meta(backup_cluster_directory):
            print("Error, unable to get cluster info.")
            sys.exit(2)

        cluster_info = rbd_restore_list.get_cluster_info()
        if cluster_info['name'] != cluster_name:
            log.error("Cluster name is not matched.",
                      "Cluster name of the backup directory: %s" % cluster_info['name'])
            print("Error, cluster name is not matched.")
            sys.exit(2)

        circle_info = rbd_restore_list.get_backup_circle_info(src_pool_name,
                                                              src_rbd_name,
                                                              restore_datetime)
        if circle_info == False:
            print("Error, unable to find belonging backup circle.")
            sys.exit(2)

        circle_datetime = circle_info['datetime']
        circle_name = circle_info['name']   # filename of backuped RBD image
        circle_path = circle_info['path']   # filepath of backuped RBD image
        print("- belonging backup circle name: %s" % circle_name)
        log.info("Found backup circle name %s." % circle_name,
                 "circle directory name: %s" % circle_path,
                 "circle datetime: %s" % circle_datetime)

        # Verify the cluster and the RBD.
        # -----------------------------------------------------
        cluster_conffile = os.path.join(backup_cluster_directory,
                                        cluster_info['conf'])
        cluster_keyring = os.path.join(backup_cluster_directory,
                                       cluster_info['keyr'])

        log.info("Verifying Ceph cluster.",
                 "config file = %s" % cluster_conffile,
                 "keyring file = %s" % cluster_keyring)

        ceph = Ceph(log, cluster_name, conffile=cluster_conffile)
        if not ceph.connect_cluster():
            log.error("Unable to connect ceph cluster.")
            # you may check user or permission to /etc/ceph directory
            print("Error, unable to connect ceph cluster.")
            sys.exit(2)

        ceph_pool_name_list = ceph.get_pool_list()
        if ceph_pool_name_list == False:
            log.error("Unable to get pool name list from ceph cluster.")
            print("Error, unable to get pool name list from ceph cluster.")
            sys.exit(2)

        # todo: create pool if not exit
        #   need pool parameters (pg count etc...), read from backup meta?
        if not dest_pool_name in ceph_pool_name_list:
            log.error("Pool name '%s' is not exist in ceph cluster.")
            print("Error, pool '%s' is not exist in ceph cluster.")
            sys.exit(2)

        if not ceph.open_ioctx(dest_pool_name):
            log.error("Unable to open ioctx of pool '%s'." % pool_name)
            print("Error,unable to open ioctx of pool '%s'." % pool_name)
            sys.exit(2)

        ceph_rbd_list = ceph.get_rbd_list()
        if ceph_rbd_list == False:
            log.error("Unable to get RBD name list from ceph cluster.")
            print("Error, Unable to get RBD name list from ceph cluster.")
            sys.exit(2)

        if dest_rbd_name in ceph_rbd_list:
            if not force_import:
                log.error("Error, the destionation RBD name '%s' is exist in ceph cluster." % dest_rbd_name)
                print("Error, the destionation RBD name '%s' is exist in ceph cluster." % dest_rbd_name)
                sys.exit(2)
            rbd_exist = True
        # -----------------------------------------------------

        log.info("Initial worker manager and task maker.")

        # initial task worker and maker
        manager = Manager(log, worker_count=1, rest_time=3)
        manager.set_worker_logger(cfg.log_file_path)
        manager.set_monitor_logger(cfg.log_file_path)
        manager.run_worker()

        task_maker = RBD_Restore_Task_Maker(log, cluster_name,
                                                 cluster_conffile,
                                                 cluster_keyring)

        # Delete exist RBD images if forced to do so
        # -----------------------------------------------------
        if rbd_exist and force_import:
            log.info("Delete exist RBD image.")
            print("- delete exist RBD image.")

            snap_purge_task = task_maker.get_rbd_snapshot_purge_task(dest_pool_name,
                                                                     dest_rbd_name)
            if not manager.add_task(snap_purge_task):
                print("Error, unable to submit snapshot purge task.")
            print("  purging RBD snapshots.")

            finished_task = manager.get_finished_task()
            result = finished_task.get_result()
            log.info("Task Result: ", result)
            if result['return_code'] != 0:
                log.error("purge RBD snapshots fail.")
                print("  purge RBD snapshots fail.")
                manager.stop()
                sys.exit(2)
            else:
                print("  purge RBD snapshots successfully.")

            rbd_delete_task = task_maker.get_rbd_delete_task(dest_pool_name,
                                                             dest_rbd_name)
            if not manager.add_task(rbd_delete_task):
                print("Error, unable to submit RBD delete task.")
            print("  deleting RBD image.")

            finished_task = manager.get_finished_task()
            result = finished_task.get_result()
            log.info("Task Result: ", result)
            if result['return_code'] != 0:
                log.error("Delete RBD image fail.")
                print("  delete RBD image fail.")
                manager.stop()
                sys.exit(2)
            else:
                print("  purge RBD snapshots successfully.")

        print("- start RBD restoring to %s" % restore_datetime)

        # Full RBD import to the cluster
        # -----------------------------------------------------
        src_path = os.path.join(backup_cluster_directory,
                                src_pool_name,
                                src_rbd_name,
                                circle_path)
        src_file = circle_name
        full_import_task = task_maker.get_rbd_import_full_task(dest_pool_name,
                                                               dest_rbd_name,
                                                               src_path,
                                                               src_file)
        if not manager.add_task(full_import_task):
            print("Error, unable to submit full import task.")
            sys.exit(2)

        log.info("Start full import RBD task %s." % full_import_task)
        print("  restoring to %s. (full)" % circle_datetime)
        finished_task = manager.get_finished_task()

        result = finished_task.get_result()
        log.info("Task Result: ", result)
        if result['return_code'] != 0:
            log.error("Restore to %s fail." % circle_name)
            print("  restore to %s fail." % circle_name)
            manager.stop()
            sys.exit(2)

        print("  restore to %s successfully." % circle_name)
        log.info("%s is completed." % finished_task)

        # Incremental RBD import to the cluster
        # -----------------------------------------------------
        circle_datetime = normalize_datetime(circle_datetime)
        if circle_datetime != restore_datetime:
            log.info("Start incremental import RBD task %s." % full_import_task)

            # create snapshot with name of from snapshot in incremental backup
            snap_create_task = task_maker.get_rbd_snapshot_create_task(dest_pool_name,
                                                                       dest_rbd_name,
                                                                       circle_name)
            if not manager.add_task(snap_create_task):
                print("Error, unable to submit snapshot create task.")
                sys.exit(2)
            finished_task = manager.get_finished_task()

            result = finished_task.get_result()
            log.info("Task Result: ", result)
            if result['return_code'] != 0:
                print("  create snapshot name '%s' fail." % circle_name)
                manager.stop()
                sys.exit(2)

            current_snap_name = circle_name  # set current snapshot name
            incr_list = rbd_restore_list.get_rbd_incr_list(src_pool_name,
                                                           src_rbd_name,
                                                           circle_name)
            for incr_info in incr_list:
                incr_datetime = incr_info['datetime']
                incr_from = incr_info['from']
                incr_to = incr_info['to']
                src_file = incr_info['name']

                print("  restoring to %s. (incr)" % incr_datetime)

                if current_snap_name != incr_from:
                    print("Error, chain of incremental backup is broken.")
                    manager.stop()
                    sys.exit(2)

                # do import diff from the incremental backup
                incr_import_task = task_maker.get_rbd_import_diff_task(dest_pool_name,
                                                                       dest_rbd_name,
                                                                       src_path,
                                                                       src_file)
                if not manager.add_task(incr_import_task):
                    print("Error, unable to submit incremental import task.")
                    sys.exit(2)

                log.info("Start incremental import RBD task %s." % incr_import_task)

                finished_task = manager.get_finished_task()

                result = finished_task.get_result()
                log.info("Task Result: ", result)
                if result['return_code'] != 0:
                    log.error("Restore to %s fail." % incr_datetime)
                    print("  restore to %s fail." % incr_datetime)
                    manager.stop()
                    sys.exit(2)

                current_snap_name = incr_to
                print("  restore to %s successfully." % incr_datetime)
                log.info("%s is completed." % finished_task)

                incr_datetime = normalize_datetime(incr_datetime)
                if incr_datetime == restore_datetime:
                    log.info("- restore to %s successfully." % restore_datetime)
                    break

        # do snapshot purge.
        print("- purge RBD snapshots")
        snap_purge_task = task_maker.get_rbd_snapshot_purge_task(dest_pool_name,
                                                                 dest_rbd_name)
        if not manager.add_task(snap_purge_task):
            print("Error, unable to submit snapshot purge task.")

        finished_task = manager.get_finished_task()
        result = finished_task.get_result()
        log.info("Task Result: ", result)
        if result['return_code'] != 0:
            log.error("Purge RBD snapshots fail.")
            print("  purge RBD snapshots fail.")
        else:
            print("  purge RBD snapshots successfully.")

        # Completed
        manager.stop()
        log.info("******** Ceph RBD restore complete ********",
                 "use %s seconds " % get_elapsed_time(begin_restore_timestamp))

    except Exception as e:

        exc_type,exc_value,exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)

        if manager != None:
            manager.stop()

        sys.exit(2)


if "__main__" == __name__:
    print("\n%s - Start RBD Restore.\n" % get_datetime())
    return_code = main(sys.argv)
    print("\n%s - Finish RBD Restore.\n" % get_datetime())

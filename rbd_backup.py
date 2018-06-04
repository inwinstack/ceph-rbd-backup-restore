#!/usr/bin/env python
# coding=UTF-8
# Author: Yu-Jung Cheng

import sys
import os
import atexit
import datetime
import traceback
import multiprocessing

from argparse import ArgumentParser

from lib.common_func import *
from lib.config import Config
from lib.logger import Logger
from lib.ceph import Ceph
from lib.manager import Manager
from lib.directory import Directory

from task.snapshot_create import SnapshotCreateTask
from task.snapshot_delete import SnapshotDeleteTask
from task.snapshot_purge import SnapshotPurgeTask
from task.export_full import ExportFullTask
from task.export_diff import ExportDiffTask

from backup.rbd_backup_const import RBD_Backup_Const as const
from backup.rbd_backup_metadata import RBD_Backup_Metadata
from backup.rbd_backup_list import RBD_Backup_List
from backup.rbd_backup_list_sort import RBD_Backup_List_Sort
from backup.rbd_backup_option import RBD_Backup_Option
from backup.rbd_backup_task_maker import RBD_Backup_Task_Maker
from backup.rbd_backup_result import RBD_Backup_Task_Result


def main(argument_list):

    #const.EXPORT_TYPE = const.EXPORT_TYPE

    backup_name = None

    # total size of RBD image to backup (provisioned size)
    total_rbd_size = 0

    # available and used size of backup directory
    backup_dir_avai_bytes = 0
    backup_dir_used_bytes = 0

    # store RBD info list (list of dict)
    backup_rbd_info_list   = []

    # task counter
    submitted_snap_create_task_count = 0
    submitted_snap_delete_task_count = 0
    submitted_rbd_export_task_count = 0
    backup_circle_delete_count = 0

    is_rbd_list_from_command_line = False

    manager = None
    ceph = None

    try:
        # parse arguments
        parser = ArgumentParser(add_help=False)
        parser.add_argument('--config-file')
        parser.add_argument('--config-section')
        parser.add_argument('--backup-name')
        parser.add_argument('rbd_list', nargs='*')
        args = vars(parser.parse_args(argument_list[1:]))

        backup_config_file = const.CONFIG_PATH
        backup_config_section = const.CONFIG_SECTION
        if args['config_file'] is not None:
            backup_config_file = args['config_file']
        if args['config_section'] is not None:
            backup_config_section = args['config_section']
        if args['backup_name'] is not None:
            backup_name = args['backup_name']
        if len(args['rbd_list']) != 0:
            is_rbd_list_from_command_line = True

        # create config obj and read config file data
        cfg = Config(backup_config_file, backup_config_section)
        if not cfg.is_valid():
            print("Error, fail to initialize config.")
            sys.exit(2)
        if not cfg.set_options(print_options=False):
            print("Error, fail to set config.")
            sys.exit(2)

        # initial backup logging
        log = Logger(cfg.log_file_path,
                     cfg.log_level,
                     cfg.log_max_bytes,
                     cfg.log_backup_count,
                     cfg.log_delay,
                     name=const.LOG_BACKUP_LOGGER_NAME)
        if not log.set_log(log_module=cfg.log_module_name):
            print("Error, unable to set logger.")
            sys.exit(2)

        # set name of this backup
        begin_backup_datetime = get_datetime()
        if backup_name == None:
            backup_name = normalize_datetime(begin_backup_datetime)
        print("- backup name: %s" % backup_name)

        # start RBD backup
        log.blank(line_count=4)
        log.info("******** Start Ceph RBD backup ********",
                 "pid = %s" % os.getpid(),
                 "config file = %s" % backup_config_file,
                 "section = %s" % backup_config_section)
        log.info('Config settings:', cfg.get_option())

        # ==================================================================
        # check backup directory environment, space size and metafile if exists.
        # ==================================================================
        log.info("________ Check Backup Directory ________")
        print("- check backup directory.")

        # Path structure of backup directory:
        # Dest. Backup Dir/Ceph Name/Pool Name/RBD name/Circle Name/Backup Files
        directory = Directory(log)
        log.info("Set backup path:",
                 " - backup destination path = %s" % cfg.backup_destination_path,
                 " - ceph cluster name = %s" % cfg.ceph_cluster_name)

        cluster_backup_path = os.path.join(cfg.backup_destination_path,
                                           cfg.ceph_cluster_name)
        if not directory.exist(cluster_backup_path):
            cluster_backup_path = directory.create(cluster_backup_path)
            if cluster_backup_path == False:
                log.error("Fail to create directory path.")
                sys.exit(2)
        print("  set backup directory: %s" % cluster_backup_path)

        log.info("Get space size info '%s'." % cluster_backup_path)
        backup_dir_avai_bytes = directory.get_available_size(cluster_backup_path)
        backup_dir_used_bytes = directory.get_used_size(cluster_backup_path)

        if backup_dir_avai_bytes == False:
            log.error("Fail to get space size of directory '%s'." % cluster_backup_path)
            sys.exit(2)

        log.info("Available %s bytes, used %s bytes." % (backup_dir_avai_bytes,
                                                         backup_dir_used_bytes))

        print("  %s Mbytes available." % int(backup_dir_avai_bytes/1024/1024))
        print("  %s Mbytes used." % int(backup_dir_used_bytes/1024/1024))

        # read metadata file in backup directory
        #   get last snapshot name and backup circle directory name
        log.info("Check metadata of the backup directory.")

        backup_meta = RBD_Backup_Metadata(log, cluster_backup_path)
        meta_cluster_info = backup_meta.get_cluster_info()
        if all (k in meta_cluster_info for k in ('name', 'fsid')):
            if meta_cluster_info['name'] != cfg.ceph_cluster_name:
                log.error("Cluster name is not match.",
                          "name from backup directory: %s" % meta_cluster_info['name'],
                          "name from backup config file: %s" % cfg.ceph_cluster_name)
                sys.exit(2)

            ceph_cfg = Config(cfg.ceph_conf_file, const.CEPH_CONFIG_SECTION)
            if not ceph_cfg.is_valid():
                log.error("Unable to read ceph config.")
                sys.exit(2)
            ceph_cfg_fsid = ceph_cfg.get_option(key='fsid')
            if meta_cluster_info['fsid'] !=  ceph_cfg_fsid:
                log.error("Cluster fsid is not match.",
                          "fsid from backup directory: %s" % meta_cluster_info['fsid'],
                          "fsid from ceph config file: %s" % ceph_cfg_fsid)
                sys.exit(2)
        else:
            # this maybe the first time of backup
            # copy ceph config and keyring files to backup directory
            directory.copy_file(cfg.ceph_conf_file, cluster_backup_path)
            directory.copy_file(cfg.ceph_keyring_file, cluster_backup_path)

        # ==================================================================
        # read rbd backup list, backup list source might either from
        # Openstack yaml file or backup list file (yaml format) or command line
        # ==================================================================
        log.info("________ Read RBD Backup List ________")
        print("- check backup rbd list.")

        backup_option = RBD_Backup_Option(log)
        rbd_backup_list = RBD_Backup_List(log)

        rbd_name_list = {}  # rbd name list : {'pool_name': ['rbd_name', ...], ...}
        pool_count = 0
        rbd_count = 0

        if is_rbd_list_from_command_line == True:
            log.info("Read backup list from command line.")
            print("  get backup list from command line input.")

            for rbd_list_input in args['rbd_list']:
                rbd_info = rbd_list_input.split("/")

                if len(rbd_info) == 2:
                    pool_name = rbd_info[0]
                    rbd_name = rbd_info[1]

                    if not rbd_name_list.has_key(pool_name):
                        rbd_name_list[pool_name] = [rbd_name]
                    else:
                        rbd_list = rbd_name_list[pool_name]
                        if not rbd_name in rbd_list:
                            rbd_list.append(rbd_name)
                            rbd_name_list[pool_name] = rbd_list
                        else:
                            log.warning("Duplicated RBD name '%s'." % rbd_name)
                            continue

                    rbd_count += 1
                    print("  %s - %s %s" % (rbd_count, pool_name, rbd_name))
                else:
                    log.error("Invalid rbd input list. %s" % rbd_list_input)
                    print("Error, Please input RBD name as '<pool_name>/<rbd_name>'\n" \
                          "For example, 3 RBDs to backup:\n" \
                          "  rbd/rbd_a rbd/rbd_b volume/rbd_1")
                    sys.exit(2)
        else:
            if cfg.backup_list_from_openstack_yaml_file == 'True':
                log.info("Read backup list from OpenStack YAML file.")
                print("  get backup list from OpenStack YAML file %s." % cfg.openstack_yaml_file_path)

                file_path = cfg.openstack_yaml_file_path
                section_name = cfg.openstack_yaml_section
                distribution = cfg.openstack_distribution
                ceph_pool = cfg.openstack_ceph_pool

                if not os.path.exists(cfg.openstack_yaml_file_path):
                    log.error("Openstack Yaml file '%s' not exists." % cfg.backup_list_file_path)
                    sys.exit(2)

                rbd_backup_list.read_yaml(cfg.openstack_yaml_file_path)
                rbd_name_list = rbd_backup_list.get_openstack_volume_names()

            else:
                log.info("Read RBD list from backup list file.")
                print("  get RBD backup list from %s." % cfg.backup_list_file_path)

                if not os.path.exists(cfg.backup_list_file_path):
                    log.error("Backup list file '%s' not exists." % cfg.backup_list_file_path)
                    sys.exit(2)

                rbd_backup_list.read_yaml(cfg.backup_list_file_path)
                rbd_name_list = rbd_backup_list.get_rbd_name_list(cfg.ceph_cluster_name)

                if rbd_name_list == {}:
                    log.warning("No any item in RBD backup list.")
                    print("Info, No any item in RBD backup list.")
                    sys.exit(0)
                if rbd_name_list == False:
                    log.error("unable to get rbd name list from backup list file.")
                    print("Error, unable to get rbd name list from backup list file.")
                    sys.exit(2)

                for pool_name, rbd_list in rbd_name_list.iteritems():
                    pool_count += 1

                    if cfg.backup_read_options == 'True':
                        for rbd_name in rbd_list:
                            options = rbd_backup_list.get_rbd_options(cfg.ceph_cluster_name,
                                                                      pool_name,
                                                                      rbd_name)
                            backup_option.add_option(pool_name, rbd_name, options)

            rbd_count += len(rbd_list)
            log.info("%s RBD images to backup in pool '%s'." % (rbd_count, pool_name))

        log.info("Total %s RBD images configured to backup." % rbd_count)
        print("  %s RBD(s) to be backuped." % rbd_count)

        if rbd_count == 0:
            sys.exit(0)

        # ==================================================================
        # check ceph cluster
        # examine the RBD backup list in CEPH cluster.
        # ignore not exist RBD in the backup list
        # ==================================================================
        log.info("________ Verify RBD Backup List ________")
        print("- verify RBD backup list.")

        valid_rbd_count = 0

        #ceph_conff_file = cfg.ceph_conf_file
        ceph = Ceph(log, cfg.ceph_cluster_name, conffile=cfg.ceph_conf_file)
        if not ceph.connect_cluster():
            log.error("Unable to connect ceph cluster.")
            # you may check user or permission to /etc/ceph directory
            print("Error, unable to connect ceph cluster.")
            sys.exit(2)

        ceph_fsid = ceph.get_fsid()
        ceph_stats = ceph.get_cluster_stats()

        if meta_cluster_info.has_key('fsid'):
            if ceph_fsid != meta_cluster_info['fsid']:
                log.error("Ceph fsid is not matching to the backup directory.")
                print("Error, the fsid from the ceph cluster is not matching " \
                      "to the backup directory.")
                sys.exit(2)

        log.info("Update cluster info metadata.")
        ceph_stats['fsid'] = ceph_fsid
        ceph_stats['name'] = cfg.ceph_cluster_name
        ceph_stats['conf'] = os.path.basename(cfg.ceph_conf_file)
        ceph_stats['keyr'] = os.path.basename(cfg.ceph_keyring_file)
        backup_meta.set_cluster_info(ceph_stats)

        all_pool_name_list = ceph.get_pool_list()
        if all_pool_name_list == False:
            log.error("Unable to get pool name list from ceph cluster.")
            print("Error, unable to get pool name list from ceph cluster.")
            sys.exit(2)
        log.info("Pool name in Ceph cluster:", all_pool_name_list)

        for pool_name, rbd_list in rbd_name_list.iteritems():
            log.info("Check RBDs in Ceph pool '%s'." % pool_name)

            if pool_name not in all_pool_name_list:
                log.warning("Pool '%s' is not found, " \
                            "skip backup of the pool." % pool_name)
                continue
            if not ceph.open_ioctx(pool_name):
                log.warning("Unable to open ioctx of pool '%s', " \
                            "skip backup of the pool." % pool_name)
                continue
            pool_rbd_name_list = ceph.get_rbd_list()  # rbd name list in a pool
            if pool_rbd_name_list == False:
                log.warning("Unable to get RBD list from ceph cluster, " \
                            "skip backup of the pool")
                continue

            # just log pool stat first
            pool_stat = ceph.get_pool_stat()
            log.info("Pool stat:", pool_stat)

            for rbd_name in rbd_list:
                log.info("Check RBD '%s'." % rbd_name)

                if rbd_name not in pool_rbd_name_list:
                    log.warning("RBD '%s' is not exist." % rbd_name)
                    continue
                rbd_size = ceph.get_rbd_size(rbd_name)
                rbd_snap = ceph.get_snap_info_list(rbd_name)    # return list of (snap id, snap size, snap name)
                if rbd_size == False or rbd_snap == False:
                    log.warning("Unable to get size or snapshot list of the RBD, "
                                "skip backup of the RBD.")
                    continue

                # build rbd backup list
                rbd_info = pack_rbd_info(pool_name, rbd_name, rbd_size, rbd_snap)
                backup_rbd_info_list.append(rbd_info)
                total_rbd_size += rbd_size

                valid_rbd_count += 1
                print("  %s/%s - %s bytes." % (pool_name, rbd_name, rbd_size))

                # compare rbd stat
                rbd_stat = ceph.get_rbd_stat(rbd_name)
                meta_rbd_stat = backup_meta.get_rbd_info(pool_name, rbd_name)
                if not cmp(rbd_stat, meta_rbd_stat):
                    log.info("RBD stat has been changed.",
                             "Old: %s" % meta_rbd_stat,
                             "New: %s" % rbd_stat)

                backup_meta.set_rbd_info(pool_name, rbd_name, rbd_stat)

            ceph.close_ioctx()

        print("  %s RBD(s) can be backuped." % valid_rbd_count)
        log.info("Total %s bytes of RBD images size to backup." % total_rbd_size)
        print("  total RBDs has %s Mbytes." % int(total_rbd_size/1024/1024))

        if valid_rbd_count == 0:
            sys.exit(0)

        reserve_space = backup_dir_avai_bytes * 0.01
        usable_space_size = backup_dir_avai_bytes - reserve_space
        if total_rbd_size > usable_space_size:
            log.error("No enough space size for backup, stop backup work.",
                      " - %s bytes of RBD images to backup." % total_rbd_size,
                      " - %s bytes of usable space size (99 percents of available bytes)." % usable_space_size,
                      " - %s bytes more required." % (total_rbd_size-usable_space_size))

            print("Error, No enough space size to backup.")
            sys.exit(2)

        # ==================================================================
        # Verify backup types.
        # Set backup type of RBD, change from incremental to full backup if
        #   a. backup_type is configured as 'full' in backup list file
        #   b. no last snapshot record found in metafile or metafile not exist
        #   c. last snapshot is not found in ceph cluster
        #   d. reached max incremental backup count
        # ==================================================================
        log.info("________ Check RBD backup type ________")
        print("- check rbd backup type.")

        # store backup options
        rbd_backup_type = {}    # RBD backup type { rbd_id: 'full' or 'incr' }

        full_weekday = cfg.weekly_full_backup.replace(" ","")
        incr_weekday = cfg.weekly_incr_backup.replace(" ","")
        full_weekdays = full_weekday.split(',')
        incr_weekdays = incr_weekday.split(',')

        if len(full_weekdays) == 0:
            log.warning("There is no full backup weekday configured.")

        log.info("Check default backup type for today.")
        weekday = str(int(datetime.datetime.today().weekday()) + 1)
        if weekday in full_weekdays:
            weekday_backup_type = const.EXPORT_TYPE[0]
        elif weekday in incr_weekday:
            weekday_backup_type = const.EXPORT_TYPE[1]
        else:
            log.info("No bacakup triggered on today (weekday=%s)." % weekday)
            print("Info, No bacakup triggered on today.")
            sys.exit(0)

        log.info("Backup type for today is '%s'." % weekday_backup_type)

        for rbd_info in backup_rbd_info_list:
            # you may do further manipulation of each rbd backup.
            # control attributed can be defined in rbd backup list file
            # and write your control logic in this block to overwrite configured setting

            pool_name, rbd_name, rbd_size, rbd_snap = unpack_rbd_info(rbd_info)
            rbd_id = convert_rbd_id(pool_name, rbd_name)

            # verify backup type
            # --------------------------------------
            log.info("Check backup type of '%s'." % rbd_id)

            if cfg.backup_read_options == 'True':
                log.info("Check backup type form backup option.")
                option_backup_type = backup_option.get_backup_type(pool_name,
                                                                   rbd_name)
                if option_backup_type == False:
                    rbd_backup_type[rbd_id] = weekday_backup_type
                else:
                    rbd_backup_type[rbd_id] = option_backup_type

            if rbd_backup_type[rbd_id] == const.EXPORT_TYPE[1]:
                # rbd snapshot check
                log.info("Check last backup snapshot.")
                meta_snap_info_list = backup_meta.get_backup_snapshot_list(pool_name,
                                                                           rbd_name)
                if len(meta_snap_info_list) == 0:
                    log.warning("No snapshot list metadata found.")
                    rbd_backup_type[rbd_id] = const.EXPORT_TYPE[0]
                else:
                    meta_last_snap_info = meta_snap_info_list[-1]
                    meta_last_snap_name = meta_last_snap_info['name']
                    ceph_snap_name_list = [i['name'] for i in rbd_snap]   # get snap name list
                    if meta_last_snap_name not in ceph_snap_name_list:
                        log.warning("Snapshot name '%s' is not found in ceph cluster." % meta_last_snap_name)
                        rbd_backup_type[rbd_id] = const.EXPORT_TYPE[0]

            if rbd_backup_type[rbd_id] == const.EXPORT_TYPE[1]:
                # backup circle directory check
                log.info("Check last backup circle.")
                meta_circle_info_list = backup_meta.get_backup_circle_list(pool_name,
                                                                           rbd_name)
                if len(meta_circle_info_list) == 0:
                    log.warning("No circle list metadata found.")
                    rbd_backup_type[rbd_id] = const.EXPORT_TYPE[0]
                else:
                    meta_last_circle_info = meta_circle_info_list[-1]
                    meta_last_circle_name = meta_last_circle_info['name']
                    if not directory.exist(cluster_backup_path,
                                           pool_name,
                                           rbd_name,
                                           meta_last_circle_name):
                        log.warning("Last backup circle directory is not exist.")
                        rbd_backup_type[rbd_id] = const.EXPORT_TYPE[0]

            if rbd_backup_type[rbd_id] == const.EXPORT_TYPE[1]:
                # max incremental backup count check
                log.info("Check max incremental backup.")
                max_incr_count = backup_option.get_backup_max_incr_count(pool_name,
                                                                         rbd_name)
                if max_incr_count == False:
                    max_incr_count = cfg.backup_max_incremental

                if max_incr_count in [0, '0', False, 'False', None]:
                    log.info("No max incremental backup limited.")
                else:
                    file_list = directory.get_file_list(cluster_backup_path,
                                                        pool_name,
                                                        rbd_name,
                                                        meta_last_circle_name)
                    if len(file_list) > int(max_incr_count):
                        log.info("Max incremental backup reached (%s/%s)." % (len(file_list),
                                                                              max_incr_count))
                        rbd_backup_type[rbd_id] = const.EXPORT_TYPE[0]

            log.info("Set backup type of '%s/%s' to '%s'." % (pool_name,
                                                              rbd_name,
                                                              rbd_backup_type[rbd_id]))
            print("  %s %s - %s backup." % (pool_name,
                                            rbd_name,
                                            rbd_backup_type[rbd_id]))

        # ==================================================================
        # sort rbd backup list by provisioned size of RBD, from large to small.
        # may implement other sorting method,
        # by type(full or incr first), priority, or others
        #(rbd loading or exist object count or created snapshot size)
        # ==================================================================
        log.info("________ Sort RBD backup list ________")
        print("- sort rbd backup list order.")

        list_sort = RBD_Backup_List_Sort(log)
        sorted_rbd_backup_list = list_sort.sort_by_rbd_size(backup_rbd_info_list)

        backup_priority = []
        count = 0
        for rbd_info in backup_rbd_info_list:
            pool_name, rbd_name, rbd_size, rbd_snap = unpack_rbd_info(rbd_info)
            count += 1
            backup_pos = "%s : %s/%s : %s" % (count, pool_name, rbd_name, rbd_size)
            backup_priority.append(backup_pos)
            print("  %s %s %s" % (pool_name, rbd_name, rbd_size))
        log.info("RBD backup priority:", backup_priority)

        # ==================================================================
        # initial worker manager, task maker
        # ==================================================================
        log.info("________ Initial task worker manager and task maker ________")
        print("- start task workers.")

        manager = Manager(log, tmp_dir=const.TMP_DIR)
        manager.set_worker_logger(cfg.log_file_path, name=const.LOG_WORKER_LOGGER_NAME)
        manager.set_monitor_logger(cfg.log_file_path, name=const.LOG_MONITOR_LOGGER_NAME)
        manager.run_worker(count=cfg.backup_concurrent_worker_count)

        print("  %s worker(s) started." % manager.worker_count)

        # initial task maker
        task_maker = RBD_Backup_Task_Maker(log,
                                           cfg.ceph_cluster_name,
                                           cfg.ceph_conf_file,
                                           cfg.ceph_keyring_file)
        task_maker.set_export_full_type(const.EXPORT_TYPE[0])
        task_maker.set_export_diff_type(const.EXPORT_TYPE[1])

        # for storing task result and write to file
        task_result = RBD_Backup_Task_Result(log)

        # ==================================================================
        # start RBD backup procedure
        # ==================================================================

        # create and submit rbd snapshot create tasks
        # ----------------------------------------------------------
        log.info("________ Create and submit RBD snapshot create task ________")
        print("- start RBD snapshot procedure.")

        for rbd_info in backup_rbd_info_list:
            pool_name, rbd_name, rbd_size, rbd_snap = unpack_rbd_info(rbd_info)
            log.info("Creating RBD snapshot create task of '%s/%s'." % (pool_name,
                                                                        rbd_name))
            snap_create_task = task_maker.get_rbd_snapshot_create_task(pool_name, rbd_name)

            if snap_create_task != False:
                log.info("Submit SnapshotCreateTask '%s'." % snap_create_task)
                if manager.add_task(snap_create_task):
                    submitted_snap_create_task_count += 1
                    print("  take snapshot of %s/%s" % (pool_name, rbd_name))
            else:
                log.error("Unable to get RBD snapshot create task.")
        log.info("Submitted %s snapshot create tasks." % submitted_snap_create_task_count)

        # verify finished snapshot tasks
        # ----------------------------------------------------------
        log.info("________ Verify finished RBD snapshot create task ________")

        for i in xrange(0, submitted_snap_create_task_count):
            try:
                finished_task = manager.get_finished_task()
                log.info("Received finished task %s." % finished_task)

                pool_name = finished_task.pool_name
                rbd_name = finished_task.rbd_name
                created_snapshot_name = finished_task.snap_name
                created_datetime = finished_task.create_datetime

                result = finished_task.get_result()
                task_result.add_snapshort_create_result(result)
                log.info("Task Result: ", result)

                # remove rbd backup item from backup list because of failure of snapshot create.
                if result['return_code'] != 0:
                    log.warning("%s is not completed. " % result['task_name'],
                                "remove the RBD from backup list.")
                    print("  snapshot of %s/%s failed." % (pool_name, rbd_name))
                    temp_rbd_backup_list = []
                    for rbd_info in backup_rbd_info_list:
                        if rbd_info['pool_name'] != pool_name and rbd_info['rbd_name'] != rbd_name:
                            temp_rbd_backup_list.append(rbd_info)
                    backup_rbd_info_list = temp_rbd_backup_list
                else:
                    log.info("%s is completed, " % result['task_name'])
                    print("  snapshot of %s/%s completed." % (pool_name, rbd_name))

                    # just set default snapshot info
                    created_snapshot_info = {'id': None,
                                             'size': None,
                                             'name': created_snapshot_name,
                                             'datetime': created_datetime}
                    if ceph.open_ioctx(pool_name):
                        snapshot_info = ceph.get_snap_info(rbd_name, created_snapshot_name)
                        if snapshot_info == False:
                            log.warning("Unable to get snapshot info")
                        else:
                            # replace with detail info
                            created_snapshot_info = snapshot_info
                    else:
                        log.error("Unable to open ioctx of pool '%s'." % pool_name)

                    log.info("Update backup snapshot info metadata.")
                    backup_meta.add_backup_snapshot_info(pool_name, rbd_name, created_snapshot_info)

            except Exception as e:
                log.error("Unable to verify snapshot create task. %s" % e)
                continue

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # todo: after create new snapshot, we may sort again the rbd_backup list
        # base on the size of the new created snapshot. (if supported to get used size)
        #
        #list_sort = RBD_Backup_List_Sort(log)
        #sorted_rbd_backup_list = list_sort.sort_by_snap_size(backup_rbd_info_list)


        # Create and submit export tasks
        # ----------------------------------------------------------
        log.info("________ Create and submit RBD export task ________")
        print("- start RBD export procedure.")

        for rbd_info in backup_rbd_info_list:
            pool_name, rbd_name, rbd_size, rbd_snap = unpack_rbd_info(rbd_info)
            log.info("Creating RBD export task of '%s/%s'." % (pool_name,
                                                               rbd_name))
            rbd_id = convert_rbd_id(pool_name, rbd_name)
            backup_type = rbd_backup_type[rbd_id]

            # get snapshot name form metadata
            meta_snap_info_list = backup_meta.get_backup_snapshot_list(pool_name, rbd_name)
            if len(meta_snap_info_list) == 0:
                log.warning("No last snapshot found.")
                continue
            new_created_snap_info = meta_snap_info_list[-1]  # from this backup
            new_created_snap_name = new_created_snap_info['name']

            log.info("Backup type is %s" % backup_type)
            if backup_type == const.EXPORT_TYPE[0]:  # full
                # create the circle dir and get the path
                new_circle_name = new_created_snap_name
                backup_circle_path = directory.create(cluster_backup_path,
                                                      pool_name,
                                                      rbd_name,
                                                      new_circle_name)
                if backup_circle_path == False:
                    log.error("Unable to create RBD backup destination path, " \
                              "skip this RBD export.")
                    continue

                export_task = task_maker.get_rbd_export_full_task(pool_name,
                                                                  rbd_name,
                                                                  new_created_snap_name,
                                                                  backup_circle_path)
            elif backup_type == const.EXPORT_TYPE[1]:  # incr
                # get snapshot created from last backup
                last_created_snap_info = meta_snap_info_list[-2]  # from last backup
                last_created_snap_name = last_created_snap_info['name']

                # get belonging circle dir name
                meta_circle_info_list = backup_meta.get_backup_circle_list(pool_name, rbd_name)
                meta_last_circle_info = meta_circle_info_list[-1]
                meta_last_circle_path = meta_last_circle_info['path']

                # get the circle dir path
                backup_circle_path = os.path.join(cluster_backup_path,
                                                  pool_name,
                                                  rbd_name,
                                                  meta_last_circle_path)

                export_task = task_maker.get_rbd_export_diff_task(pool_name,
                                                                  rbd_name,
                                                                  new_created_snap_name,
                                                                  last_created_snap_name,
                                                                  backup_circle_path)
            else:
                log.warning("Unknown backup type '%s'. skip." % rbd_backup_type[rbd_id])
                continue

            if export_task != False:
                log.info("Submit RBD export task '%s'" % export_task)
                if manager.add_task(export_task):
                    submitted_rbd_export_task_count += 1
                    print("  export %s/%s" % (pool_name, rbd_name))
            else:
                log.error("Unable to get RBD export task.")

        log.info("Submitted %s RBD export tasks." % submitted_rbd_export_task_count)

        # verify finished export tasks
        # ----------------------------------------------------------
        log.info("________ Verify finished RBD export task ________")
        backup_list_info = []
        for i in xrange(0, submitted_rbd_export_task_count):
            try:
                finished_task = manager.get_finished_task()
                log.info("Received finished task %s." % finished_task)

                pool_name = finished_task.pool_name
                rbd_name = finished_task.rbd_name
                snap_name = finished_task.snap_name
                created_datetime = finished_task.create_datetime
                circle_dir_name = directory.get_basename(finished_task.dest_path)

                result = finished_task.get_result()
                task_result.add_export_task_result(result)
                log.info("Task Result: ", result)

                if result['return_code'] != 0:
                    log.warning("%s is not completed, " % result['task_name'])
                    print("  export %s/%s failed." % (pool_name, rbd_name))
                    # remove export incompleted file if exist
                    if finished_task.export_type == const.EXPORT_TYPE[0]:
                        directory.delete(finished_task.dest_path)
                    elif finished_task.export_type == const.EXPORT_TYPE[1]:
                        directory.delete(finished_task.dest_filepath)
                else:
                    log.info("%s is completed, " % result['task_name'])
                    print("  export of %s/%s completed." % (pool_name, rbd_name))
                    if finished_task.export_type == const.EXPORT_TYPE[0]:
                        log.info("Update backup circle info metadata.")
                        circle_info =  {'backup_name': backup_name,
                                        'name': finished_task.dest_file,
                                        'path': circle_dir_name,
                                        'datetime': created_datetime}
                        backup_meta.add_backup_circle_info(pool_name, rbd_name, circle_info)
                    elif finished_task.export_type == const.EXPORT_TYPE[1]:
                        log.info("Update incremental backup info metadata.")
                        incr_info = {'backup_name': backup_name,
                                     'name': finished_task.dest_file,
                                     'from': finished_task.from_snap,
                                     'to': finished_task.snap_name,
                                     'datetime': created_datetime}
                        backup_meta.add_backup_incremental_info(pool_name,
                                                                rbd_name,
                                                                circle_dir_name,
                                                                incr_info)
                backup_list_info.append((result['return_code'],
                                         pool_name,
                                         rbd_name,
                                         circle_dir_name,
                                         snap_name))
            except Exception as e:
                log.error("Unable to verify export task. %s" % e)
                continue

            log.info("Update backup export info metadata.")
            backup_meta.add_backup_info(backup_name, backup_list_info)

        # remove exceed snapshot
        # ----------------------------------------------------------
        log.info("________ Delete exceed RBD snapshots ________")
        print("- check exceed RBD snapshot.")

        # reduce number of worker to 1 only for sequence exec of snapshot delete task
        stop_worker_count = int(cfg.backup_concurrent_worker_count) - 1
        if stop_worker_count != 0:
            manager.stop_worker(count=stop_worker_count)

        for rbd_info in backup_rbd_info_list:
            pool_name, rbd_name, rbd_size, rbd_snap = unpack_rbd_info(rbd_info)
            log.info("Check snapshots of RBD '%s/%s'." % (pool_name,
                                                          rbd_name))

            max_snap_count = backup_option.get_snapshot_retain_count(pool_name,
                                                                     rbd_name)
            if max_snap_count == False:
                snap_retain_count = cfg.backup_snapshot_retain_count
            else:
                snap_retain_count = max_snap_count

            # retrieve snapshot name to a list
            ceph_snap_name_list = [i['name'] for i in rbd_snap]

            matched_snapshot_naem_list = []
            meta_snap_info_list = backup_meta.get_backup_snapshot_list(pool_name, rbd_name)

            for meta_snap_info in meta_snap_info_list:
                meta_snap_name = meta_snap_info['name']
                if meta_snap_name in ceph_snap_name_list:
                    matched_snapshot_naem_list.append(meta_snap_name)

            # do a trick to correct count of matched snapshot name list
            # add one more count for the new created snapshot before.
            matched_snapshot_count = (len(matched_snapshot_naem_list)+1)

            # create snapshot delete task.
            diff_count = (matched_snapshot_count - int(snap_retain_count))
            if diff_count > 0:
                log.info("%s exceed snapshot to be deleted." % diff_count)
                for i in range(0, diff_count):

                    snap_name = matched_snapshot_naem_list[i]    # get snap name for matched snapshot name
                    snap_delete_task = task_maker.get_rbd_snapshot_delete_task(pool_name,
                                                                               rbd_name,
                                                                               snap_name)
                    if snap_create_task != False:
                        log.info("Submit SnapshotDeleteTask '%s'" % snap_delete_task)

                        if manager.add_task(snap_delete_task):

                            # check result after submit the task
                            finished_task = manager.get_finished_task()
                            log.info("%s is completed." % (finished_task))
                            pool_name = finished_task.pool_name
                            rbd_name = finished_task.rbd_name
                            deleted_snap_name = finished_task.snap_name

                            result = finished_task.get_result()
                            task_result.add_snapshort_delete_result(result)
                            log.info("Task Result: ", result)

                            # mark deleted snapshot,
                            if result['return_code'] != 0:
                                log.error("%s is not completed." % result['task_name'])
                                continue
                            else:
                                log.info("Update backup snapshot info metadata.")
                                meta_snap_info_list = backup_meta.get_backup_snapshot_list(pool_name,
                                                                                           rbd_name)
                                for meta_snap_info in meta_snap_info_list:
                                    if meta_snap_info['name'] == deleted_snap_name:
                                        backup_meta.del_backup_snapshot_info(pool_name,
                                                                             rbd_name,
                                                                             meta_snap_info,
                                                                             key='name')
                                        break

                                print("  delete snapshot %s of %s/%s" % (snap_name,
                                                                         pool_name,
                                                                         rbd_name))
                                submitted_snap_delete_task_count += 1
                    else:
                        log.error("Unable to get RBD snapshot delete task.")
            else:
                log.info("No snapshot to be deleted.")

        log.info("Total deleted %s RBD snapshots." % submitted_snap_delete_task_count)

        # remove exceed backup circle
        # ----------------------------------------------------------
        log.info("________ Delete exceed backup circle ________")
        print("- check execeed RBD backup circle.")

        for rbd_info in backup_rbd_info_list:
            try:
                pool_name, rbd_name, rbd_size, rbd_snap = unpack_rbd_info(rbd_info)
                log.info("Check backup circle of RBD '%s/%s'." % (pool_name,
                                                                  rbd_name))

                max_circ_count = backup_option.get_backup_circle_retain_count(pool_name,
                                                                              rbd_name)
                if max_circ_count == False:
                    circle_retain_count = cfg.backup_circle_retain_count
                else:
                    circle_retain_count = max_circ_count

                backup_circle_dir_list = directory.get_dir_list(cluster_backup_path,
                                                                pool_name,
                                                                rbd_name)
                meta_circle_info_list = backup_meta.get_backup_circle_list(pool_name,
                                                                           rbd_name)

                circle_counter = 0
                matched_circle_dir_list = []
                for meta_circle_info in meta_circle_info_list:
                    if meta_circle_info['path'] in backup_circle_dir_list:
                        matched_circle_dir_list.append(meta_circle_info)
                    else:
                        log.warning("Missing circle directory '%s'." % meta_circle_info['path'])

                matched_circle_dir_count = len(matched_circle_dir_list)

                log.info("%s matched backup circle, " \
                         "%s backup circle to retain." %
                         (matched_circle_dir_count, circle_retain_count))

                diff_count = (matched_circle_dir_count - int(circle_retain_count))
                if diff_count <= 0:
                    log.info("No backup circle to be deleted.")
                    continue

                log.info("%s exceed backup circle to be deleted." % diff_count)
                for i in range(0, diff_count):
                    delete_backup_circle_info = matched_circle_dir_list[i]
                    circle_path = delete_backup_circle_info['path']
                    circle_name = delete_backup_circle_info['name']
                    log.info("Delete backup circle dir '%s'." % circle_path)
                    delete_circle_path = directory.delete(cluster_backup_path,
                                                          pool_name,
                                                          rbd_name,
                                                          circle_path)
                    if delete_circle_path == False:
                        log.warning("Unable to delete the backup circle dir.")
                        continue

                    log.info("Update backup circle info metadata.")
                    backup_meta.del_backup_circle_info(pool_name,
                                                       rbd_name,
                                                       delete_backup_circle_info,
                                                       key='name')
                    log.info("Update incremental backup info metadata.")
                    backup_meta.del_backup_incremental_info(pool_name,
                                                            rbd_name,
                                                            circle_name)

                    print("  delete backup circle %s of %s/%s" % (circle_name,
                                                                  pool_name,
                                                                  rbd_name))
                    task_result.add_backup_circle_delete_result(delete_circle_path)
                    backup_circle_delete_count += 1
            except Exception as e:
                log.error("Unable to complete delete of exceed backup circle. %s" % e)
                continue

        log.info("Total deleted %s backup circle directory." % backup_circle_delete_count)


        # finalize RBD backup
        # ----------------------------------------------------------
        log.info("________ Finalize RBD backup ________")

        task_result.write_to_file(backup_name)

        manager.stop()
        ceph.disconnect_cluster()

        begin_backup_timestamp = get_timestamp(begin_backup_datetime)
        log.info("******** Ceph RBD backup complete ********",
                 "use %s seconds " % get_elapsed_time(begin_backup_timestamp))


    except Exception as e:

        exc_type,exc_value,exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)

        if manager != None:
            manager.stop()
        if ceph  != None:
            ceph.disconnect_cluster()

        sys.exit(2)


if "__main__" == __name__:
    datetime_now = get_datetime()
    print("\n%s - Start RBD Backup.\n" % datetime_now)

    pidfile = '/var/run/rbd_backup.pid'
    pid = pidfile_check(pidfile)
    if pid:
        print("- pidfile exist, maybe a backup is running currently? exit.")
        sys.exit(1)

    atexit.register(pidfile_clear, pidfile)
    pid = str(os.getpid())
    file(pidfile, 'w+').write("%s\n" % pid)
    print("- process start running, pid %s" % pid)

    return_code = main(sys.argv)

    print("\n%s - Finish RBD Backup.\n" % get_datetime())
    sys.exit(return_code)

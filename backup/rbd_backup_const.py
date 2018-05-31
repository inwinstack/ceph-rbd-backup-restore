#!/usr/bin/env python
# coding=UTF-8
# Author: Yu-Jung Cheng


class RBD_Backup_Const(object):

    CONFIG_PATH = '/etc/rbd_backup_restore/config.ini'
    CONFIG_SECTION = 'default'

    PID_FILE = '/var/run/rbd_backup.pid'

    EXPORT_TYPE = ('full', 'incr')

    LOG_BACKUP_LOGGER_NAME = 'backup'
    LOG_RESTORE_LOGGER_NAME = 'restore'
    LOG_WORKER_LOGGER_NAME = 'worker'
    LOG_MONITOR_LOGGER_NAME = 'monitor'
    LOG_SHOW_LOGGER_NAME = 'show'
    LOG_DELETE_LOGGER_NAME = 'delete'

    META_DIRNAME = '.backup.rbd.meta.dir'
    META_CLUSTER_INFO = 'cluster.system.info'
    META_CLUSTER_RBD_INFO = 'cluster.rbd.info'
    META_CLUSTER_RBD_SNAPSHOT_INFO = 'cluster.rbd.snapshot.info'
    META_BACKUP_EXPORT_INFO = 'backup.export.info'
    META_BACKUP_CIRCLE_INFO = 'backup.circle.info'
    META_BACKUP_INCREMENTAL_INFO = 'backup.incremental.info'
    META_LIST_COUNTER_KEY = 'counter'
    META_ROTATION_LENGTH = 7



    SNAPSHOT_NAME_DATETIME_FMT = '%Y_%m_%d_%H_%M_%S'
    BACKUP_NAME_DATETIME_FMT = '%Y_%m_%d_%H_%M_%S'

    CEPH_CONFIG_SECTION = 'global'
    TMP_DIR = '/tmp'

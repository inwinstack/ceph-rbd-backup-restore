#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Yu-Jung Cheng

import rados, subprocess, sys

from rbd import RBD, Image


class Ceph(object):

    def __init__(self, log, cluster_name, conffile=''):
        self.log = log
        self.cluster_name = cluster_name
        self.conffile = conffile

        self.cluster = None
        self.ioctx = None
        self.pool_name = None

    def _ioctx_check(f):
        if self.ioctx == None:
            return False
        return True

    def connect_cluster(self, timeout=10):
        try:
            self.log.debug("Connect to the Ceph cluster. timeout=%s seconds" % timeout)
            self.cluster = rados.Rados(conffile=self.conffile)
            self.cluster.connect(timeout=timeout)
            self.log.debug("Successfully connected to the Ceph cluster.")
            return True
        except Exception as e:
            self.log.error("Fail to connect ceph cluster. %s" % e)
            return False

    def disconnect_cluster(self):
        try:
            self.cluster.shutdown()
            self.log.debug("Disconnect from the Ceph cluster.")
            return True
        except Exception as e:
            self.log.error("Fail to disconnect ceph cluster. %s" % e)
            return False

    def open_ioctx(self, pool_name):
        try:
            self.log.debug("Open ioctx of pool '%s'" % pool_name)
            self.pool_name = pool_name
            self.ioctx = self.cluster.open_ioctx(pool_name)
            self.log.debug("Ioctx of pool '%s' is opened successfully." % pool_name)
            return True
        except Exception as e:
            self.log.error("Fail to open ioctx. %s" % e)
            return False

    def close_ioctx(self):
        try:
            self.log.debug("Close ioctx of pool '%s'" % self.pool_name)
            self.ioctx.close()
            self.pool_name = None
            self.log.debug("Ioctx of pool '%s' is opened successfully." % self.pool_name)
            return True
        except Exception as e:
            self.log.error("Fail to close ioctx of pool '%s'. %s" % (self.pool_name, e))
            return False

    def get_pool_list(self):
        try:
            self.log.debug("Get pool name list from ceph cluster.")
            pool_list = self.cluster.list_pools()
            self.log.debug("Pool name list:", pool_list)
            return pool_list
        except Exception as e:
            self.log.error("Fail to get pool name list")
            return False

    def get_rbd_list(self):
        try:
            self.log.debug("Get RBD name list from ceph cluster.")
            rbd = RBD()
            rbd_list = rbd.list(self.ioctx)
            self.log.debug("RBD name list:", rbd_list)
            return rbd_list
        except Exception as e:
            self.log.error("Fail to get RBD name list. %s" % e)
            return False

    def get_cluster_stats(self):
        return self.cluster.get_cluster_stats()

    def get_fsid(self):
        return self.cluster.get_fsid()

    def get_rbd_size(self, rbd_name):
        try:
            if self.ioctx == None:
                self.log.error("Pool ioctx is not opened yet.")
                return False

            image = Image(self.ioctx, rbd_name)
            size = image.size()
            self.log.debug("Size of RBD '%s/%s' in Ceph cluster is %s." % (self.pool_name,
                                                                           rbd_name,
                                                                           size))
            return size
        except Exception as e:
            self.log.error("Fail to get RBD size. %s" % e)
            return False
        finally:
            image.close()

    def get_snap_info_list(self, rbd_name):
        try:
            image = Image(self.ioctx, rbd_name)
            snaps = image.list_snaps()
            snap_list = []
            for snap in snaps:
                snap_list.append({'id': snap['id'],
                                  'size': snap['size'],
                                  'name': snap['name']})

            self.log.debug("Snapshot list of RBD '%s/%s' in Ceph cluster:" % (rbd_name,
                                                                              self.pool_name),
                            snap_list)
            sorted_snap_list = sorted(snap_list, key=lambda k: k['id'])
            return sorted_snap_list
        except Exception as e:
            self.log.error("Fail to get snapshot. %s" % e)
            return False
        finally:
            image.close()

    def get_snap_info(self, rbd_name, snap_name):
        try:
            snapshot_list = self.get_snap_info_list(rbd_name)
            for snap_info in snapshot_list:
                if snap_name == snap_info['name']:
                    return snap_info
            return False
        except Exception as e:
            self.log.error("Fail to get snapshot info. %s" % e)
            return False

    def get_rbd_stat(self, rbd_name):
        try:
            image = Image(self.ioctx, rbd_name)
            rbd_info = image.stat()
            return rbd_info
        except Exception as e:
            self.log.error("Fail to get RBD info. %s" % e)
            return False
        finally:
            image.close()

    def get_pool_stat(self):
        try:
            pool_stat = self.ioctx.get_stats()
            return pool_stat
        except Exception as e:
            self.log.error("Fail to get pool stat. %s" % e)
            return False

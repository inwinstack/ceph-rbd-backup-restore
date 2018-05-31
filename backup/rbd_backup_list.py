#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Yu-Jung Cheng

import yaml, os


class RBD_Backup_List(object):

    def __init__(self, log):
        self.log = log
        self.rbd_list_data = {}

    def read_yaml(self, yaml_filepath):
        try:
            if os.path.exists(yaml_filepath):
                yaml_file = open(yaml_filepath, 'r')
                self.rbd_list_data = yaml.load(yaml_file, Loader=yaml.CLoader)
                self.log.debug("Read RBD list from YAML file '%s'." % yaml_filepath)
                yaml_file.close()
            else:
                self.log.error("YAML file '%s' not exist." % yaml_filepath)

        except Exception as e:
            self.log.error('Unable to read YAML file. %s' % e)

    def get_rbd_name_list(self, cluster_name, key='name'):
        try:
            name_key = key
            self.log.debug("Get RBD name list in cluster '%s'." % cluster_name)
            if self.rbd_list_data.has_key(cluster_name):

                cluster_data = self.rbd_list_data[cluster_name]
                rbd_list_data = {}
                for pool_data in cluster_data:
                    for pool_name, rbd_list in pool_data.iteritems():
                        rbd_name_list = [d[name_key] for d in rbd_list if name_key in d]
                        rbd_list_data[pool_name] = rbd_name_list

                self.log.debug("RBD name list:", rbd_list_data)
                return rbd_list_data
            else:
                return {}
        except Exception as e:
            self.log.error('Unable to generate RBD name list data. %s' % e)
            return False

    def get_rbd_options(self, cluster_name, pool_name, rbd_name):
        try:
            self.log.debug("Get RBD backup options of '%s/%s' in cluster '%s'." %
                           (pool_name, rbd_name, cluster_name))
            options = {}
            if self.rbd_list_data.has_key(cluster_name):
                rbd_list_data = self.rbd_list_data[cluster_name]

                for rbd_list_info in rbd_list_data:

                    if rbd_list_info.has_key(pool_name):
                        rbd_list = rbd_list_info[pool_name]

                        for rbd_item in rbd_list:

                            if rbd_item.has_key('name'):
                                if rbd_item['name'] == rbd_name:
                                    options = rbd_item
                                    del options['name']
                                    break
                        break
            self.log.debug("RBD backup options:", options)
            return options
        except Exception as e:
            self.log.error('Unable to fetch RBD option. %s' % e)
            return False

    # for openstack yaml file
    def get_openstack_volume_names(self):
        return []

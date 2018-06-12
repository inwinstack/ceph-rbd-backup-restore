#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Yu-Jung Cheng

import yaml, os


class RBD_Backup_List(object):

    def __init__(self, log):
        self.log = log
        self.rbd_list_data = {}
        self.openstack_yaml_data = {}
        self.cinder_client = None

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

    def read_openstack_yaml(self, yaml_filepath, yaml_section):
        try:
            if os.path.exists(yaml_filepath):
                yaml_file = open(yaml_filepath, 'r')
                yaml_data = yaml.load(yaml_file, Loader=yaml.CLoader)
                self.openstack_yaml_data = yaml_data[yaml_section]
                self.log.debug("Read OpenStack Yaml file '%s'." % yaml_filepath)
                yaml_file.close()
            else:
                self.log.error("YAML file '%s' not exist." % yaml_filepath)

        except Exception as e:
            self.log.error('Unable to read openstack Yaml file. %s' % e)
            return False

    def set_cinder_client(self, distribution=None,
                                api_version=2,
                                site_packages_path='lib/python2.7/site-packages/',
                                timeout=120,
                                endpoint_type='internalURL'):
        try:
            self.log.debug("Read OpenStack Yaml data.")
            user_name = self.yaml_data['user_name']
            password = self.yaml_data['password']
            tenant_name = self.yaml_data['tenant_name']
            cacert_path = self.yaml_data['cacert_path']
            auth_url = self.yaml_data['auth_url']
        except Exception as e:
            self.log.error('Unable to read openstack Yaml data. %s' % e)
            return False

        try:
            if distribution is None:
                from cinderclient import client
            elif self.distribution == "helion":
                helion_cindercleint = glob.glob("/opt/stack/venv/cinderclient*")
                if len(helion_cinderclient) == 0:
                    raise ImportError("Error, unable to import helion cindercleint.")
                helion_path = os.path.normpath(os.path.join(helion_cindercleint[0],
                                                            site_packages_path))
                self.log.debug("Add helion path %s" % helion_path)
                sys.path = [helion_path] + sys.path
                from cinderclient import client
            else:
                self.log.error("Unknow OpenStack distribution. %s" % distribution)
                return False
        except Exception as e:
            self.log.error('Unable to read OpenStack Yaml data. %s' % e)
            return False

        try:
            if os.path.exists(cacert_path):
                self.cinder_client = client.Client(api_version,
                                                   user_name,
                                                   password,
                                                   tenant_name,
                                                   auth_url,
                                                   cacert=cacert_path,
                                                   verify=False,
                                                   endpoint_type=endpoint_type)
            else:
                self.cinder_client = client.Client(api_version,
                                                   user_name,
                                                   password,
                                                   tenant_name,
                                                   auth_url,
                                                   timeout=timeout,
                                                   insecure=True,
                                                   verify=False,
                                                   endpoint_type=endpoint_type)

            return True
        except Exception as e:
            self.log.error("Unable to set cinder client. %s" % e)
            return False

    def _cinder_volumes(self):
        try:
            self.log.debug("Get cinder volume list.")
            return self.cinder_client.volumes.list()
        except:
            self.log.error("Unable to get cinder volume list.")
            return False

    def get_cinder_volume_list(self, only_yaml_volumes=True):
        try:
            cinder_volumes = self._cinder_volumes()
            volume_list = []
            volume_map = {}

            if only_yaml_volumes:
                map_volumes = self.openstack_yaml_data['volume_names']
                for cinder_volume in cinder_volumes:
                    if cinder_volume.name in map_volumes:
                        self.log.info("Map volume name '%s' to volume id '%s'." % (cinder_volume.name('ascii'),
                                                                                   cinder_volume.id.encode('ascii')))
                        volume_list.append(cinder_volume.id.encode('ascii'))
                        volume_map[cinder_volume.name.encode('ascii')] = cinder_volume.id.encode('ascii')
                    else:
                        self.log.warning("Unable to map volume name '%s'." % cinder_volume.name('ascii'))
            else:
                for cinder_volume in cinder_volumes:
                    self.log.info("Map volume name '%s' to volume id '%s'." % (cinder_volume.name('ascii'),
                                                                               cinder_volume.id.encode('ascii')))
                    volume_list.append(cinder_volume.id.encode('ascii'))
                    volume_map[cinder_volume.name.encode('ascii')] = cinder_volume.id.encode('ascii')

            self.volume_map = volume_map
            return volume_list

        except:
            self.log.error("Unable to create volume list.")
            return False

    def get_cinder_volume_map(self):
        self.volume_map = {}
        self.get_cinder_volume_list(only_yaml_volumes=True)
        return self.volume_map

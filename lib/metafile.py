#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, yaml, traceback


class MetaFile():

    def __init__(self, log, path=None):
        self.log = log
        self.filepath = path

    def _get_dict_list_data(self, dict_list, key_name, key_value, data_name, data_type='str'):
        dict_info_data_value = ''
        for dict_info in dict_list:
            if dict_info[key_name] == key_value:
                if rbd_info.has_key(data_name):
                    if data_type == 'list':
                        dict_info_data_value = str(dict_info[data_name]).replace(" ","").split(',')
                    else:
                        dict_info_data_value = str(dict_info[data_name])
                    break
        return dict_info_data_value

    def read(self, filepath=None, section_name=None):
        try:
            use_filepath = self.filepath
            if filepath != None:
                use_filepath = filepath

            self.log.debug("Reading metafile '%s', section = %s" % (use_filepath, section_name))

            if os.path.isfile(use_filepath):
                yaml_file = open(use_filepath, 'r')
                yaml_data = yaml.load(yaml_file, Loader=yaml.CLoader)
                yaml_file.close()
                metadata = yaml_data
            else:
                self.log.warning("Metafile not exist.")
                return {}

            if section_name is None:
                self.log.debug("Metadata:", metadata)
                return metadata
            else:
                if metadata.has_key(section_name):
                    self.log.debug("Metadata:", metadata[section_name])
                    return metadata[section_name]
                else:
                    self.log.warning("Metadata has no section '%s'." % section_name)
                    return {}
        except Exception as e:
            self.log.error("Unable to read metafile '%s'. %s" % (use_filepath, e))
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

    def write(self, data, filepath=None, section_name=None):
        try:
            if filepath != None:
                use_filepath = filepath
            else:
                use_filepath = self.filepath

            self.log.debug("Writing metafile '%s', section = %s" % (use_filepath, section_name))

            if not os.path.exists(os.path.dirname(use_filepath)):
                try:
                    os.makedirs(os.path.dirname(use_filepath))
                except OSError as exc: # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise

            if section_name != None:
                old_data = self.read()
                old_data[section_name] = data
                new_data = old_data
            else:
                new_data = data

            self.log.debug("Writing metadata:", new_data)
            with open(use_filepath, 'w') as yaml_file:
                yaml.safe_dump(new_data, yaml_file, indent=4, width=1024, default_flow_style=False)
                self.log.debug("Write metadata successfully.")
                return True
            return False
        except Exception as e:
            self.log.error("Unable to write metafile '%s'. %s" % (use_filepath, e))
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

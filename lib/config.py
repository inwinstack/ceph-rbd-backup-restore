#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Yu-Jung Cheng

import os, sys, collections

from ConfigParser import ConfigParser


class Config(object):

    def __init__(self, path, section):

        self.init_status = True

        if os.path.exists(path):
            self.path = path
        else:
            self.init_status = False
            print("Error, missing config file (%s)." % path)

        try:
            self.config = ConfigParser()
            self.config.read(self.path)
        except Exception:
            self.init_status = False
            print("Error, unable to read config file (%s)." % path)

        try:
            if self.config.has_section(section):
                self.section = section
            else:
                print("Error, missing section (%s)." % section)
                self.init_status = False
        except Exception:
            print("Error, unable to read section (%s)." % section)
            self.init_status = False

    def is_valid(self):
        return self.init_status

    def get_option(self, key=None):
        if key == None:
            return  collections.OrderedDict(self.config.items(self.section))

        if self.config.has_option(self.section, key):
            return self.config.get(self.section, key)
        else:
            print("Error, missing option (%s)." % key)
            return False

    def set_options(self, print_options=False):
        try:
            if print_options:
                print("Set config options.")

            options = dict(self.config.items(self.section))

            for option in options:
                value = self.config.get(self.section, option)
                setattr(self, option, value)
                if print_options:
                    print("  set %s to %s" % (option, value))

            return True
        except Exception as e:
            print("Error, set option failed, %s." % e)
            return False

    def verify_options(self):
        # todo: verify config options and set default value if not exist.

        return

'''
def main(argument_list):
    path='etc/rbd_backup_restore/config'
    cfg = Config(path=path,
                 section="default")
    if cfg.set_options():
        print("config initialized.")
        #print(cfg.config.items("default"))

if "__main__" == __name__:
    return_code = main(sys.argv)
    sys.exit(return_code)
'''

#!/usr/bin/env python
# coding=UTF-8

import os, sys, subprocess, traceback, datetime, time


class Directory(object):

    def __init__(self, log):
        self.log = log

    def _exec_cmd(self, cmd, shell=True):
        try:
            p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE)
            output, error = p.communicate()
            return_code = p.returncode

            #self.log.debug("Exec cmd: %s" % cmd)
            #self.log.debug("  Output: %s" % output)
            #self.log.debug("  Return: %s" % return_code)
            #print output, return_code
            if return_code != 0:
                self.log.error("Error occur while execute command %s. %s" % (cmd, error))
                return False
            return output
        except Exception as e:
            self.log.error("Fail to execute command '%s'. %s" % (cmd, e))
            return False

    def _path_join(self, *args):
        try:
            path = ''
            for dir_name in args[0]:
                path = os.path.join(path, dir_name)
            return path
        except Exception as e:
            self.log.error("Unable to join path. %s" % e)
            return False

    def delete(self, *args, **argvs):
        try:
            path = self._path_join(args)
            if path == '' or path == '/' or path == '/*' or path == False:
                return False

            cmd = "rm -rf %s" % path
            result = self._exec_cmd(cmd)

            if result == False:
                self.log.debug("Fail to delete directory path '%s'." % path)
            else:
                self.log.debug("Deleted directory path '%s' successfully." % path)
            return path
        except Exception as e:
            self.log.error("Unable to delete directory path. %s" % e)
            return False

    def copy_file(self, src_filepath, dest_path):
        self.log.debug("Copy %s to %s" % (src_filepath, dest_path))
        cmd = "cp -afpR %s %s" % (src_filepath, dest_path)
        result = self._exec_cmd(cmd)

    def exist(self, *args, **argvs):
        try:
            path = self._path_join(args)
            if os.path.isdir(path):
                self.log.debug("'%s' is exist." % path)
                return True
            self.log.debug("'%s' is not exist." % path)
            return False
        except Exception as e:
            self.log.error("Unable to create directory path. %s" % e)
            return False

    def create(self, *args, **argvs):
        # verify and create the path
        try:
            path = self._path_join(args)

            if os.path.isfile(path):
                self.log.error("The path '%s' is a regular file." % path)
                return False
            else:
                if not os.path.isdir(path):

                    self.log.info("'%s' is not exist, create the directory path." % path)
                    cmd = "mkdir -p %s" % path
                    result = self._exec_cmd(cmd)

                    if result == False:
                        self.log.error("Fail to create directory '%s'." % path)
                        return False
                    else:
                        self.log.debug("Created directory path '%s' successflly." % path)
            return path
        except Exception as e:
            self.log.error("Unable to create directory path. %s" % e)
            return False

    def get_file_list(self, *args, **argvs):
        try:
            path = self._path_join(args, argvs)
            cmd = "find %s -maxdepth 1 -type f -printf \"%%f\n\"" % path
            return_str = str(self._exec_cmd(cmd))
            return return_str.splitlines()
        except Exception as e:
            self.log.error("unable to get file list in %s. %s" % (path, e))
            return False

    def get_dir_list(self, *args, **argvs):
        try:
            path = self._path_join(args, argvs)
            cmd = "find %s -maxdepth 1 -type d -not -path %s -printf \"%%f\n\"" % (path, path)
            return_str = str(self._exec_cmd(cmd))
            return return_str.splitlines()
        except Exception as e:
            self.log.error("unable to get sub directory list in %s. %s" % (path, e))
            return False

    def get_available_size(self, *args, **argvs):
        try:
            path = self._path_join(args, argvs)
            cmd = "df -k --output=avail %s | tail -1" % path
            available_bytes = int(self._exec_cmd(cmd)) * 1024
            return int(available_bytes)
        except Exception as e:
            self.log.error("Unable to get available bytes of path '%s'. %s" % (path, e))
            return False

    def get_used_size(self, *args, **argvs):
        try:
            path = self._path_join(args, argvs)
            cmd = "du -sk %s | awk '{print $1}'" % path
            used_bytes = int(self._exec_cmd(cmd)) * 1024
            return int(used_bytes)
        except Exception as e:
            self.log.error("Unable to get used bytes of path '%s'. %s" % (path, e))
            return False

    def get_basename(self, path):
        try:
            if path[-1] == '/':
                return os.path.basename(path[:-1])
            else:
                return os.path.basename(path)
        except Exception as e:
            self.log.error("Unable to get basename. %s" % e)
            return False

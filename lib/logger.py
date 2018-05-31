#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Yu-Jung Cheng

import os, sys
import json
import logging
import logging.handlers
import inspect
import traceback
import datetime, time


class Logger(object):

    def __init__(self, path, level, max_bytes, backup_count, delay, name="logger"):
        self.path = path
        self.level = level
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.delay = delay
        self.name = name

        self.file_path = None
        self.logger = None
        self.log_module = False

    def _datetime(self):
        timestamp = time.time()
        datetime_str = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S,%f")
        return "[%s]" % datetime_str[:-3]

    def _convert_json(self, dict_data):
        try:
            json_data = json.dumps(dict_data, indent=2)
            json_data = self._indent_msg(json_data, indent_start_line=0)
            return json_data
        except Exception as e:
            self.logger.error("faile to convert json type. %s" % e)
            return dict_data

    def _get_space(self, indent_count):
        return " " * indent_count

    def _indent_msg(self, msg, indent_count=26, indent_start_line=1):

        def _convert_msg(msg, key_padding=''):
            try:
                n_msg = ''
                if isinstance(msg, dict):
                    for key, value in msg.iteritems():
                        n_msg = "".join([n_msg, '\n',str(key), ' = ', str(value)])
                    return n_msg[1:]
                elif isinstance(msg, list):
                    for item in msg:
                        n_msg = "".join([n_msg, '\n', str(item)])
                    return n_msg[1:]
                elif isinstance(msg, tuple) or hasattr(msg, '__iter__'):
                    for item in msg:    # check carefully if something wrong here
                        t_msg = _convert_msg(item)
                        n_msg = "".join([n_msg, '\n', str(t_msg)])
                    return n_msg[1:]

                return str(msg)
            except Exception as e:
                self.logger.error("fail to convert log message type. %s" % e)
                return str(msg)

        if isinstance(msg, str):
            n_msg = msg
        else:
            n_msg = _convert_msg(msg)

        line_list = str(n_msg).splitlines()
        space_indent = self._get_space(indent_count)
        line_count = len(line_list)
        for i in range(indent_start_line, line_count):
            line_list[i] = space_indent.join(['\n', line_list[i]])

        if indent_start_line == 0:
            first_line = line_list[0]
            line_list[0] = first_line[1:]
        return "".join(line_list)

    def set_log(self, log_format="[%(asctime)s] [%(levelname)s] %(message)s",
                      log_module=False):
        try:
            os.system("mkdir -p %s" % self.path)
            if not os.path.exists(self.path):
                print("Error, %s is not created.")
                return False
            if not os.access(self.path, os.W_OK):
                print("Error, %s has no write permission." % self.path)
                return False

            self.file_path = os.path.join(self.path, "%s.log" %self.name)

            if os.path.isfile(self.file_path):
                if not os.access(self.file_path, os.W_OK):
                    print("Error, %s has no write permission." % self.file_path)
                    return False

            logger = logging.getLogger(self.name)
            logger.setLevel(self.level)
            log_handler = logging.handlers.RotatingFileHandler(self.file_path,
                                                               maxBytes=self.max_bytes,
                                                               backupCount=self.backup_count,
                                                               delay=self.delay)
            log_handler.setLevel(self.level)

            #log_format1 = "[%(asctime)s] [%(levelname)s] [%(module)s] %(message)s"
            log_formatter = logging.Formatter(log_format)
            log_handler.setFormatter(log_formatter)

            logger.addHandler(log_handler)

            #print "Logger initialized."
            self.logger = logger

            if log_module in ['True', True]:
                self.log_module = True
            elif log_module == ['False', False]:
                self.log_module = False

            return True
        except Exception as e:
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

    def get_log(self):
        return self.logger

    def blank(self, line_count=1):
        cmd = "echo '' >> %s" % self.file_path
        for i in range(line_count):
            os.system(cmd)

    def section(self, section_msg):
        msg = self._indent_msg(section_msg)
        self.msg(msg)

    def msg(self, msg, timestamp=False):
        if timestamp == True:
            log_msg = self._datetime()
            log_msg += " %s" % msg
            cmd = "echo '%s' >> %s" % (log_msg, self.file_path)
        else:
            cmd = "echo '%s' >> %s" % (msg, self.file_path)
        os.system(cmd)

    def info(self, msg, *args, **kwargs):
        i_msg = ''
        for arg in args:
            i_arg = self._indent_msg2(arg)
            i_msg = "".join([i_msg, '\n', str(i_arg)])

        if i_msg != '':
            n_msg = "".join([msg, i_msg])
        else:
            n_msg = msg

        self._header_msg(msg)

        if self.log_module:
            n_msg = self._log_module_(n_msg)
        self.logger.info(n_msg)

    def error(self, msg, *args, **kwargs):
        i_msg = ''
        for arg in args:
            i_arg = self._indent_msg2(arg)
            i_msg = "".join([i_msg, '\n', str(i_arg)])

        if i_msg != '':
            n_msg = "".join([msg, i_msg])
        else:
            n_msg = msg

        if self.log_module:
            n_msg = self._log_module_(n_msg)
        self.logger.error(n_msg)

    def warning(self, msg, *args, **kwargs):
        i_msg = ''
        for arg in args:
            i_arg = self._indent_msg2(arg)
            i_msg = "".join([i_msg, '\n', str(i_arg)])

        if i_msg != '':
            n_msg = "".join([msg, i_msg])
        else:
            n_msg = msg

        if self.log_module:
            n_msg = self._log_module_(n_msg)
        self.logger.warning(n_msg)

    def debug(self, msg, *args, **kwargs):
        i_msg = ''
        for arg in args:
            i_arg = self._indent_msg2(arg)
            i_msg = "".join([i_msg, '\n', str(i_arg)])

        if i_msg != '':
            n_msg = "".join([msg, i_msg])
        else:
            n_msg = msg

        if self.log_module:
            n_msg = self._log_module_(n_msg)

        self.logger.debug(n_msg)

    def _log_module_(self, msg):
        try:
            frame = inspect.stack()[2]
            module = inspect.getmodulename(frame[1])
            n_msg = "".join(['[', module, '] ', msg])
            return n_msg
        except Exception as e:
            self.logger.error("Fail to log calling module name. %s" % e)
            return msg

    def _header_msg(self, msg):
        if isinstance(msg, str):
            if msg[:8] == "________" and msg[-8:] == "________":
                self.blank()

    def _indent_msg2(self, msg, indent_count=26, indent_start_line=0):

        def _convert_msg(msg, indent_len=0):
            try:
                n_msg = ''
                indent_space = self._get_space(indent_len)
                if isinstance(msg, dict):
                    for key, value in msg.iteritems():
                        #key_len = (len(key) + 3)
                        #value = _convert_msg(value, indent_len=key_len)
                        n_msg = "".join([n_msg, '\n', str(key), ' = ', str(value)])
                elif isinstance(msg, list):
                    for item in msg:
                        #n_item = _convert_msg(item, indent_len=4)
                        #n_msg = "".join([n_msg, '\n', str(n_item)])
                        n_msg = "".join([n_msg, '\n', str(item)])
                elif isinstance(msg, tuple) or hasattr(msg, '__iter__'):
                    for item in msg:
                        #n_item = _convert_msg(item, indent_len=4)
                        #n_msg = "".join([n_msg, ', ', str(n_item)])
                        n_msg = "".join([n_msg, ', ', str(item)])
                else:
                    return "%s%s" % (indent_space, msg)
                return n_msg[1:]
            except Exception as e:
                self.logger.error("Fail to convert log message type. %s" % e)
                return str(msg)

        try:
            n_msg = _convert_msg(msg)

            line_list = str(n_msg).splitlines()
            space_indent = self._get_space(indent_count)
            line_count = len(line_list)

            for i in range(indent_start_line, line_count):
                line_list[i] = "%s%s" % (space_indent, line_list[i])

            return "\n".join(line_list)
        except Exception as e:
            self.logger.error("Fail to indent log message. %s" % e)
            return str(msg)

    def printTree(tree, depth = 0):
        if tree == None or len(tree) == 0:
            print "\t" * depth, "-"
        else:
            for key, val in tree.items():
                print "\t" * depth, key
                self.printTree(val, depth+1)

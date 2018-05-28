#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import os
import datetime
import traceback
from argparse import ArgumentParser


def main(argument_list):

    DEFAULT_CONFIG_PATH = const.CONFIG_PATH
    DEFAULT_CONFIG_SECTION = const.CONFIG_SECTION

    try:
        parser = ArgumentParser(add_help=False)

        parser.add_argument('--config-file')
        parser.add_argument('--config-section')
        parser.add_argument('--cluster-name')
        parser.add_argument('--backup-directory')


        parser.add_argument('options', nargs='+')


    except Exception as e:

        exc_type,exc_value,exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)

        sys.exit(2)

if "__main__" == __name__:
    return_code = main(sys.argv)
    sys.exit(return_code)

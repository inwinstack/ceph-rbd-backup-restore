#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys


def main(argument_list):

    # value will replaced by install.sh
    python_path = '_PYTHON_PATH_'
    install_path = '_INSTALL_PATH_'

    try:
        if len(argument_list) == 1:
            print("Please pass arguments as instruction to the RBD backup restore tool.")
            return

        cmd_sub = argument_list[1]
        cmd_opt = ' '.join(argument_list[2:])

        if cmd_sub == 'show':
            cmd = "./backup_show.py"
        elif cmd_sub == 'merge':
            cmd = "./backup_merge.py"
        elif cmd_sub == 'delete':
            cmd = "./backup_delete.py"
        elif cmd_sub == 'backup':
            cmd = "./rbd_backup.py"
        elif cmd_sub == 'restore':
            cmd = "./rbd_restore.py"
        else:
            print("Unreconginzed instruction '%s'." % cmd_sub)
            sys.exit(2)

        cmd = "%s %s %s" % (python_path, cmd, cmd_opt)
        os.chdir(install_path)
        os.system(cmd)
        #os.system("%s &" % cmd)
        #os.system("nohup %s &" % cmd)

    except Exception as e:
        print("Error, %s" % e)
        return 2

if "__main__" == __name__:
    return_code = main(sys.argv)
    sys.exit(return_code)

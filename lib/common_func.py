#!/usr/bin/env python
# coding=UTF-8
# Author: Yu-Jung Cheng

import time, datetime, collections, hashlib, os

def get_elapsed_time(begin_time):
    now_time = time.time()
    return now_time - begin_time

def pidfile_check(pidfile):
    try:
        pf = file(pidfile,'r')
        pid = int(pf.read().strip())
        pf.close()
        return pid
    except Exception as e:
        return None

def pidfile_clear(pidfile):
    if os.path.exists(pidfile):
        os.remove(pidfile)

def convert_rbd_id(pool_name, rbd_name, hashing='md5'):
    return "%s_%s" % (pool_name, rbd_name)

    if len(pool_name) < 100 and len(rbd_name) < 100:
        pool_l = "%02d_%s" % (len(pool_name), pool_name)
        rbd_l = "%02d_%s" % (len(rbd_name), rbd_name)
        rbd_id = "%s_%s" % (pool_l, rbd_l)
        return rbd_id

    # if hasing == 'md5':
    #    rbd_id = "%s_%s" % (pool_name, rbd_name)
    #    rbd_hash = hashlib.md5(rbd_id)

def reverse_rbd_id(rbd_id):
    rbd_id_split = rbd_id.split('_')
    return rbd_id_split[0], rbd_id_split[1]

def pack_rbd_info(pool_name, rbd_name, rbd_size, rbd_snap):
    info = {}
    info['pool_name'] = pool_name
    info['rbd_name'] = rbd_name
    info['rbd_size'] = rbd_size
    info['rbd_snap'] = rbd_snap
    return info

def unpack_rbd_info(info):
    return info['pool_name'], info['rbd_name'], info['rbd_size'], info['rbd_snap']

# generate datetime string with base_path
def get_datetime_path(base_path=None, str_format='%Y_%m_%d_%H_%M_%S'):
    #timestamp = time.time()
    #datetime_str = datetime.datetime.formatimestamp(int(timestamp)).strftime(str_format)
    datetime_str = datetime.datetime.now().strftime(str_format)
    if base_path == None:
        return datetime_str
    return os.path.join(base_dir, datetime_str)

def get_datetime_fmt(timestamp, str_format='%Y-%m-%d %H:%M:%S'):
    try:
        return datetime.datetime.fromtimestamp(timestamp).strftime(str_format)
    except Exception as e:
        print e
        return False

def get_datetime(str_format='%Y-%m-%d %H:%M:%S'):
    return datetime.datetime.now().strftime(str_format)

# datetime string convert back to timestamp
def get_timestamp(datetime_str, str_format='%Y-%m-%d %H:%M:%S'):
    try:
        return time.mktime(datetime.datetime.strptime(datetime_str, str_format).timetuple())
    except Exception as e:
        print e
        return False

def normalize_datetime(datetime_str, fmt_char='_', match_chars=[':', ' ', '-']):
    n_datetime = datetime_str
    for match_char in match_chars:
        n_datetime = n_datetime.replace(match_char, fmt_char)
    double_fmt_char = "%s%s" % (fmt_char, fmt_char)
    n_datetime = n_datetime.replace(double_fmt_char, fmt_char)
    return n_datetime

def sort_tuple_list(tuple_list, key_pos=0, reverse=False):
    try:
        return sorted(tuple_list, key = lambda x : x[key_pos], reverse=reverse)
    except Exception as e:
        print e
        return False

def sort_dict_list(dict_list, key_val, reverse=False):
    try:
        return sorted(dict_list, key=lambda k: k[key_val], reverse=reverse)
    except Exception as e:
        print e
        return False

def sort_datetime_list(datetime_list, str_format='%Y_%m_%d_%H_%M_%S', reverse=False):
    try:
        return datetime_list.sort(key=lambda x: time.strptime(x, str_format)[0:6], reverse=True)
    except Exception as e:
        print e
        return False

def sort_datetime_list2(datetime_list, str_format='%Y_%m_%d_%H_%M_%S', reverse=False):
    try:
        def foo(x):
            return time.strptime(x, str_format)[0:6]
        return datetime_list.sort(key=foo, reverse=reverse)
    except Exception as e:
        print e
        return False

def diff_dict_list(dict_list1, dict_list2, diff_key='name'):
    return

def get_dict_list_data(self, dict_list, key_name, key_value, data_name, data_type='str'):
    ''' dict list = [{key_name: key_value,
                      data_name1: data_value2,
                      data_name2: data_value2, ... }, ...]
    '''
    dict_info_data_value = ''
    for dict_info in dict_list:
        if dict_info[key_name] == key_value:
            if rbd_info.has_key(data_name):
                if data_type == 'list':
                    dict_info_data_value = list(str(dict_info[data_name]).replace(" ","").split(','))
                else:
                    dict_info_data_value = str(dict_info[data_name])
                break
    return dict_info_data_value

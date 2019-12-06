#!/usr/bin/env python
from xml.etree.ElementTree import tostring
from xml.dom import minidom
import re


def _name_of_func(func):
    assert func is not None
    return func.__name__

#def traverse(obj, path=None, callback=None):
#    if path is None:
#        path = []
#
#    if isinstance(obj, dict):
#        value = {k: traverse(v, path + [k], callback)
#                 for k, v in obj.items()}
#    elif isinstance(obj, list):
#        value = [traverse(elem, path + [[]], callback)
#                 for elem in obj]
#    else:
#        value = obj
#
#    if callback is None:  # if a callback is provided, call it to get the new value
#        return value
#    else:
#        return callback(path, value)


def traverse_modify(obj, path=None, action_func=None):
    if path is None:
        path = []
    if isinstance(obj, dict):
        value = {k: traverse_modify(v, path + [k], action_func)
                 for k, v in obj.items()}
    elif isinstance(obj, list):
        value = [traverse_modify(elem, path + [[]], action_func)
                 for elem in obj]
    if action_func is None:
        return value
    else:
        try:
            value = action_func(obj)
        except:
            value = obj
        return value


def modify_tree_nodes(obj, action_func):
    if isinstance(obj, dict):
        try:
            for k, v in obj.items():
                obj[k] = action_func(obj[k])
        except:
            modify_tree_nodes(obj, action_func)
    elif isinstance(obj, dict):
        try:
            for ix, v in obj.items():
                obj[k] = action_func(obj[k])
        except:
            modify_tree_nodes(obj, action_func)
    else:
        try:
            obj = action_func(obj)
        except:
            return obj
    return obj


def prettify(elem):
    rough_string = tostring(elem, 'UTF-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="    ",encoding=None)


def groupattr(x):
    try:
        return x.group
    except:
        return x


def validate_cycle_def(cycdef):
    start_stop_step = re.compile(r'\d{12} \d{12} \d{2}:\d{2}:\d{2}')
    crude_cron_syntax = re.compile(r'(.+\s+){5}.')

    if start_stop_step.search(cycdef):
        return True
    elif crude_cron_syntax.search(cycdef):
        return True
    else:
        return False


def yes_or_no(question):
    while "not y or n response":
        reply = str(input(question+' (y/n): ')).lower().strip()
        if reply in ['y','yes']:
            return True
        if reply in ['n','no']:
            return False


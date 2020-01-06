#!/usr/bin/env python
from xml.etree.ElementTree import tostring
from xml.dom import minidom
import re
from abc import ABC, abstractmethod


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


def validate_cycle_def(cycdef):
    start_stop_step = re.compile(r'\d{12} \d{12} \d{2}:\d{2}:\d{2}')
    crude_cron_syntax = re.compile(r'(.+\s+){5}.')

    if start_stop_step.search(cycdef):
        return True
    elif crude_cron_syntax.search(cycdef):
        return True
    else:
        return False


#def yes_or_no(question):
#    while "not y or n response":
#        reply = str(input(question+' (y/n): ')).lower().strip()
#        if reply in ['y','yes']:
#            return True
#        if reply in ['n','no']:
#            return False


class Validator(ABC):
    def __set_name__(self, owner, name):
        self.private_name = f'_{name}'

    def __get__(self, obj, objtype=None):
        return getattr(obj, self.private_name)

    def get_name(self):
        ''' can be used by subclass with super().get_name() to discover 
            the private name without _ ; helpfull for raising informative errors'''
        return self.private_name.strip('_')

    def __set__(self, obj, value):
        v = self.validate(value)
        if v is not None: value = v
        setattr(obj, self.private_name, value)
        if not hasattr(obj, '_validated'):
            obj._validated = []
        if self.private_name not in obj._validated:
            obj._validated.append(self.private_name)

    @abstractmethod
    def validate(self, value):
        ''' validate method can accept (null return), augment (return augmented)
        or raise an error'''
        pass

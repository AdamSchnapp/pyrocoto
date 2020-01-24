#!/usr/bin/env python
from xml.etree.ElementTree import Element, tostring
from xml.dom import minidom
from .helpers import  Validator
#from collections import OrderedDict
from itertools import product
#import inspect
#import copy
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class String(Validator):                                                                            
    def __init__(self, contains=None):
        self.isin = contains

    def validate(self, value):
        if isinstance(value, Offset):
            return # offset objects are strings with additional offset
        name = super().get_name()
        if not isinstance(value, str):
            raise TypeError(f'Expected "{name}" value {value!r} to be a string')

        if self.isin is not None:
            if self.isin not in value:
                raise ValueError(f'Expected {self.isin} in "{name}" value {repr(value)}')


class Offset:
    ''' entry that should recieve time offset '''
    offset = String()
    value = String(contains='@')

    def __init__(self, value, offset):
        self.offset = offset
        self.value = value

    def to_element(self, name):
        E = Element(name)
        Esub = Element('cyclestr', offset=self.offset)
        Esub.text = self.value
        E.append(Esub)
        return E
         

class Envar(Validator):
    def __init__(self, contains=None):
        self.isin = contains

    def validate(self, value):
        ''' stores xml from dict input '''
        if not isinstance(value, dict):
            raise TypeError(f'Expected envar value {value!r} to be a dictionary')
        envars = []
        for name, v in value.items():
            envar = Element('envar')
            name_element = Element('name')
            name_element.text = name
            envar.append(name_element)
            if isinstance(v, str):
                value_element = Element('value')
                value_element.text = v
                value_element = cyclestr(value_element)
            else:
                value_element = to_element(v, 'value')
            envar.append(value_element)
            envars.append(envar)
            
        return envars



class Meta(Validator):
    def __init__(self, contains=None):
        self.isin = contains

    def validate(self, value):
        if not isinstance(value, dict):
            raise TypeError(f'Expected meta value {value!r} to be a dictionary')
        for v in value.values():
            if not isinstance(v, str):
                raise TypeError(f'Expected to find string values in meta dict, \
                                  but found {repr(v)}')

class Cycledefs(Validator):
    def __init__(self, contains=None):
        self.isin = contains

    def validate(self, value):
        ''' return list of cycledefs if not already '''
        if isinstance(value,  CycleDefinition):
            return([value])
        if isinstance(value, list):
            for i in value:
                if not isinstance(i, CycleDefinition):
                    msg = f'Expected CycleDefinition, but got {type(i)}'
                    raise TypeError(msg)
        if not isinstance(value, list):
            raise TypeError(f'Expected Cycledefs value {value!r} to' \
                    'be CycleDefinition or list of CycleDefinitions\n') 



class CycleDefinition():
    # add logic to verify user provides a valid definition
    def __init__(self, group, definition, activation_offset=None):
        self.group = str(group)
        self.definition = str(definition)
        self.activation_offset = str(activation_offset)

    def __repr__(self):
        return "CycleDefinition({!r})".format(self.__dict__) 

    def __eq__(self, other):
        if isinstance(other, CycleDefinition):
            if self.group == other.group:
                return True
            else:
                return (self.definition == other.definition and
                        self.activation_offset == other.activation_offset)
        else:
            return False

    def __hash__(self):
        return hash(self.group + self.definition + self.activation_offset)
    
    def generate_xml(self):
        cycledef_element = Element('cycledef', group=self.group)
        cycledef_element.text = self.definition
        if self.activation_offset != 'None':
            cycledef_element.attrib['activation_offset'] = self.activation_offset
        return cycledef_element



def cyclestr(element):
    ''' Wrap text elements containing '@' for syclestr information with cyclestr tag.
        Elements that do not contain '@' are returned unchanged'''
    if not isinstance(element, Element):
        raise TypeError('element passed must be of type Element')
    if element.text is None:
        raise ValueError('passed element does not have text')
    if '@' in element.text:
        text = element.text
        element.text = None
        cyclestr_element = Element('cyclestr')
        cyclestr_element.text = text
        element.append(cyclestr_element)
    return element



class Workflow(object):
    ''' Implement an abstarction layer on top of rocoto workflow management engine
        The WorkFlow class will serve as a central object that registers all units of work
        (tasks) for any number of desired cycle definitions.
    '''

    def __init__(self, realtime='T', scheduler='lsf', **kwargs):
        self.tasks = []
        self.cycle_definitions = set()

        self.workflow_element = Element('workflow', realtime=realtime,
                                        scheduler=scheduler, **kwargs)
        self.log_element = None

    def define_cycle(self, group, definition, activation_offset=None):
        cycledef = CycleDefinition(group, definition, activation_offset)
        self.cycle_definitions.add(cycledef)
        return cycledef

    def set_log(self, logfile):
        log = Element('log')
        log.text = logfile
        self.log_element = cyclestr(log)

    def add_task(self, task):
        task.validate() # will raise error if eggregate of task info appears to have issues
        for cycledef in task.cycledefs:
            self.cycle_definitions.add(cycledef)
        self.tasks.append(task)

    def task(self):
        ''' decorator used to associate tasks with workflow
            Use to wrap functions that will return task object

        @flow.task()
        def task():
            namespace for defining task
            return Task(locals())
        '''
        def decorator(func):
            task = func()
            self.add_task(task)
            logger.info(f'adding task {task.name}')
        return decorator


    @staticmethod
    def prettify(elem):
        rough_string = tostring(elem, 'UTF-8')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="    ", encoding=None)

    def write_xml(self, xmlfile, cycledefs=None):
        ''' write xml workflow.
        '''

        xml = self.workflow_element
        xml.append(self.log_element)

        for cycledef in self.cycle_definitions:
            E = cycledef.generate_xml()
            xml.append(E)

        for task in self.tasks:
            E = task.generate_xml()
            xml.append(E)

        print(self.workflow_element)
        with open(xmlfile, 'w') as f:
            f.write('<?xml version="1.0"?>\n<!DOCTYPE workflow []>')
            f.write(self.prettify(self.workflow_element)[22:])


class Task: 
    ''' Implement container for information pertaining to a single task '''
    # validate and track class meta data
    # note: validated data attributes are added to self._validated by the validators
    name = String() # tasks added to workflow should have unique name
    jobname = String()
    command = String(contains = '/')
    join = String(contains = '/')
    stderr = String(contains = '/')
    account = String()
    memory = String() # maybe validate this more
    walltime = String(contains = ':')
    maxtries = String()
    queue = String()
    native = String()
    cores = String()
    envar = Envar()
    meta = Meta()
    cycledefs = Cycledefs()
#   dependency = Dependency()

    defaults = {'maxtries':'2',
                'walltime':'20:00'}

    # Specify required metadata, multiple entries indicates atleast one of is required
    # I.E atleast one of 'join' or 'stderr' is required
    _required = [['name'],
                 ['command'],
                 ['join', 'stderr'],
                 ['cores', 'nodes'],
                 ['cycledefs']]
    # All metadata that is _for_xml should be validated and stored as 
    # a string, Element or an object that has method as_element
    _for_xml = ['jobname',
               'command',
               'join',
               'stderr',
               'account'
               'memory',
               'walltime',
               'cores',
               'nodes',
               'native',
               'memory',
               'envar',
               'dependency']


    def __init__(self, d):
        # set some defaults if not already set
        for k, v in self.defaults.items()
        if not hasattr(self, k):
            setattr(self, k, v)
        # set user passed data that will overwrite any defaults
        for var, value in d.items():
            setattr(self, var, value)

    def validate(self):
        # ensure that metadata that should be diffrent by job is.
        # jobname, join/stderr,
        # meta keys should be specified when meta and
        # ensure the agregate of data for this task looks ok
        # check here for common mistakes as found only if they are based on a combination of data pieces
        # single data validation should occur within a validator
        for req_attrs in self._required:
            good = False
            for attr in req_attrs:
                if hasattr(self,attr):
                    good = True
                    break
            if not good:
                raise ValueError(f'Expected one of {repr(req_attrs)} to be set')

    def generate_xml(self):
        ''' Convert task's metadata into a Task rocoto XML element '''
        task_attrs = {'name': self.name,
                'cycledefs': ','.join([x.group for x in self.cycledefs]),
                'maxtries': self.maxtries}
#        task_attrs['name']
        elm_task = Element('task', task_attrs)
        for attr in self._for_xml:
            ''' metadata will be string, list, or accomodated by to_element function '''
            if hasattr(self, attr):
                print(attr)
                V = getattr(self, attr)
                Ename = attr.strip('_')
                if isinstance(V, str):
                    E = Element(Ename)
                    E.text = V
                    E = cyclestr(E)
                    elm_task.append(E)
                elif isinstance(V, list):
                    elm_task.extend(V)
                else: 
                    elm_task.append(to_element(V,Ename))
        if hasattr(self, 'meta'):
            E_metatask  = Element('metatask')
            for k, v in self.meta.items():
                print(k,v)
                E = Element('var', name=k)
                E.text = v
                E_metatask.append(E)
            E_metatask.append(elm_task)
            elm_task = E_metatask

        return elm_task

def to_element(obj, name):
    if hasattr(obj, 'to_element'):
        print(f"{name} has attr to_element")
        return obj.to_element(name)
    elif isinstance(obj, list):
        # lists are assumed to be lists of elements
        E = Element()
        E.extend(obj)
        return E

class Dependency():
    def __init__(self, deptype, data=None, offset=None, task=None, metatask=None, sh=None, **kwargs):
        deptypes = set(['taskdep', 'datadep', 'timedep', 'metataskdep', 'sh'])
        if deptype not in deptypes:
            raise ValueError(f"deptype is {deptype}, but expected one of {deptypes}")
        if deptype == 'taskdep':
            if task is None:
                raise ValueError()
            self.elm = Element(deptype, task=task, **kwargs)
            # task dependency allows for cycle_offset and state attribute.
            # these can be passed as kwargs
        elif deptype == 'datadep':
            if data is None:
                raise
            self.elm = Element(deptype, **kwargs)
            self.elm.text = data
            if offset is not None:
                self.elm = cyclestr(self.elm)
            else:
                self.elm = cyclestr(self.elm)
        elif deptype == 'timedep':
            if offset is None:
                raise
            elm = Element(deptype)
            elm.text = '@Y@m@d@H@M@S'
            self.elm = cyclestr(elm)
        elif deptype == 'sh':
            if sh is None:
                raise
            elm = Element(deptype)
            elm.text = sh
            self.elm = elm
        elif deptype == 'metataskdep':
            if metatask is None:
                raise ValueError()
            self.elm = Element(deptype, metatask=metatask, **kwargs)
            # metatask dependency allows for cycle_offset, state, and threshold attribute.
            # these can be passed as kwargs

    @staticmethod
    def operator(oper, *args):
        elm = Element(oper)
        for dep in args:
            if isinstance(dep,Element):
                add_elm = dep
            else:
                add_elm = dep.elm
            elm.append(add_elm)
        return elm

def product_meta(dict):
    new_dict = {}
    keys = [k for k in dict.keys()]
    l = [v.split(' ') for v in dict.values()]

    iter = product(*l)
    l = [list(i) for i in iter]
    l = [list(x) for x in zip(*l)] # reshape
    l = [" ".join(x) for x in l]

    for k,v in zip(keys,l):
        new_dict[k]=v

    return new_dict


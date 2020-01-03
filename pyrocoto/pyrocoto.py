#!/usr/bin/env python
from xml.etree.ElementTree import Element, tostring
from xml.dom import minidom
from .helpers import _name_of_func, groupattr, modify_tree_nodes, yes_or_no, Validator
from collections import OrderedDict
import inspect
import copy
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class String(Validator):
    def __init__(self, contains=None):
        self.isin = contains

    def validate(self, value):
        name = super().get_name()
        if not isinstance(value, str):
            raise TypeError(f'Expected "{name}" value {value!r} to be a string')

        if self.isin is not None:
            if self.isin not in value:
                raise ValueError(f'Expected {self.isin} in "{name}" value {repr(value)}')

#class Groups(Validator):
#    def __init__(self):
#        pass
#
#    def validate(self, value):
#        msg = f'Expected groups value {value!r} to be string or list of strings' 
#        if not isinstance(value, list) and not isinstance(value, str):
#            raise TypeError(msg)
#        if isinstance(value, list):
#            for item in value:
#                if not isinstance(item, str):
#                    raise TypeError(msg)

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
            value_element = Element('value')
            if isinstance(v, tuple):
                value_element.text = v[0]
                offset = value[1]
            else:                  
                value_element.text = v
                offset = None
                value_element = cyclestr(value_element, offset)
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



def cyclestr(element, offset=None):
    ''' Wrap text elements containing '@' for syclestr information with cyclestr tag '''
    if not isinstance(element, Element):
        raise ValueError('element passed must be of type Element')
    if element.text is None:
        raise ValueError('passed element does not have text')
    if '@' in element.text:
        text = element.text
        element.text = None
        if offset is not None:
            if not isinstance(offset, str):
                raise ValueError('offset passed must be of type str but was '+str(type(offset)))
            cyclestr_element = Element('cyclestr', offset=offset)
        else:
            cyclestr_element = Element('cyclestr')
        cyclestr_element.text = text
        element.append(cyclestr_element)
    else:
        if offset is not None:
            raise ValueError("offset was passed but no '@' in element.text")
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
        def taskname(task_group):
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

    def write_xml(self, xmlfile, groups=None, cycledefs=None):
        ''' write xml workflow.
            Groups of tasks or tasks based on specific cycle definitions can
            be spacifically written.
            By default all registerd tasks and cycle definitions are written.
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

#class TaskBasic():
#    ''' Implement container for information pertaining to a single task '''

class Task: 
    """ write something useful """
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


    _required = [['name'],
                 ['command'],
                 ['join', 'stderr'],
                 ['cores', 'nodes'],
                 ['cycledefs']]
    # all metadata that is _for_xml should be validated and stored as 
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
        # set some defaults
        self.maxtries = '1'
        # set user passed data that will overwrite any default
        for var, value in d.items():
            setattr(self, var, value)

    def validate(self):
        # ensure that metadata that should be diffrent by job is.
        # jobname, join/stderr,
        # meta keys should be specified when meta and
        # :group: should be specified when groups
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
        task_attrs = {'name': self.name,
                'cycledefs': [x.group for x in self.cycledefs],
                'maxtries': self.maxtries}
#        task_attrs['name']
        elm_task = Element('task', task_attrs)
        for attr in self._for_xml:
            if hasattr(self,attr):
                if getattr(self,attr) is not Element:
                    if attr.strip('_') == 'envar': 
                        elm_task.extend(getattr(self,attr))
                    else:
                        E = Element(attr.strip('_'))
                        E.text = getattr(self,attr)
                        elm_task.append(E)
                else:
                    elm_task.append(getattr(self,attr))

        return elm_task


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
                self.elm = cyclestr(self.elm, offset=offset, **kwargs)
            else:
                self.elm = cyclestr(self.elm, **kwargs)
        elif deptype == 'timedep':
            if offset is None:
                raise
            elm = Element(deptype)
            elm.text = '@Y@m@d@H@M@S'
            self.elm = cyclestr(elm, offset=offset, **kwargs)
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
            try:
                add_elm = dep.elm
            except:
                add_elm = dep
            elm.append(add_elm)
        return elm

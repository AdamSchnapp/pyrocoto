#!/usr/bin/env python
from xml.etree.ElementTree import Element, tostring
from xml.dom import minidom
from .helpers import Validator
from itertools import product
import logging

logger = logging.getLogger(__name__)


class String(Validator):
    def __init__(self, contains=None):
        self.isin = contains

    def validate(self, value):
        if isinstance(value, Offset):
            return  # offset objects are strings with additional offset
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

    def to_element(self, name, **kwargs):
        E = Element(name, kwargs)
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
                value_element = _cyclestr(value_element)
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


class XmlElement(Validator):
    def __init__(self):
        pass

    def validate(self, value):
        if not isinstance(value, Element):
            raise(TypeError(f'Expected Element but got {type(value)}'))


class Dependency():

    elm = XmlElement()

    def __init__(self, elm):
        self.elm = elm

    @staticmethod
    def operator(oper, *args):
        ''' Return new dependency wrapped in an operator tag; the operator is not validated'''
        if len(args) < 2:
            raise TypeError(f'Expected atleast two args, but got {len(args)},{args}')
        for arg in args:
            if not isinstance(arg, Dependency):
                raise TypeError(f'Expected Dependency but got {type(arg)},{arg}')
        elm = Element(oper)
        for arg in args:
            elm.append(arg.elm)
        return Dependency(elm)

    def to_element(self, name='dependency'):
        E = Element(name)
        E.append(self.elm)
        return E


class IsDependency(Validator):

    def __init__(self):
        pass

    def validate(self, value):
        if not isinstance(value, Dependency):
            name = super().get_name()
            raise TypeError(f'Expected "{name}" value {value!r} to be a Dependency')


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
            raise TypeError(f'Expected Cycledefs value {value!r} to'
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
        return hash(self.group)

    def _generate_xml(self):
        cycledef_element = Element('cycledef', group=self.group)
        cycledef_element.text = self.definition
        if self.activation_offset != 'None':
            cycledef_element.attrib['activation_offset'] = self.activation_offset
        return cycledef_element


def _cyclestr(element):
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
        self.task_names = set()  # set of unique task names, metatasks are expended.
        self.metatask_names = set()
        self.cycle_definitions = []  # this is a list so that iteration is deterministic

        self.workflow_element = Element('workflow', realtime=realtime,
                                        scheduler=scheduler, **kwargs)
        self.log_element = None

    def define_cycle(self, group, definition, activation_offset=None):
        cycledef = CycleDefinition(group, definition, activation_offset)
        print(f'adding cycle {group}')
        if cycledef in self.cycle_definitions:
            raise ValueError('cannot add cycle with same group name')
        self.cycle_definitions.append(cycledef)
        return cycledef

    def set_log(self, logfile):
        log = Element('log')
        log.text = logfile
        self.log_element = _cyclestr(log)

    def _validate_task_dependencies(self, task):
        if hasattr(task, 'dependency'):
            for elm in task.dependency.to_element().iter():
                if elm.tag == 'taskdep':
                    if elm.attrib['task'] not in self.task_names:
                        n = elm.attrib['task']
                        raise ValueError(f'Task depenency {repr(n)} is not in workflow')
                if elm.tag == 'metataskdep':
                    if elm.attrib['metatask'] not in self.metatask_names:
                        n = elm.attrib['metatask']
                        raise ValueError(f'Metatask dependency {repr(n)} is not in workflowa')

    def add_task(self, task):
        task._validate()  # will raise error if eggregate of task info appears to have issues
        self._validate_task_dependencies(task)  # will raise errors if task dependency issues
        for cycledef in task.cycledefs:
            if cycledef not in self.cycle_definitions:
                self.cycle_definitions.append(cycledef)
        self.tasks.append(task)
        if not self.task_names.isdisjoint(task.task_names):  # if intersection
            raise ValueError(f'Task names must be unique; Error adding task {repr(task.name)}')
        else:
            self.task_names.update(task.task_names)
        if hasattr(task, 'metatask_name'):
            self.metatask_names.add(task.metatask_name)

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
            logger.info(f'adding task {repr(task.name)}')
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
            E = cycledef._generate_xml()
            xml.append(E)

        for task in self.tasks:
            E = task._generate_xml()
            xml.append(E)

        with open(xmlfile, 'w') as f:
            f.write('<?xml version="1.0"?>\n<!DOCTYPE workflow []>')
            f.write(self.prettify(self.workflow_element)[22:])


class Task:
    ''' Implement container for information pertaining to a single task '''
    # validate and track class meta data
    # note: validated data attributes are added to self._validated by the validators
    name = String()  # tasks added to workflow should have unique name
    metatask_name = String()
    jobname = String()
    command = String(contains='/')
    join = String(contains='/')
    stderr = String(contains='/')
    account = String()
    memory = String()  # maybe validate this more
    walltime = String(contains=':')
    maxtries = String()
    queue = String()
    partition = String()
    native = String()
    cores = String()
    envar = Envar()
    meta = Meta()
    cycledefs = Cycledefs()
    dependency = IsDependency()

    defaults = {'maxtries': '2',
                'walltime': '20:00'}

    # Specify required metadata, multiple entries indicates atleast one of is required
    # I.E atleast one of 'join' or 'stderr' is required
    _required = [['name'],
                 ['command'],
                 ['join', 'stderr'],
                 ['cores', 'nodes'],
                 ['cycledefs'],
                 ['queue'],
                 ['account']]
    # All metadata that is _for_xml should be validated and stored as
    # a string, Element or an object that has method as_element
    _for_xml = ['jobname',
                'command',
                'join',
                'stderr',
                'stdout',
                'account',
                'queue',
                'partition',
                'walltime',
                'cores',
                'nodes',
                'native',
                'memory',
                'envar',
                'dependency']

    def __init__(self, d):
        # set some defaults if not already set
        for k, v in self.defaults.items():
            if not hasattr(self, k):
                setattr(self, k, v)
        # set user passed data that will overwrite any defaults
        self.task_names = set()
        for var, value in d.items():
            setattr(self, var, value)

    def _validate(self):
        # ensure that metadata that should be diffrent by job is.
        # jobname, join/stderr,
        # meta keys should be specified when meta and
        # ensure the agregate of data for this task looks ok
        # check for common mistakes that are based on a combination of data
        # single data validation should occur within a validator
        for req_attrs in self._required:
            good = False
            for attr in req_attrs:
                if hasattr(self, attr):
                    good = True
                    break
            if not good:
                raise ValueError(f'Expected one of {repr(req_attrs)} to be set')
        # store task name(s) in self.task_names
        if not hasattr(self, 'meta'):
            self.task_names.add(self.name)
        else:
            # check that all vars are same length and convert to dict of lists
            for v in self.meta.values():
                ntasks = len(v.split())
                break
            meta_with_lists = {}
            for k, v in self.meta.items():
                as_list = v.split()
                if len(as_list) != ntasks:
                    raise ValueError('meta vars not all equal length')
                meta_with_lists[k] = as_list
            for ix in range(ntasks):
                n = self.name
                for key in meta_with_lists.keys():
                    n = n.replace(f'#{key}#', meta_with_lists[key][ix])
                if n not in self.task_names:
                    self.task_names.add(n)
                else:
                    raise ValueError('meta variables must produce unique tasks')

    def _generate_xml(self):
        ''' Convert task's metadata into a Task rocoto XML element '''
        task_attrs = {'name': self.name,
                      'cycledefs': ','.join([x.group for x in self.cycledefs]),
                      'maxtries': self.maxtries}
#        task_attrs['name']
        elm_task = Element('task', task_attrs)
        for attr in self._for_xml:
            ''' metadata will be string, list, or accomodated by to_element function '''
            if hasattr(self, attr):
                V = getattr(self, attr)
                Ename = attr.strip('_')
                if isinstance(V, str):
                    E = Element(Ename)
                    E.text = V
                    E = _cyclestr(E)
                    elm_task.append(E)
                elif isinstance(V, list):
                    elm_task.extend(V)
                else:
                    elm_task.append(to_element(V, Ename))
        if hasattr(self, 'meta'):
            if hasattr(self, 'metatask_name'):
                E_metatask = Element('metatask', name=self.metatask_name)
            else:
                E_metatask = Element('metatask')
            for k, v in self.meta.items():
                E = Element('var', name=k)
                E.text = v
                E_metatask.append(E)
            E_metatask.append(elm_task)
            elm_task = E_metatask

        return elm_task


def to_element(obj, name):
    if hasattr(obj, 'to_element'):
        return obj.to_element(name)
    elif isinstance(obj, list):
        # lists are assumed to be lists of elements
        E = Element()
        E.extend(obj)
        return E


class DataDep(Dependency):
    def __init__(self, data, age=None, minsize=None):
        if not isinstance(data, str) and not isinstance(data, Offset):
            raise TypeError(f'Expected data to be type str or Offset, but was {type(data)}')
        E_attrs = {}
        if isinstance(age, str):
            E_attrs['age'] = age
        if isinstance(minsize, str):
            E_attrs['minsize'] = minsize
        if isinstance(data, Offset):
            E = data.to_element('datadep', **E_attrs)
        else:
            E = Element('datadep', E_attrs)
            E.text = data
            E = _cyclestr(E)
        self.elm = E


class TaskDep(Dependency):
    def __init__(self, task, cycle_offset=None, state=None):
        if not isinstance(task, str):
            raise TypeError(f'Expected data to be type str, but was {type(task)}')
        E_attrs = {}
        E_attrs['task'] = task
        if isinstance(cycle_offset, str):
            E_attrs['cycle_offset'] = cycle_offset
        if isinstance(state, str):
            E_attrs['state'] = state
        E = Element('taskdep', E_attrs)
        self.elm = E


class MetaTaskDep(Dependency):
    def __init__(self, metatask, cycle_offset=None, state=None, threshold=None):
        if not isinstance(metatask, str):
            raise TypeError(f'Expected metatask to be type str, but was {type(metatask)}')
        E_attrs = {}
        E_attrs['metatask'] = metatask
        if isinstance(cycle_offset, str):
            E_attrs['cycle_offset'] = cycle_offset
        if isinstance(state, str):
            E_attrs['state'] = state
        if isinstance(threshold, str):
            E_attrs['threshold'] = threshold
        E = Element('metataskdep', E_attrs)
        self.elm = E


class TimeDep(Dependency):
    def __init__(self, time):
        if not isinstance(time, str) and not isinstance(time, Offset):
            raise TypeError(f'Expected time to be type str or Offset, but was {type(time)}')
        if isinstance(time, Offset):
            E = time.to_element('timedep')
        else:
            E = Element('timedep')
            E.text = time
            E = _cyclestr(E)
        self.elm = E


class TagDep(Dependency):
    ''' provide mechanism for user to specify the tag 'sh' or 'rb'
        and the text for the tag; User must provide cyclstr tags in text if they need them '''
    def __init__(self, tag, text):
        E = Element(tag)
        E.text = text
        self.elm = E
        

def product_meta(dict_in):
    if not isinstance(dict_in, dict):
        raise TypeError(f'Expected dict, but got {type(dict_in)}')
    new_dict = {}
    keys = [k for k in dict_in.keys()]
    values_as_list_of_lists = [v.split(' ') for v in dict_in.values()]
    prodicized = product(*values_as_list_of_lists)
    combinations_list_of_lists = [list(i) for i in prodicized]
    new_values_as_list_of_lists = [list(x) for x in zip(*combinations_list_of_lists)]  # reshape
    new_values_as_list = [" ".join(x) for x in new_values_as_list_of_lists]

    for k, v in zip(keys, new_values_as_list):
        new_dict[k] = v

    return new_dict

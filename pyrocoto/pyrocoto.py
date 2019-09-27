#!/usr/bin/env python
from xml.etree.ElementTree import Element, tostring
from xml.dom import minidom
from .helpers import _name_of_func, groupattr, modify_tree_nodes
from collections import OrderedDict
import inspect
import copy
import logging

logger = logging.getLogger(__name__)


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
            return (self.group == other.group and
                    self.definition == other.definition and
                    self.activation_offset == other.activation_offset)
        else:
            return False

    def __hash__(self):
        return hash(self.group + self.definition + self.activation_offset)


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


class Singleton(type):
    def __init__(cls, name, bases, dic):
        super(Singleton, cls).__init__(name, bases, dic)
        cls.instance = None

    def __call__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls.instance


class Workflow(object):
    __metaclass__ = Singleton
    ''' Implement an abstarction layer on top of rocoto workflow management engine
        The WorkFlow class will serve as a central object that registers all units of work
        (tasks) for any number of desired cycle definitions.
    '''

    def __init__(self, **kwargs):
        self.tasks = OrderedDict()
        self.task_data = []
        self.cycle_definitions = set()
        self.cycle_elements = []
        self.task_elements = []

        try:
            realtime = kwargs["realtime"]
            del kwargs["realtime"]
        except:
            realtime = 'T'
        try:
            scheduler = kwargs["scheduler"]
            del kwargs["scheduler"]
        except:
            scheduler = 'lsf'

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

    def register_task(self, groups=None, task_name=None, task=None,
                      meta=None, metaname=None):
        ''' register tasks in the workflows task registry'''

        if task is not None:
            task.verify()
            task.meta_dict = meta
            task.metaname = metaname

            if groups is not None:
                for group in groups:
                    task.apply_group(group)
                    task.task_group = group
                    task_unique_name = group + '_' + task_name
                    self.tasks[task_unique_name] = copy.deepcopy(vars(task))
            else:
                self.tasks[task_name] = vars(task)
                task.task_group = None

    def task(self, groups=None, meta=None, **options):
        ''' a decorator used to register task functions

        @flow.task
        def taskname(task_group):
            namespace for defining task
            return Task(var())
        '''

        def decorator(func):
            task = func()
            metaname = None
            try:
                task_name = task['__taskname__']
                if meta is not None:
                    metaname = _name_of_func(func)
            except:
                # let function name be task name
                task_name = _name_of_func(func)
                task.__taskname__ = task_name
                if meta is not None:
                    print('__taskname__ must be used with a metatask')
                    raise ValueError

            self.register_task(groups, task_name, task, meta, metaname)
            return func

        return decorator

    @staticmethod
    def prettify(elem):
        rough_string = tostring(elem, 'UTF-8')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="    ", encoding=None)

    def write_xml(self, xmlfile, groups=None, cycledefs=None):
        ''' Validate that registered workflow tasks and cycles are valid and
            write xml workflow.
            Groups of tasks or tasks based on specific cycle definitions can
            be spacifically written.
            By default all registerd tasks and cycle definitions are written.
        '''

        # Construct cycle definition XML elements
        self.cycle_groups = set()
        for cycledef in self.cycle_definitions:
            if cycledef.group in self.cycle_groups:
                raise ValueError('duplicate cycle group defined')
            self.cycle_groups.add(cycledef.group)
            cycledef_element = Element('cycledef', group=cycledef.group)
            if cycledef.activation_offset != 'None':
                cycledef_element.attrib['activation_offset'] = cycledef.activation_offset
            cycledef_element.text = cycledef.definition
            self.cycle_elements.append(cycledef_element)

        # Evaluate task functions, verify tasks have a cycle definition
        # Store pertinent tasks in task_data

        for taskname, task in self.tasks.items():

            if groups is not None and task['task_group'] not in groups:
                continue

            task['task_unique_name'] = taskname

            for cycledef in task['cycledefs']:
                try:
                    cycle_group = cycledef.group
                except:
                    cycle_group = cycledef
                if cycle_group not in self.cycle_groups:
                    raise ValueError('cycle group {} not defined'.format(cycle_group))

            task['cycledefs'] = [groupattr(x) for x in task['cycledefs']]

            if cycledefs is not None:
                cycledefs = [groupattr(x) for x in cycledefs]
                task['cycledefs'] = list(set(task['cycledefs']) & set(cycledefs))
            self.task_data.append(task)

        # Perform checks on task_data and prep for writing XML
        for ix, task in enumerate(self.task_data):
            self.task_data[ix]['xml_elements'] = []
            for key in task.keys():

                ''' Ignore data not directly relevant to rocoto xml workflow '''
                if key not in ['command', 'account', 'queue', 'cores', 'nodes',
                               'walltime', 'envar', 'native', 'memory', 'jobname',
                               'join', 'stdout', 'stderr', 'deadline', 'dependency']:
                    continue

                ''' handle optional invironament variables (envar) '''
                if key == 'envar':

                    for name, value in task[key].items():
                        envar = Element('envar')
                        name_element = Element('name')
                        name_element.text = name
                        envar.append(name_element)
                        value_element = Element('value')
                        if isinstance(value, tuple):
                            value_element.text = value[0]
                            offset = value[1]
                        else:
                            value_element.text = value
                            offset = None
                        value_element = cyclestr(value_element, offset)
                        envar.append(value_element)
                        self.task_data[ix]['xml_elements'].append(envar)

                # handle optional dependencies/triggers for the task
                elif key == 'dependency':
                    try:
                        task['dependency'] = task['dependency'].elm
                    except:
                        pass
                    for dep in task['dependency'].iter():
                        if dep.tag == 'taskdep':
                            valid_task_dep_name = False
                            if task['task_group'] is not None:
                                if dep.attrib['task'].startswith('!'):
                                    dep.attrib['task'] = dep.attrib['task'][1:]
                                else:
                                    dep.attrib['task'] = task['task_group'] + '_' + dep.attrib['task']
                            for i, data in enumerate(self.task_data):
                                if (data['task_unique_name'] == dep.attrib['task']):
                                    if i > ix:
                                        print('task {} has dependencies which follow it'.format(data['name']))
                                        print('review the order of imported tasks')
                                        raise ValueError('dependent tasks not properly ordered')
                                    valid_task_dep_name = True

                                    break
                            if not valid_task_dep_name:
                                print(dep.attrib['task'], ' is not a known task, continuing')
                                # raise ValueError

                    deptag = Element('dependency')
                    deptag.append(task[key])

                    self.task_data[ix]['dependency'] = deptag

                #  handle remaining workflow tags provided by user
                else:
                    elm = Element(key)
                    elm.text = task[key]
                    self.task_data[ix]['xml_elements'].append(cyclestr(elm))

            ''' handle craetion of task XML elements '''
            task_attributes = {}
            task_attributes['name'] = self.task_data[ix]['task_unique_name']
            task_attributes['cycledefs'] = ' '.join(self.task_data[ix]['cycledefs'])
            try:
                task_attributes['maxtries'] = self.task_data[ix]['maxtries']
            except:
                pass
            try:
                task_attributes['throttle'] = self.task_data[ix]['throttle']
            except:
                pass
            self.task_data[ix]['xml_task'] = Element('task', task_attributes)

            self.task_data[ix]['xml_task'].extend(self.task_data[ix]['xml_elements'])
            try:
                self.task_data[ix]['xml_task'].append(self.task_data[ix]['dependency'])
            except:
                pass

            # handle meta task
            if self.task_data[ix]['meta_dict'] is not None:
                metaelm = Element('metatask', name=self.task_data[ix]['metaname'])
                for var, metavars in self.task_data[ix]['meta_dict'].items():
                    elm = Element('var', name=var)
                    elm.text = metavars
                    metaelm.append(elm)
                self.task_data[ix]['vars'] = metaelm
                self.task_data[ix]['vars'].append(self.task_data[ix]['xml_task'])
                self.task_data[ix]['xml_task'] = self.task_data[ix]['vars']
            # print(self.task_data[ix]['xml_task'])
            self.task_elements.append(self.task_data[ix]['xml_task'])
            print('added task {}'.format(self.task_data[ix]['task_unique_name']))

        self.workflow_element.extend(self.cycle_elements)
        self.workflow_element.append(self.log_element)
        self.workflow_element.extend(self.task_elements)

        print(self.workflow_element)
        with open(xmlfile, 'w') as f:
            f.write('<?xml version="1.0"?>\n<!DOCTYPE workflow []>')
            f.write(self.prettify(self.workflow_element)[22:])


class Task():
    ''' Implement container for information pertaining to a single task '''

    def __init__(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)
        self.name = inspect.stack()[1][3]
        self.account = getattr(self, 'account', 'MDLST-T2O')
        self.walltime = getattr(self, 'walltime', '00:20:00')
        self.maxtries = getattr(self, 'maxtries', '1')
        self.queue = getattr(self, 'queue', 'dev2_shared')
        if self.queue in ['dev2_shared', 'transfer']:
            self.memory = getattr(self, 'memory', '2056M')
            self.native = getattr(self, 'native', '-R affinity[core]')
            self.cores = getattr(self, 'cores', '1')
        try:
            self.jobname = getattr(self, 'jobname', self.__taskname__ + '_@Y@m@d@H')
        except:
            self.jobname = getattr(self, 'jobname', self.name + '_@Y@m@d@H')

    def __repr__(self):
        return '%s' % vars(self)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, val):
        return setattr(self, key, val)

    def apply_group(self, group):
        for key, obj in vars(self).items():
            # def f(x):
            #     return x.replace(':group:', group)
            f = lambda x: x.replace(':group:', group)
            obj = modify_tree_nodes(obj, action_func=f)
            self[key] = obj

    def verify(self):
        required_keys = [['command'], ['join', 'stderr'],
                         ['cores', 'nodes'], ['cycledefs']]
        keys = self.__dict__.keys()
        for reqkey in required_keys:
            if bool(set(reqkey) & set(keys)):
                continue
            print('missing requred inputs ' + str(reqkey))
            raise ValueError('required inputs missing')


class Dependency():
    def __init__(self, deptype, data=None, offset=None, task=None, metatask=None, sh=None, **kwargs):
        if deptype not in ['taskdep', 'datadep', 'timedep', 'metataskdep', 'sh']:
            raise
        if deptype == 'taskdep':
            if task is None:
                raise
            try:
                self.elm = Element(deptype, task=task.__name__)
            except:
                self.elm = Element(deptype, task=task, **kwargs)

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
                raise
            try:
                self.elm = Element(deptype, metatask=metatask.__name__)
            except:
                self.elm = Element(deptype, metatask=metatask, **kwargs)

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

    pass

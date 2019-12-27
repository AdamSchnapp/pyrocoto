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
        if not isinstance(value, str):
            raise TypeError(f'Expected value {value!r} to be a string')

        if self.isin is not None:
            if self.isin not in value:
                raise ValueError(f'Expected {self.isin} in value {repr(value)}')

class Groups(Validator):
    def __init__(self):
        pass

    def validate(self, value):
        msg = f'Expected groups value {value!r} to be string or list of strings' 
        if not isinstance(value, list) and not isinstance(value, str):
            raise TypeError(msg)
        if isinstance(value, list):
            for item in value:
                if not isinstance(item, str):
                    raise TypeError(msg)

class Envar(Validator):
    def __init__(self, contains=None):
        self.isin = contains

    def validate(self, value):
        if not isinstance(value, dict):
            raise TypeError(f'Expected Envar value {value!r} to be a dictionary')
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
                print(tostring(value_element))
                offset = None
                value_element = cyclestr(value_element, offset)
                envar.append(value_element)
            envars.append(envar)
            
        return envars

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


#class Config():
#    '''Implement configuration creation and access'''
#    def __init__(self):
#        '''Use configuaration file in current directory over home/config/'''
#            
#        config_here = os.path.join(Path.cwd(),'pyrocoto.yaml')
#        config_home = os.path.join(Path.home(),'config','pyrocoto.yaml')
#        
#        config_prio = [config_here, config_home]
#        for config_file in config_prio:
#            if os.path.isfile(config_file):
#                with open(config_file, 'r') as f:
#                    self.config = yaml.full_load(f)
#                    self.config_file = config_file
#                return
#                
#        # no configfile was found; set up new configfile at home/config
#        self.config = {}
#        self.config_file = config_home
#        dirname = os.path.dirname(config_home)
#        if not os.path.exists(dirname):
#            os.makedirs(dirname)
#                
#    def set_settings(self, settings, section='default'):
#        for setting, value in settings.items():
#            if value is None:
#                value = input(f"Provide {section} setting for {setting}: ")
#            if section not in self.config:
#                self.config[section] = {}
#            self.config[section][setting] = value
#        with open(self.config_file,'w') as f:
#            yaml.dump(self.config, f)
#                    
#    def get_setting(self, setting, section='default'):
#        '''This method iteratively calls itself until it errors or returns with setting'''
#        if section not in self.config:
#            if yes_or_no(f"{section} does not already exist; create it?"):
#                self.set_settings({setting:None}, section=section)
#                self.get_setting(setting,section)
#            else:
#                raise ValueError(f"section {section} does not exist and is not being created") # exit point
#        if setting in self.config[section]:
#            return self.config[section][setting]  # exit point
#        else:
#            self.set_settings({setting:None}, section)
#            self.get_setting(setting, section)
        

class Workflow(object):
    ''' Implement an abstarction layer on top of rocoto workflow management engine
        The WorkFlow class will serve as a central object that registers all units of work
        (tasks) for any number of desired cycle definitions.
    '''

    def __init__(self, realtime='T', scheduler='lsf', **kwargs):
        self.tasks = []
        self.task_data = []
        self.cycle_definitions = set()
        self.cycle_elements = []
        self.task_elements = []

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
                    task_work = copy.deepcopy(task)
                    task_work.apply_group(group)
                    task_work.task_group = group
                    task_unique_name = group + '_' + task_name
                    self.tasks[task_unique_name] = copy.deepcopy(vars(task_work))
            else:
                self.tasks[task_name] = vars(task)
                task.task_group = None

#    def task(self, groups=None, meta=None, **options):
#        ''' a decorator used to register task functions
#
#        @flow.task
#        def taskname(task_group):
#            namespace for defining task
#            return Task(var())
#        '''
#
#        def decorator(func):
#            task = func()
#            metaname = None
#            if hasattr(task, '__taskname__'):
#                task_name = task.__taskname__
#                if meta is not None:
#                    metaname = task.__taskname__.replace('#','')
#                    for key in meta.keys():
#                        if key not in task_name:
#                            raise ValueError('var "{}" was not fount in __taskname__ as #{}#'.format(key,key))
#            else:
#                # let function name be task name
#                task_name = _name_of_func(func)
#                if meta is not None:
#                    print('__taskname__ must be used with a metatask')
#                    raise ValueError
#
#            self.register_task(groups, task_name, task, meta, metaname)
#            return func
#
#        return decorator
#        ''' a decorator used to associate tasks with workflow
#
#        @flow.task
#        def taskname(task_group):
#            namespace for defining task
#            return Task(var())
#        '''
    def add_task(self, task, groups=None, meta=None):
        task._groups = groups
        task._meta = meta
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
            print(task)
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

#        # Construct cycle definition XML elements
#        self.cycle_groups = set()
#        for cycledef in self.cycle_definitions:
#            if cycledef.group in self.cycle_groups:
#                raise ValueError('duplicate cycle group defined')
#            self.cycle_groups.add(cycledef.group)
#            cycledef_element = Element('cycledef', group=cycledef.group)
#            if cycledef.activation_offset != 'None':
#                cycledef_element.attrib['activation_offset'] = cycledef.activation_offset
#            cycledef_element.text = cycledef.definition
#            self.cycle_elements.append(cycledef_element)
#
#        # Evaluate task functions, verify tasks have a cycle definition
#        # Store pertinent tasks in task_data


#        for taskname, task in self.tasks.items():
#
#            if groups is not None and task['task_group'] not in groups:
#                continue
#
#            task['task_unique_name'] = taskname
#
#            for cycledef in task['cycledefs']:
#                try:
#                    cycle_group = cycledef.group
#                except:
#                    cycle_group = cycledef
#                if cycle_group not in self.cycle_groups:
#                    raise ValueError('cycle group {} not defined'.format(cycle_group))
#
#            task['cycledefs'] = [groupattr(x) for x in task['cycledefs']]
#
#            if cycledefs is not None:
#                cycledefs = [groupattr(x) for x in cycledefs]
#                task['cycledefs'] = list(set(task['cycledefs']) & set(cycledefs))
#            self.task_data.append(task)
#
#        # Perform checks on task_data and prep for writing XML
#        for ix, task in enumerate(self.task_data):
#            self.task_data[ix]['xml_elements'] = []
#            for key in task.keys():
#
#                ''' Ignore data not directly relevant to rocoto xml workflow '''
#                if key not in ['command', 'account', 'queue', 'cores', 'nodes',
#                               'walltime', 'envar', 'native', 'memory', 'jobname',
#                               'join', 'stdout', 'stderr', 'deadline', 'dependency']:
#                    continue
#
#                ''' handle optional invironament variables (envar) '''
#                if key == 'envar':
#
#                    for name, value in task[key].items():
#                        envar = Element('envar')
#                        name_element = Element('name')
#                        name_element.text = name
#                        envar.append(name_element)
#                        value_element = Element('value')
#                        if isinstance(value, tuple):
#                            value_element.text = value[0]
#                            offset = value[1]
#                        else:
#                            value_element.text = value
#                            offset = None
#                        value_element = cyclestr(value_element, offset)
#                        envar.append(value_element)
#                        self.task_data[ix]['xml_elements'].append(envar)
#
#                # handle optional dependencies/triggers for the task
#                elif key == 'dependency':
#                    try:
#                        task['dependency'] = task['dependency'].elm
#                    except:
#                        pass
#                    for dep in task['dependency'].iter():
#                        if dep.tag == 'taskdep':
#                            valid_task_dep_name = False
#                            if task['task_group'] is not None:
#                                if dep.attrib['task'].startswith('!'):
#                                    dep.attrib['task'] = dep.attrib['task'][1:]
#                                else:
#                                    dep.attrib['task'] = task['task_group'] + '_' + dep.attrib['task']
#                            for i, data in enumerate(self.task_data):
#                                if (data['task_unique_name'] == dep.attrib['task']):
#                                    if i > ix:
#                                        print('task {} has dependencies which follow it'.format(data['name']))
#                                        print('review the order of imported tasks')
#                                        raise ValueError('dependent tasks not properly ordered')
#                                    valid_task_dep_name = True
#
#                                    break
#                            if not valid_task_dep_name:
#                                print(dep.attrib['task'], ' is not a known task, continuing')
#                                # raise ValueError
#
#                    deptag = Element('dependency')
#                    deptag.append(task[key])
#
#                    self.task_data[ix]['dependency'] = deptag
#
#                #  handle remaining workflow tags provided by user
#                else:
#                    elm = Element(key)
#                    elm.text = task[key]
#                    self.task_data[ix]['xml_elements'].append(cyclestr(elm))
#
#            ''' handle craetion of task XML elements '''
#            task_attributes = {}
#            task_attributes['name'] = self.task_data[ix]['task_unique_name']
#            task_attributes['cycledefs'] = ' '.join(self.task_data[ix]['cycledefs'])
#            try:
#                task_attributes['maxtries'] = self.task_data[ix]['maxtries']
#            except:
#                pass
#            try:
#                task_attributes['throttle'] = self.task_data[ix]['throttle']
#            except:
#                pass
#            self.task_data[ix]['xml_task'] = Element('task', task_attributes)
#
#            self.task_data[ix]['xml_task'].extend(self.task_data[ix]['xml_elements'])
#            try:
#                self.task_data[ix]['xml_task'].append(self.task_data[ix]['dependency'])
#            except:
#                pass
#
#            # handle meta task
#            if self.task_data[ix]['meta_dict'] is not None:
#                metaelm = Element('metatask', name=self.task_data[ix]['metaname'])
#                for var, metavars in self.task_data[ix]['meta_dict'].items():
#                    elm = Element('var', name=var)
#                    elm.text = metavars
#                    metaelm.append(elm)
#                self.task_data[ix]['vars'] = metaelm
#                self.task_data[ix]['vars'].append(self.task_data[ix]['xml_task'])
#                self.task_data[ix]['xml_task'] = self.task_data[ix]['vars']
#            # print(self.task_data[ix]['xml_task'])
#            self.task_elements.append(self.task_data[ix]['xml_task'])
#            print('added task {}'.format(self.task_data[ix]['task_unique_name']))
#
#        self.workflow_element.extend(self.cycle_elements)
#        self.workflow_element.append(self.log_element)
#        self.workflow_element.extend(self.task_elements)
#
        print(self.workflow_element)
        with open(xmlfile, 'w') as f:
            f.write('<?xml version="1.0"?>\n<!DOCTYPE workflow []>')
            f.write(self.prettify(self.workflow_element)[22:])

#class TaskBasic():
#    ''' Implement container for information pertaining to a single task '''
#    all_have = ['command', 'account', 'walltime', 'queue', 'maxtries']
#    command = String(contains='/')
    
#    def __init__(self, **kwargs):

class Task: 
    """ write something useful """
    # validate and track class meta data
    # note: validated data attributes are added to self._validated by the validators
    command = String()
    jobname = String()
    account = String()
    walltime = String(contains = ":")
    maxtries = String() # make int
    queue = String()
    memory = String() # maybe validate this more
    native = String()
    cores = String() # make int
#   dependency = Dependency()
    groups = Groups()
    envar = Envar()

    name = String() # tasks added to workflow should have unique name
#    _groups = String() # make this a list, ensure all items unique
#    _meta = String() # make this a dict, ensure all values are strings

    _required = [['command'],
                 ['join', 'stderr'],
                 ['cores', 'nodes'],
                 ['cycledefs']]

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
        # If groups or meta provided, set them first as other metadata
        # will validate or augment based on them
        if 'groups' in d:
            setattr(self,'groups', d['groups'])
        if 'meta' in d:
            setattr(self,'meta', d['meta'])
        for k,v in d.items():
            setattr(self, k, v)



    def validate(self):
        # ensure that metadata that should be diffrent by job is.
        # jobname, join/stderr,
        # meta keys should be specified when meta and
        # :group: should be specified when groups
        # ensure the agregate of data for this task looks ok
        # check here for common mistakes as found only if they are based on a combination of data pieces
        # single data validation should occur within a validator
        pass

    def generate_xml(self):
        elm_task = Element('task')
        for attr in self._for_xml:
            print(f'trying {attr}')
            if hasattr(self,attr):
                print(f'found {attr}')
                if getattr(self,attr) is not Element:
                    print(f'working on {attr}')
                    if attr.strip('_') == 'envar': 
                        elm_task.extend(getattr(self,attr))
                        print(tostring(getattr(self,attr)[0]))
                    else:
                        E = Element(attr.strip('_'))
                        E.text = getattr(self,attr)
                        elm_task.append(E)
                else:
                    print(f'working on {attr}')
                    elm_task.append(getattr(self,attr))

        #print(Workflow.prettify(elm_task))
        return elm_task
#        element_task = Element('task', task_attributes)
#       return task(s) xml

#class Task(object):
#    ''' Implement container for information pertaining to a single task '''
#    command = String(contains='/')
#
#    def __init__(self, *args, **kwargs):
#        self.cfg = Config()
#        self.inputs = {}
#        self.inputs.update(**kwargs)
#        setattr(self, 'command', self.inputs['command'])
#        print(dir(self))
#        print(getattr(self,'command'))
#        if 'config_section' in self.__dict__:
#            sect = self.__dict__['config_section']
#        else:
#            sect = 'default'
#        self.name = inspect.stack()[1][3]
#        self.account = getattr(self, 'account', self.cfg.get_setting('account'))
#        self.walltime = getattr(self, 'walltime', '00:20:00')
#        self.maxtries = getattr(self, 'maxtries', '1')
#        self.queue = getattr(self, 'queue', self.cfg.get_setting('account'))
#        if 'queue_defaults' in self.cfg.config[sect]:
#            defaults = self.cfg.config[sect]['queue_defaults']
#            if self.queue in defaults:
#                for setting in defaults[self.queue]:
#                    self[setting] = defaults[self.queue][setting]
#
#       # if self.queue in ['dev2_shared', 'transfer']: # puth this stuff in config
#       #     self.memory = getattr(self, 'memory', '2056M')
#       #     self.native = getattr(self, 'native', '-R affinity[core]')
#       #     self.cores = getattr(self, 'cores', '1')
#        try:
#            self.jobname = getattr(self, 'jobname', self.__taskname__ + '_@Y@m@d@H')
#        except:
#            self.jobname = getattr(self, 'jobname', self.name + '_@Y@m@d@H')
#
#    def __repr__(self):
#        return '%s' % vars(self)
#
#    def __getitem__(self, key):
#        return getattr(self, key)
#
#    def __setitem__(self, key, val):
#        return setattr(self, key, val)
#
#    def apply_group(self, group):
#        for key, obj in vars(self).items():
#            # def f(x):
#            #     return x.replace(':group:', group)
#            f = lambda x: x.replace(':group:', group)
#            obj = modify_tree_nodes(obj, action_func=f)
#            self[key] = obj
#
#    def verify(self):
#        required_keys = [['command'], ['join', 'stderr'],
#                         ['cores', 'nodes'], ['cycledefs']]
#        keys = self.__dict__.keys()
#        for reqkey in required_keys:
#            if set(reqkey) & set(keys):
#                continue
#            print('missing requred inputs ' + str(reqkey))
#            raise ValueError(f'expected information for {repr(reqkey)}')


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

    pass

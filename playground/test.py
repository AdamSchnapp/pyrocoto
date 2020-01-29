#!/usr/bin/env python
from pyrocoto import Workflow, Task, Dependency, Offset, DataDep, TaskDep, MetaTaskDep, product_meta
from pathlib import Path
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

flow = Workflow()


hourly = flow.define_cycle('hourly','30 * * * * *')
min15 = flow.define_cycle('min15','0,15,45 * * * * *', activation_offset='-00:00:04') # specify, name of definition, definition, and other optional attributes

@flow.task()
def prep():
    name = 'prep'
    cycledefs = hourly
    command = '/runcommand'
    jobname = 'jobname_@Y@m@d@H'
    queue = 'queue'
    envar = {'var1' : 'value1',
             'var2' : 'value2',
             'var3' : Offset('@Y@m@d@H','2:00:00'),
             'var4' : '@Y@m@d@H'}
    cores = '1'
    join = '/temp/job@Y@m@d@H.join'
    return Task(locals())


@flow.task()
def obs_granalysis():
    metatask_name = 'obs_gran'
    name = '#element#_#level#_obs'  #required for specifying metatask vars in tasks name
    meta = product_meta({'element' : 'cig vis', 'level' : 'ifr lifr'})
    cycledefs = [hourly, min15]                     # Required
    command = Offset('/runcommand @Y@m@d@H','2:00:00')       # Required
    jobname = 'jobname_#element#_#level#_@Y@m@d@H'          # Will default to something reasonable
    queue = 'queue'                                
    envar = {'element' : '#element#',                   # Optional, set environment variables
             'level' : '#level#'}                       # top level Job script should do the bulk of environment variable setting
    nodes = '1:ppn=24'                            # Requires either cores or nodes tag
    native = '-a openmp'
    join = f'/{jobname}.join'                        # every task should have a join or stderr and stdout for logs

    #and1 = Dependency('datadep',data='filename')
    #and2 = Dependency('taskdep',task=':group:_taskname', cycle_offset='-6:00:00')
    #and3 = Dependency('timedep', offset='00:00:34')
#    dependency = and1
#    dependency = Dependency.operator('and' ,and1, and2, and3)
#    dependency = Dependency('taskdep',task='plotvis')
    d1 = DataDep('/root/file1')
    d2 = DataDep(Offset('/root/file2_@H','-00:01:00'))
    d1_and_d2 = Dependency.operator('and' ,d1 ,d2)
    d3 = TaskDep('prep')
    dependency = Dependency.operator('and' ,d1_and_d2 ,d3)
                                                
    return Task(locals())                           # don't modify this


@flow.task()
def prep():
    name = 'after_obs_granalsysis'
    cycledefs = hourly
    command = '/runcommand'
    jobname = 'jobname_@Y@m@d@H'
    queue = 'queue'
    envar = {'var1' : 'value1',
             'var2' : 'value2',
             'var3' : Offset('@Y@m@d@H','2:00:00'),
             'var4' : '@Y@m@d@H'}
    cores = '1'
    join = '/temp/job@Y@m@d@H.join'
    dependency = MetaTaskDep('obs_gran')
    return Task(locals())


if __name__ == '__main__':
    name = Path(__file__).stem
    flow.set_log(f'{name}.@Y@m@d@H')
    flow.write_xml(f'{name}.xml')

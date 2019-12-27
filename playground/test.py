#!/usr/bin/env python
from pyrocoto import Workflow, Task, Dependency
from pathlib import Path

flow = Workflow()


hourly = flow.define_cycle('hourly','30 * * * * *')
min15 = flow.define_cycle('min15','0,15,45 * * * * *', activation_offset='-00:00:04') # specify, name of definition, definition, and other optional attributes

@flow.task()
def prep():
    groups = ['expr','expr2']
    name = ':group:_taskname'
    cycledefs=[hourly, min15]
    command = 'runcommand_:group:'
    jobname = ':group:_jobname_@Y@m@d@H'
    queue = 'queue'
    envar = {'var1' : 'value1',
             'var2' : 'value2',
             'var3' : '@Y@m@d@H'}
    cores = '1'
    join = '/temp/job@Y@m@d@H.join'
    return Task(locals())


@flow.task()
def obs_granalysis():
    name = '#element#_#level#_obs'  #required for specifying metatask vars in tasks name
    meta = {'element' : 'cig vis', 'level' : 'ifr lifr'}
    groups = ['expr','expr2']
    cycledefs = [hourly, min15]                     # Required
    command = 'runcommand_:group: @Y@m@d@H'       # Required
    jobname = ':group:_jobname_#element#_#level#_@Y@m@d@H'          # Will default to something reasonable
    queue = 'queue'                                
    envar = {'element' : '#element#',                   # Optional, set environment variables
             'level' : '#level#'}                       # top level Job script should do the bulk of environment variable setting
    nodes = '1:ppn=24'                            # Requires either cores or nodes tag
    native = '-a openmp'
    join = jobname+'.join'                        # every task should have a join or stderr and stdout for logs

    and1 = Dependency('datadep',data='filename')
    and2 = Dependency('taskdep',task=':group:_taskname', cycle_offset='-6:00:00')
    and3 = Dependency('timedep', offset='00:00:34')
#    dependency = and1
#    dependency = Dependency.operator('and' ,and1, and2, and3)
#    dependency = Dependency('taskdep',task='plotvis')
                                                
    return Task(locals())                           # don't modify this



if __name__ == '__main__':
    name = Path(__file__).stem
    flow.set_log(f'{name}.@Y@m@d@H')
    flow.write_xml(f'{name}.xml')

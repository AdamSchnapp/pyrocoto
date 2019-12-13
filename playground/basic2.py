#!/usr/bin/env python
from pyrocoto import Workflow, Task, Config
from pathlib import Path

cfg = Config()

settings = {}
settings['queue_defaults'] = {'service_2':
                                {'memory':'2056M',
                                 'cores':'1'
                                },
                              'shared':
                                {'memory':'2056M',
                                 'cores':'1'
                                }
                             }
cfg.set_settings(settings)


flow = Workflow()
hourly = flow.define_cycle('hourly','0 * * * * *')

@flow.task()
def task1():
    cycledefs = [hourly]
    jobname = 'task1'
    command = 'task1_command'
    queue = 'service_2'
    join = 'task1.join'
    return Task(locals())

if __name__ == '__main__':
    name = Path(__file__).stem
    flow.set_log(f'{name}.@Y@m@d@H')
    flow.write_xml(f'{name}.xml')

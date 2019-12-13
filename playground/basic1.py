#!/usr/bin/env python
from pyrocoto import Workflow, Task
from pathlib import Path

flow = Workflow()
hourly = flow.define_cycle('hourly','0 * * * * *')

@flow.task()
def task1():
    cycledefs = [hourly]
    jobname = 'task1'
    command = 'task1_command'
    queue = 'service'
    cores = '1'
    join = 'task1.join'
    return Task(locals())

if __name__ == '__main__':
    name = Path(__file__).stem
    flow.set_log(f'{name}.@Y@m@d@H')
    flow.write_xml(f'{name}.xml')

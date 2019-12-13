#!/usr/bin/env python
from pyrocoto import Workflow, Task, Dependency
from pathlib import Path

flow = Workflow()
hourly = flow.define_cycle('hourly', '0 * * * * *')
six30 = flow.define_cycle('six30', '30 6 * * * *')

@flow.task()
def task1():
    cycledefs = [hourly, six30]
    jobname = 'task1'
    command = 'task1_command'
    queue = 'service'
    cores = '1'
    join = 'task1.join'
    return Task(locals())

@flow.task()
def task2():
    cycledefs = [hourly, six30]
    jobname = 'task2'
    command = 'task2_command'
    queue = 'service'
    cores = '1'
    join = 'task2.join'
    dependency = Dependency('datadep', data='path/to/file')
    return Task(locals())

@flow.task()
def task3():
    cycledefs = [hourly, six30]
    jobname = 'task3'
    command = 'task3_command'
    queue = 'service'
    cores = '1'
    join = 'task3.join'
    d1 = Dependency('taskdep',task='task1')
    d2 = Dependency('taskdep',task='task2')
    dependency = Dependency.operator('and', d1, d2)
    return Task(locals())


if __name__ == '__main__':
    name = Path(__file__).stem
    flow.set_log(f'{name}.@Y@m@d@H')
    flow.write_xml(f'{name}.xml')

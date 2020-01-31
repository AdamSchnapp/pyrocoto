
## Intoduction
Pyrocoto is a python interface for generationg rocoto workflow xml definitions.
See rocoto documentation at https://github.com/christopherwharrop/rocoto/wiki/Documentation for how rocoto supports creating and running workflows to aid scientific computing and numerical modeling.

## Pyrocoto use case

Pyrocoto is designed to aid rocoto users in creating and maintaining complicated workflows. It provides an api for specifying the tasks, metatasks, dependencies, and most other settings which the rocoto workflow manager has implemented. pyrocoto validates task input and produces rocoto complient xml definitions.

### Requirements
* Python 3.8+

### Installation

    python setup.py

## Pyrocoto API

### Quickstart

Create a workflow
    
```python
from pyrocoto import Workflow
flow = Workflow()
```

Add a cycle definition with group name 'hourly' and cron syntax
    
```python 
hourly = flow.define_cycle('hourly', '0 * * * * *')
```

Create a Task. Task creation occurs when metadata in a dict-like format with known keywords is passed to the Task constructor
    
```python
from pyrocoto import Task
taskdata = {'name': 'task_name',
            'cycledefs': hourly,
            'command': '~/command arg1',
            'queue': 'queue',
            'cores': '1',
            'join': '~/task_name.join',
            'account': 'my_account'}
task1 = Task(taskdata)
```

The task should then be added to the workflow
    
```python
flow.add_task(T)
```

The prior steps can be performed with the the Workflow's task decorator like so.
    
```python
@flow.task()
def task1():
    name = 'task_name'
    cycledefs = hourly
    command = '~/command arg1'
    queue = 'queue'
    cores = '1'
    join = '~/task_name.join'
    account = 'my_account'
    return Task(locals())
```

The Task data is validated on Task instance creation and when added to the workflow.

Set the workflow log file and write the workflow definition to an xml file for use with the rocoto workflow manager.

```python
if __name__ == '__main__':
    flow.set_log('~/my_workflow_log.@Y@m@d@H')
    flow.write_xml('~/my_workflow.xml
```

### Task keywords

* name (required)
* command (required)
* cycledefs (required)
* queue (required)
* account (required)
* join
* stderr
* stdout
* cores
* nodes
* memory
* jobname
* walltime
* envar
* dependency
* native
* partition
* meta
* metatask_name

### Dependecies

Dependencies are a core part of a Rocoto workflow.
See the following example for how to create dependencies in pyrocoto


```python
from pyrocoto import Dependency, DataDep, TaskDep, TimeDep, Offset
file_dep = DataDep('needed_file', age='10')  # "needed_file" must exist and be atleast 10 seconds old
time_dep = TimeDep(Offset('@Y@m@d@H', '00:10:00'))  # must be 10 minutes after cycle time
prev_task = TaskDep('name_of_prev_task')  # task with name "name_of_prev_task" must be complete
dependency = Dependency.operator('and', file_dep, time_dep, prev_task)  # important that variable is names "dependency"
```





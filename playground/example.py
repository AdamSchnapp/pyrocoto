#!/usr/bin/env python
from pyrocoto import Workflow, Task, Dependency, Offset, DataDep, TaskDep, MetaTaskDep, TimeDep, product_meta
from pathlib import Path
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

flow = Workflow(realtime='Y', cyclelifespan="12:00:00", taskthrottle="50")

VERSION='1.0.0'
HOME='/myhome'

hourly = flow.define_cycle('hourly','0 * * * * *')
bidaily = flow.define_cycle('bidaily','0 0,12 * * * *', activation_offset='-00:00:05') # specify, name of definition, definition, and other optional attributes


class MySerialTask(Task):
    def __init__(self, *args):
        self.account = 'myproject'
        self.cores = '1'
        self.memory = '4G'
        self.walltime = '00:10:00'
        self.queue = 'serialqueue'
        super().__init__(*args)

class MyForecastTask(Task):
    def __init__(self, *args):
        self.account = 'myproject'
        self.walltime = '00:30:00'
        self.queue = 'forecast_queue'
        super().__init__(*args)


ENVAR = {'domain': 'conus',
         'element': 'temp',
         'YYYYMMDDHH': '@Y@m@d@H',
         'VERSION': VERSION,
         'YESTERDAY': Offset('@Y@m@d','-24:00:00')}

@flow.task()
def prep():
    name = "prep"
    cycledefs = hourly
    command = f'{HOME}/runcommand prep'
    jobname = '{name}_@Y@m@d@H'
    envar = ENVAR
    join = f'/temp/{jobname}.join'
    timedep = TimeDep(Offset('@Y@m@d@H','00:10:00'))  # 10 minutes after cycle time 
    datadep = DataDep('/some_data')
    dependency = Dependency.operator('or', timedep, datadep)
    return MySerialTask(locals())

@flow.task()
def forecast():
    name = "forecast"
    cycledefs = hourly
    command = f'{HOME}/runcommand forecast'
    jobname = '{name}_@Y@m@d@H'
    nodes = '4:ppn=24'
    envar = ENVAR
    join = f'/temp/{jobname}.join'
    dependency = TaskDep('prep')
    return MyForecastTask(locals())


if __name__ == '__main__':
    name = Path(__file__).stem
    flow.set_log(f'{name}.@Y@m@d@H')
    flow.write_xml(f'{name}.xml')

from pyrocoto import Workflow, Task, Dependency, tostring
from pathlib import Path


flow = Workflow()
hourly = flow.define_cycle('hourly','30 * * * * *')
min15 = flow.define_cycle('min15','0,15,45 * * * * *', activation_offset='-00:00:04') # specify, name of definition, definition, and other optional attributes

if __name__ == '__main__':
    name = Path(__file__).stem
    flow.set_log(f'{name}.@Y@m@d@H')
    flow.write_xml(f'{name}.xml')

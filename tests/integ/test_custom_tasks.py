import pytest
from pyrocoto import Workflow, Task, Offset, DataDep
import os
from distutils import dir_util


@pytest.fixture
def data_dir(tmpdir, request):
    '''
    Fixture responsible for searching a folder with the same name of test
    module and, if available, moving all contents to a temporary directory so
    tests can use them freely.
    '''
    os.chdir(str(tmpdir))
    file = request.module.__file__
    test_dir, _ = os.path.split(file)
    basename = os.path.splitext(file)[0]
    print(test_dir)
    testdata = os.path.join(test_dir,basename)
    print(testdata)
    if os.path.isdir(testdata):
        print(testdata)
        print(str(tmpdir))
        dir_util.copy_tree(testdata, str(tmpdir))
    return tmpdir


#@pytest.fixture
#def setup(data_dir):
#    config=configparser.ConfigParser()
#    configfile = 'pyrocoto.ini'
#    test_settings = {'account': 'test_account',
#                     'walltime': '00:20:00',
#                     'maxtries': '2',
#                     'queue': 'test_queue',
#                     'memory': '4000M',
#                     'cores': '1'}
#    config['default'] = test_settings
#    with open(configfile,'w') as f:
#        config.write(f)

class MySerialTask(Task):
    def __init__(self, d):
        ''' set a bunch of default settings '''
        self.account = 'myproject'
        self.cores = '1'
        self.memory = '2G'
        self.queue = 'queue_for_my_serial_task'
        self.walltime = '00:10:00'
        super().__init__(d)




def test_mytasks_workflow(data_dir, request):

    flow = Workflow()
    hourly = flow.define_cycle('hourly','0 * * * * *')

    bunch_of_args = ['thing1', 'thing2', 'thing3']
    for arg in bunch_of_args:
        @flow.task()
        def task():
            name = f'task{arg}'
            cycledefs = hourly
            command = f'/runcommand @Y@m@d@H {arg}'
            jobname = f'task1_@Y@m@d@H_{arg}'
            join = f'/task1_@Y@m@d@H_{arg}.join'
            dependency = DataDep(f'file_needed_for_{arg}')
            return MySerialTask(locals())
        

    flow.set_log('log_task.@Y@m@d@H')

    wf_name = request.node.name[5:]
    wf_file = f'{wf_name}.xml'
    flow.write_xml(str(data_dir.join(wf_file)))
    
    validationfile = f'{wf_name}.validate'
    with open(validationfile) as f, open(wf_file) as f2:
        assert  f.read() == f2.read()



import pytest
from pyrocoto import Workflow, Task, Offset
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

def test_minimal_workflow(data_dir, request):

    flow = Workflow(_shared=False)
    hourly = flow.define_cycle('hourly','0 * * * * *')

    @flow.task()
    def task1():
        name = 'task1'
        cycledefs = hourly
        command = '/runcommand @Y@m@d@H'
        jobname = 'task1_@Y@m@d@H'
        cores = '1'
        join = '/task1_@Y@m@d@H.join'
        queue = 'queue'
        account = 'my_account'
        return Task(locals())

    flow.set_log('log_task1.@Y@m@d@H')

    wf_name = request.node.name[5:]
    wf_file = f'{wf_name}.xml'
    flow.write_xml(str(data_dir.join(wf_file)))

    validationfile = f'{wf_name}.validate'
    with open(validationfile) as f, open(wf_file) as f2:
        assert  f.read() == f2.read()


def test_task2_workflow(data_dir, request):

    flow = Workflow(_shared=False)
    hourly = flow.define_cycle('hourly','0 * * * * *')  # once/hour at top of hour
    min15 = flow.define_cycle('min15','15,30,45 * * * * *')  # at 15,30, and 45 minutes after top of hour

    @flow.task()
    def task2():
        name = 'task2'
        cycledefs = [hourly, min15]  # run at hourly times and min15 times
        command = '/runcommand @Y@m@d@H@M'
        jobname = 'task2_@Y@m@d@H@M'
        envar = {'SETTING1': 'YES',
                 'SETTING2': 'NO',
                 'YESTERDAY': Offset('@Y@m@d','-24:00:00')}
        nodes = '10:ppn=4'  # request 4 cores on each of 10 nodes
        nodesize = '4'
        queue = 'queue'
        account = 'my_account'
        native = '-setting_to_pass_to_scheduler_that_rocoto_may_not_support'
        join = '/task2_@Y@m@d@H@M.join'
        walltime = '1:00:00'
        maxtries = '3'
        return Task(locals())

    flow.set_log('log_task1.@Y@m@d@H')

    wf_name = request.node.name[5:]
    print(wf_name)
    wf_file = f'{wf_name}.xml'
    flow.write_xml(str(data_dir.join(wf_file)))

    validationfile = f'{wf_name}.validate'
    with open(validationfile) as f, open(wf_file) as f2:
        assert  f.read() == f2.read()

import pytest
import configparser
from pyrocoto import Workflow, Task
import os
from distutils import dir_util


flow = Workflow()

hourly = flow.define_cycle('hourly','0 * * * * *')


    

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


@pytest.fixture
def setup(data_dir):
    config=configparser.ConfigParser()
    configfile = 'pyrocoto.ini'
    test_settings = {'account': 'test_account',
                     'walltime': '00:20:00',
                     'maxtries': '2',
                     'queue': 'test_queue',
                     'memory': '4000M',
                     'cores': '1'}
    config['default'] = test_settings
    with open(configfile,'w') as f:
        config.write(f)


def test_basic_workflow(setup, tmpdir, request):

    flow = Workflow()
    hourly = flow.define_cycle('hourly','0 * * * * *')
    
    @flow.task()
    def task1():
        cycledefs=[hourly]
        command = 'runcommand @Y@m@d@H'
        jobname = 'task1_@Y@m@d@H'
        join = 'task1_@Y@m@d@H.join'
        return Task(locals())

    flow.set_log('log_task1.@Y@m@d@H')
    flow.write_xml(str(tmpdir.join('basic_workflow.xml')))
    
    validationfile = request.node.name[5:] + '.validate'
    with open(validationfile) as f, open('basic_workflow.xml') as f2:
        assert  f.read() == f2.read()

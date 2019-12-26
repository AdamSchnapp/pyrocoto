from pyrocoto import Config

cfg = Config()

settings = {}
settings['queue_defaults'] = {'service':
                                {'memory':'2056M',
                                 'cores':'1'
                                },
                              'shared':
                                {'memory':'2056M',
                                 'cores':'1'
                                }
                             }
cfg.set_settings(settings)

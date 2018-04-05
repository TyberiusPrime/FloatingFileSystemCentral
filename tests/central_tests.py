import unittest
import time
import shutil
from pprint import pprint
import collections
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from central import engine, ssh_message_que, default_config


class FakeMessageSender():

    def __init__(self):
        self.outgoing = []

    def send_message(self, receiver, receiver_info, msg):
        msg = msg.copy()
        msg['to'] = receiver
        self.outgoing.append(msg)

    def kill_unsent_messages(self):
        pass

    def get_messages_for_node(self, receiver):
        return [x for x in self.outgoing if x['to'] == receiver]


class EngineTests(unittest.TestCase):

    def assertMsgEqualMinusSnapshot(self, actual, supposed):
        msg = actual.copy()
        for k in ['target_ssh_cmd', 'target_user', 'target_ffs', 'target_node', 'target_storage_prefix']:
            if k in msg and not k in supposed:
                del msg[k]
        self.assertMsgEqual(msg, supposed)

    def strip_send_snapshot_target_stuff(self, msg):
        msg = msg.copy()
        return msg

    def assertMsgEqual(self, msgA, msgB):
        def strip_storage(msg):
            msg = msg.copy()
            for k in ['storage_prefix']:
                if k in msg:
                    del msg[k]
            return msg
        # at least one needs be auto generated
        self.assertTrue('storage_prefix' in msgA or 'storage_prefix' in msgB)
        # if they're both specifed, must be the same, otherwise: ignore
        if 'storage_prefix' not in msgA or 'storage_prefix' not in msgB:
            msgA = strip_storage(msgA)
            msgB = strip_storage(msgB)
        self.assertEqual(msgA, msgB)

    @classmethod
    def tearDownClass(cls):
        try:
            shutil.rmtree('node')
        except OSError:
            pass

    def _get_test_config(self):
        class NobodyConfig(default_config.DefaultConfig):

            def get_chown_user(self, ffs):
                return 'nobody'

            def get_chmod_rights(self, ffs):
                return '0777'

            def get_default_properties(self):
                return {}

            def get_enforced_properties(self):
                return {}
        return NobodyConfig()

    def get_engine(self, quick_ffs_definition, config=None):
        """Helper to get a startuped-engine running quickly.
        Just pass in a definition of
        {host: {
            'ffs_name': [snapshot, (property_name, value), snapshot, ...]
        }}.
        Mark the main by giving it _ffs_name instead of of ffs_name.
        Example
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ('ffs:test', '2')]},
            'gamma': {},
        })
        """
        nodes = collections.OrderedDict()
        for name in quick_ffs_definition:
            nodes[name] = {'storage_prefix': '/' + name,
                           'hostname': name, 'public_key': b'#no such key'}
        if config is None:
            config = self._get_test_config()
        config._nodes = nodes
        config = default_config.CheckedConfig(config)

        fm = FakeMessageSender()
        e = engine.Engine(
            config,
            fm,
        )
        e.incoming_client({'msg': 'startup', })
        for node_name, ffs_to_snapshots in quick_ffs_definition.items():
            e.incoming_node({'msg': 'deploy_done', 'from': node_name})
        fm.outgoing.clear()
        for node_name, ffs_to_snapshots in quick_ffs_definition.items():
            ffs = {}
            for ffs_name, snapshots_and_props in ffs_to_snapshots.items():
                if ffs_name.startswith('_'):
                    ffs_name = ffs_name[1:]
                    props = {'ffs:main': 'on', 'readonly': 'off'}
                else:
                    props = {'ffs:main': 'off', 'readonly': 'on'}
                snapshots = []
                for x in snapshots_and_props:
                    if isinstance(x, tuple):
                        props[x[0]] = x[1]
                    else:
                        snapshots.append(x)
                ffs[ffs_name] = {'snapshots': snapshots, 'properties': props}
            e.incoming_node({'msg': 'ffs_list',
                             'from': node_name,
                             'ffs': ffs
                             })
        self.assertTrue(e.startup_done)
        return e, fm.outgoing

    def ge(self, config=None):
        nodes = collections.OrderedDict()
        nodes['alpha'] = {'storage_prefix': '/alpha',
                          'hostname': 'alpha', 'public_key': b'a'}
        nodes['beta'] = {'storage_prefix': '/beta',
                         'hostname': 'beta', 'public_key': b'b'}
        fm = FakeMessageSender()
        if config is None:
            config = self._get_test_config()
        config._nodes = nodes
        config = default_config.CheckedConfig(config)
        return engine.Engine(
            config,
            fm,
        ), fm.outgoing


class StartupTests(EngineTests):

    def test_nodes_may_not_start_with_dash(self):
        nodes = {}
        nodes['_alpha'] = {'storage_prefix': '/alpha',
                           'hostname': 'alpha', 'public_key': b'a'}

        def ignore(*args, **kwargs):
            pass

        def inner():
            cfg = self._get_test_config()
            cfg._nodes = nodes
            engine.Engine(
                default_config.CheckedConfig(cfg),
                ignore
            )
        self.assertRaises(ValueError, inner)

    def test_startup_sends_deployment_followed_by_list_ffs(self):
        def strip_node_zip(msg):
            if 'node.zip' in msg:
                import base64
                base64.decodebytes(msg['node.zip'].encode(
                    'utf-8'))  # make sure it decodes
                msg['node.zip'] = '...base64...'
            return msg
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        self.assertMsgEqual(strip_node_zip(outgoing_messages[0]),
                            {
            'to': 'alpha',
            'msg': 'deploy',
            'node.zip': '...base64...',
        })
        self.assertMsgEqual(strip_node_zip(outgoing_messages[1]),
                            {
            'to': 'beta',
            'msg': 'deploy',
            'node.zip': '...base64...',
        })
        outgoing_messages.clear()
        e.incoming_node({'msg': 'deploy_done', 'from': 'alpha'})
        e.incoming_node({'msg': 'deploy_done', 'from': 'beta'})
        self.assertFalse(e.startup_done)
        self.assertMsgEqual(outgoing_messages[0],
                            {
            'to': 'alpha',
            'msg': 'list_ffs'
        })
        self.assertMsgEqual(outgoing_messages[1],
                            {
            'to': 'beta',
            'msg': 'list_ffs'
        })
        outgoing_messages.clear()

        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {}
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {}
                         })
        self.assertTrue(e.startup_done)
        # no ffs info -> nothing happens
        self.assertEqual(len(outgoing_messages), 0)

    def test_list_ffs_failure_leads_to_faulted_engine(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })

        def inner():
            e.incoming_node({"error": "exception", "content": "storage prefix not a ZFS",
                             "traceback": "Traceback (most recent call last):\n  File \"/home/ffs/node.py\", line 451, in check_storage_prefix\n    is_root = get_zfs_property(zfs_name, 'ffs:root') == 'on'\n  File \"/home/ffs/node.py\", line 27, in get_zfs_property\n    return _get_zfs_properties(zfs_name)[property_name]\n  File \"/home/ffs/node.py\", line 19, in _get_zfs_properties\n    lines = zfs_output(['sudo', 'zfs', 'get', 'all', zfs_name, '-H']\n  File \"/home/ffs/node.py\", line 14, in zfs_output\n    raise subprocess.CalledProcessError(p.returncode, cmd_line, stdout + stderr)\nsubprocess.CalledProcessError: Command '['sudo', 'zfs', 'get', 'all', 'doesnotexist', '-H']' returned non-zero exit status 1\n\nDuring handling of the above exception, another exception occurred:\n\nTraceback (most recent call last):\n  File \"/home/ffs/node.py\", line 462, in dispatch\n    check_storage_prefix(msg)\n  File \"/home/ffs/node.py\", line 457, in check_storage_prefix\n    raise ValueError(\"storage prefix not a ZFS\")\nValueError: storage prefix not a ZFS\n"})
        self.assertRaises(engine.ManualInterventionNeeded, inner)
        self.assertTrue(e.faulted)

    def test_restoring_main_from_ro(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'readonly': 'off',
                             }
                             }
                         }
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {}
                         })
        self.assertTrue(e.startup_done)
        self.assertEqual(len(e.model), 1)
        self.assertEqual(e.model['one']['_main'], 'beta')
        self.assertEqual(e.model['one']['beta'][
                         'properties']['readonly'], 'off')
        # no ffs info -> nothing happens
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:main': 'on'}
        })
        outgoing_messages.clear()
        e.incoming_node({
            "msg": 'set_properties_done',
            'from': 'beta',
            'ffs': 'one',
            'properties': {'ffs:main': 'on'}
        })
        self.assertEqual(e.model['one']['beta'][
                         'properties']['readonly'], 'off')
        self.assertEqual(e.model['one']['beta'][
                         'properties']['ffs:main'], 'on')

    def test_no_main_multiple_one_rw_main_restauration(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'readonly': 'off',
                             }
                             }
                         }
                         })

        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {
                                'one': {'snapshots': [],
                                        'properties': {
                                    'readonly': 'on',
                                }
                                }
                         }
                         })
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:main': 'off'}
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:main': 'on'}
        })

    def test_main_restores_readonly(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'off',
                             }
                             }
                         }
                         })

        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {
                                'one': {'snapshots': [],
                                        'properties': {
                                    'ffs:main': 'on',
                                    'readonly': 'on'
                                }
                                }
                         }
                         })
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'readonly': 'off'}
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'readonly': 'on'}
        })

    def test_no_main_multiple_non_ro_raises_inconsistency(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'readonly': 'off',
                             }
                             }
                         }
                         })

        def inner():
            e.incoming_node({'msg': 'ffs_list',
                             'from': 'alpha',
                             'ffs': {
                                 'one': {'snapshots': [],
                                         'properties': {
                                     'readonly': 'off',
                                 }
                                 }
                             }
                             })
        self.assertRaises(engine.ManualInterventionNeeded, inner)
        self.assertTrue(e.faulted)

    def test_orphan_target_from_other_ffs_raises_inconsitency(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'readonly': 'on',
                                 'ffs:main': 'off',
                             }
                             }
                         }
                         })

        def inner():
            e.incoming_node({'msg': 'ffs_list',
                             'from': 'alpha',
                             'ffs': {}
                             })
        self.assertRaises(engine.ManualInterventionNeeded, inner)
        self.assertTrue(e.faulted)

    def test_multiple_main_raises(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'on',
                             }
                             }
                         }
                         })

        def inner():
            e.incoming_node({'msg': 'ffs_list',
                             'from': 'alpha',
                             'ffs': {
                                 'one': {'snapshots': [],
                                         'properties': {
                                     'ffs:main': 'on',
                                 }
                                 }
                             }
                             })
        self.assertRaises(engine.ManualInterventionNeeded, inner)
        self.assertTrue(e.faulted)

    def test_setting_ro_from_main(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup',
                           })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'on',
                             }
                             }
                         }
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'off',
                             }
                             }
                         }
                         })
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'readonly': 'on'}
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'readonly': 'off'}
        })

    def _get_cfg_enforced_properties(self, props):
        cfg = self._get_test_config()
        cfg.get_enforced_properties = lambda: props
        return cfg

    def test_enforced_properties_set_if_unset(self):
        e, outgoing_messages = self.ge(
            self._get_cfg_enforced_properties({'ffs:test': 'shu'}))
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'off',
                                 'readonly': 'on',
                             }
                             }
                         }
                         })

        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {
                                'one': {'snapshots': [],
                                        'properties': {
                                    'ffs:main': 'on',
                                    'readonly': 'off'
                                }
                                }
                         }
                         })
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:test': 'shu'}
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:test': 'shu'}
        })

    def test_enforced_properties_set_if_wrong_not_set_if_unchanged(self):
        e, outgoing_messages = self.ge(
            self._get_cfg_enforced_properties({'ffs:test': 'shu'}))
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': ['ffs-1'],
                                     'properties': {
                                 'ffs:main': 'off',
                                 'readonly': 'on',
                                 'ffs:test': 'shu',
                             }
                             }
                         }
                         })

        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {
                                'one': {'snapshots': ['ffs-1'],
                                        'properties': {
                                    'ffs:main': 'on',
                                    'ffs:test': 'sha',
                                    'readonly': 'off'
                                }
                                }
                         }
                         })
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:test': 'shu'}
        })
        self.assertEqual(len(outgoing_messages), 1)

    def test_enforced_properties_during_readonly(self):
        e, outgoing_messages = self.ge(
            self._get_cfg_enforced_properties({'ffs:test': 'shu'}))
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': ['ffs-1'],
                                     'properties': {
                                 'ffs:main': 'off',
                             }
                             }
                         }
                         })

        outgoing_messages.clear()
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {
                                'one': {'snapshots': ['ffs-1'],
                                        'properties': {
                                    'ffs:main': 'on',
                                    'readonly': 'on'
                                }
                                }
                         }
                         })
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'readonly': 'off'}
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'readonly': 'on'}
        })
        self.assertMsgEqual(outgoing_messages[2], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:test': 'shu'}
        })
        self.assertMsgEqual(outgoing_messages[3], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:test': 'shu'}
        })
        self.assertEqual(len(outgoing_messages), 4)

    def test_prune_on_startup_leaves_at_least_one_snapshot(self):
        cfg = self._get_test_config()

        def decide(ffs_name, snapshots):
            return []
        cfg.decide_snapshots_to_keep = decide
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2', '3']},
            'beta': {'one': ['1', '2', '3']},
        }, config=cfg)
        self.assertEqual(4, len(outgoing_messages))
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1',
            'to': 'alpha'
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '2',
            'to': 'alpha'
        })
        self.assertMsgEqual(outgoing_messages[2], {
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1',
            'to': 'beta'
        })
        self.assertMsgEqual(outgoing_messages[3], {
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '2',
            'to': 'beta'
        })

    def test_nested_roots_raise(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {}
                         })

        def inner():
            e.incoming_node({'msg': 'ffs_list',
                             'from': 'alpha',
                             'ffs': {
                                 'one': {'snapshots': [],
                                         'properties': {
                                    'ffs:main': 'on',
                                    'readonly': 'on',
                                    'ffs:root': True,
                                 }
                                 }
                             }
                             })

        self.assertRaises(engine.ManualInterventionNeeded, inner)
        self.assertTrue(e.faulted)


class PostStartupTests(EngineTests):

    def ge(self, cfg=None):
        nodes = collections.OrderedDict()
        nodes['alpha'] = {'storage_prefix': '/alpha',
                          'hostname': 'alpha', 'public_key': b'a'}
        nodes['beta'] = {'storage_prefix': '/beta',
                         'hostname': 'beta', 'public_key': b'a'}
        if cfg is None:
            cfg = self._get_test_config()
        if not hasattr(cfg, '_nodes'):
            cfg._nodes = nodes
        fm = FakeMessageSender()
        e = engine.Engine(
            default_config.CheckedConfig(cfg),
            fm
        )
        e.incoming_client({'msg': 'startup', })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'on',
                             }
                             }
                         }
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'off',
                             }
                             }
                         }
                         })
        fm.outgoing.clear()
        return e, fm.outgoing


class PreStartupRaisesTests(EngineTests):

    def test_new(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client({"msg": 'new', 'ffs': 'two'})
        self.assertRaises(engine.StartupNotDone, inner)


class NewTests(PostStartupTests):

    def test_new_raises_on_existing(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client(
                {"msg": 'new', 'ffs': 'one', 'targets': ['alpha']})
        self.assertRaises(ValueError, inner)

    def test_raises_without_targets(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client({"msg": 'new', 'ffs': 'one'})
        self.assertRaises(ValueError, inner)

    def test_raises_without_ffs(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client({"msg": 'new', 'targets': ['alpha']})
        self.assertRaises(ValueError, inner)

    def test_non_list_targets_raises(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client({'msg':
                               'new',
                               'ffs': 'two',
                               'targets': 'gamma'})

    def test_new_call_deceide_targets_on_empty_targets(self):
        cfg = self._get_test_config()
        called = [False]

        def decide_targets(ffs):
            called[0] = True
            return ['alpha']
        cfg.decide_targets = decide_targets
        e, outgoing_messages = self.ge(cfg)

        e.incoming_client({"msg": 'new', 'ffs': 'two', 'targets': []})
        self.assertTrue(called[0])

    def test_new_raises_on_non_existing_targets(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client(
                {"msg": 'new', 'ffs': 'one', 'targets': ['gamma']})
        self.assertRaises(engine.InvalidTarget, inner)

    def test_new_single_node(self):
        e, outgoing_messages = self.ge()
        e.incoming_client(
            {"msg": 'new', 'ffs': 'two', 'targets': ['alpha']})
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'new',
            'ffs': 'two',
            'properties': {
                'ffs:main': 'on',
                'readonly': 'off',
            }})
        self.assertTrue('two' in e.model)
        self.assertEqual(e.model['two']['_main'], 'alpha')
        self.assertEqual(e.model['two']['alpha'], {'_new': True})
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'alpha',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off'
             }
             }
        )
        self.assertFalse('_new' in e.model['two']['alpha'])
        self.assertEqual(e.model['two']['alpha'][
                         'properties']['ffs:main'], 'on')
        self.assertEqual(e.model['two']['alpha'][
                         'properties']['readonly'], 'off')

    def test_new_multi_nodes(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'new', 'ffs': 'two',
                           'targets': ['beta', 'alpha']})
        self.assertEqual(len(outgoing_messages), 2)
        self.assertMsgEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'new',
            'ffs': 'two',
            'properties': {
                'ffs:main': 'on',
                'readonly': 'off',
            }})
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'new',
            'ffs': 'two',
            'properties': {
                'ffs:main': 'off',
                'readonly': 'on',
            }})
        self.assertTrue('two' in e.model)
        self.assertEqual(e.model['two']['_main'], 'beta')
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'beta',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off'
             }
             }
        )

        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'alpha',
             'properties': {
                 'ffs:main': 'off',
                 'readonly': 'on'
             }
             }
        )
        self.assertEqual(e.model['two']['beta'][
                         'properties']['ffs:main'], 'on')
        self.assertEqual(e.model['two']['beta'][
                         'properties']['readonly'], 'off')

        self.assertEqual(e.model['two']['alpha'][
                         'properties']['ffs:main'], 'off')
        self.assertEqual(e.model['two']['alpha'][
                         'properties']['readonly'], 'on')

    def test_name_ok_callback(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'new', 'ffs': 'Three',
                           'targets': ['beta', 'alpha']})

        cfg = self._get_test_config()
        cfg.accepted_ffs_name = lambda ffs: ffs[0] != ffs[0].upper()
        e, outgoing_messages = self.ge(cfg)

        def inner():
            e.incoming_client({"msg": 'new', 'ffs': 'Three',
                               'targets': ['beta', 'alpha']})
        self.assertRaises(ValueError, inner)

    def test_new_dual_main_faults(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'new', 'ffs': 'two',
                           'targets': ['beta', 'alpha']})
        self.assertEqual(len(outgoing_messages), 2)
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'beta',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off'
             }
             }
        )

        def inner():
            e.incoming_node(
                {'msg': 'new_done',
                 'ffs': 'two',
                 'from': 'alpha',
                 'properties': {
                     'ffs:main': 'on',
                     'readonly': 'off'
                 }
                 }
            )
        self.assertRaises(engine.CodingError, inner)
        self.assertTrue(e.faulted)

    def test_capture_while_new(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'new', 'ffs': 'two',
                           'targets': ['beta', 'alpha']})
        self.assertEqual(len(outgoing_messages), 2)
        # any node in new state prevents capture

        def inner():
            e.incoming_client({'msg': 'capture', 'ffs': 'two'})
        self.assertRaises(ValueError, inner)
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'beta',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off'
             }
             }
        )
        self.assertRaises(ValueError, inner)
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'alpha',
             'properties': {
                 'ffs:main': 'off',
                 'readonly': 'on'
             }
             }
        )
        outgoing_messages.clear()
        e.incoming_client({'msg': 'capture', 'ffs': 'two'})
        self.assertEqual(1, len(outgoing_messages))

    def test_remove_while_new(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'new', 'ffs': 'two',
                           'targets': ['beta']})
        self.assertEqual(len(outgoing_messages), 1)
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'beta',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off'
             }
             }
        )
        self.assertFalse('_new' in e.model['two']['beta'])
        self.assertFalse('alpha' in e.model['two'])
        e.incoming_client(
            {'msg': 'add_targets', 'ffs': 'two', 'targets': ['alpha']})
        self.assertTrue('alpha' in e.model['two'])
        self.assertTrue(e.model['two']['alpha']['_new'])

        def inner():
            e.incoming_client(
                {'msg': 'remove_target', 'ffs': 'two', 'target': 'alpha'})
        self.assertRaises(engine.NewInProgress, inner)

    def test_remove_while_new_unrelated(self):
        cfg = self._get_test_config()
        nodes = {}
        nodes['alpha'] = {'storage_prefix': '/alpha',
                          'hostname': 'alpha', 'public_key': b'a'}
        nodes['beta'] = {'storage_prefix': '/beta',
                         'hostname': 'beta', 'public_key': b'a'}
        nodes['gamma'] = {'storage_prefix': '/gamma',
                          'hostname': 'gamma', 'public_key': b'a'}
        cfg._nodes = nodes
        e, outgoing_messages = self.ge(cfg)
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'gamma',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'off',
                             }
                             }
                         }
                         })
        outgoing_messages.clear()
        e.incoming_client({"msg": 'new', 'ffs': 'two',
                           'targets': ['beta', 'alpha', 'gamma']})
        self.assertEqual(len(outgoing_messages), 3)
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'beta',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off'
             }
             }
        )
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'gamma',
             'properties': {
                 'ffs:main': 'off',
                 'readonly': 'on'
             }
             }
        )

        self.assertFalse('_new' in e.model['two']['beta'])
        self.assertFalse('_new' in e.model['two']['gamma'])
        self.assertTrue('_new' in e.model['two']['alpha'])
        e.incoming_client(
            {'msg': 'remove_target', 'ffs': 'two', 'target': 'gamma'})
        self.assertTrue(e.model['two']['gamma'] == {'removing': True})

    def test_new_while_removing(self):
        e, outgoing_messages = self.ge()
        e.incoming_client(
            {'msg': "remove_target", 'ffs': 'one', 'target': 'alpha'})
        self.assertTrue(e.model['one']['alpha'] == {'removing': True})

        def inner():
            e.incoming_client(
                {'msg': 'add_targets', 'ffs': 'one', 'targets': ['alpha']})
        self.assertRaises(engine.RemoveInProgress, inner)

    def test_new_sets_default_properties(self):
        cfg = self._get_test_config()
        cfg.get_default_properties = lambda: {
            'ffs:test': 23}  # also tests stringification
        e, outgoing_messages = self.ge(cfg)
        e.incoming_client({'msg': "new", 'ffs': 'two', 'targets': ['alpha']})
        self.assertEqual(1, len(outgoing_messages))
        self.assertEqual(outgoing_messages[0]['properties'], {
            'ffs:main': 'on',
            'readonly': 'off',
            'ffs:test': '23'
        })

    def test_new_if_parent_is_not_on_target_fails(self):
        raise NotImplementedError()


def remove_snapshot_from_message(msg):
    msg = msg.copy()
    if 'snapshot' in msg:
        del msg['snapshot']
    return msg


class CaptureTest(PostStartupTests):

    def test_pruning_after_capture(self):
        cfg = self._get_test_config()

        def decide(sffs, snapshots):
            return snapshots[-2:]
        cfg.decide_snapshots_to_keep = decide
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['b', 'a', ]},
            'beta':  {'one': ['b', 'a', ]},
        }, config=cfg)
        self.assertFalse(outgoing_messages)
        e.incoming_client({'msg': 'capture', 'ffs': 'one'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]), {
            'msg': 'capture',
            'ffs': 'one',
            'to': 'alpha',
        })
        sn = outgoing_messages[0]['snapshot']
        e.incoming_node({'msg': 'capture_done',
                         'from': 'alpha',
                         'ffs': 'one',
                         'snapshot': sn
                         })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[1], {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'ffs': 'one',
            'snapshot': sn,
            'target_host': 'beta'
        })
        self.assertMsgEqual(outgoing_messages[2], {
            'msg': 'remove_snapshot',
            'to': 'alpha',
            'ffs': 'one',
            'snapshot': 'b'
        })
        self.assertEqual(len(outgoing_messages), 3)
        self.assertEqual(e.model['one']['alpha']['snapshots'], ['a', sn])
        outgoing_messages.clear()
        e.incoming_node({"msg": "send_snapshot_done",
                         'from': 'alpha',
                         'target_node': 'beta',
                         'ffs': 'one',
                         'snapshot': sn
                         })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove_snapshot',
            'to': 'beta',
            'ffs': 'one',
            'snapshot': 'b'
        })

    def test_pruning_after_capture_main_first_then_multiple_in_reply_order(self):
        cfg = self._get_test_config()
        cfg.decide_snapshots_to_keep = lambda ffs, snapshots: snapshots[-2:]
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['b', 'a', ]},
            'alpha': {'one': ['b', 'a', ]},
            'gamma': {'one': ['b', 'a']},
        }, config=cfg)
        self.assertFalse(outgoing_messages)
        e.incoming_client({'msg': 'capture', 'ffs': 'one'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]), {
            'msg': 'capture',
            'ffs': 'one',
            'to': 'beta',
        })
        sn = outgoing_messages[0]['snapshot']
        e.incoming_node({'msg': 'capture_done',
                         'from': 'beta',
                         'ffs': 'one',
                         'snapshot': sn
                         })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[1], {
            'msg': 'send_snapshot',
            'to': 'beta',
            'ffs': 'one',
            'snapshot': sn,
            'target_host': 'alpha'
        })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[2], {
            'msg': 'send_snapshot',
            'to': 'beta',
            'ffs': 'one',
            'snapshot': sn,
            'target_host': 'gamma'
        })

        self.assertMsgEqual(outgoing_messages[3], {
            'msg': 'remove_snapshot',
            'to': 'beta',
            'ffs': 'one',
            'snapshot': 'b'
        })
        self.assertEqual(len(outgoing_messages), 4)
        outgoing_messages.clear()
        e.incoming_node({
            'msg': 'send_snapshot_done',
            'from': 'beta',
            'ffs': 'one',
            'target_host': 'alpha',
            'target_node': 'alpha',
            'snapshot': sn
        })
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove_snapshot',
            'to': 'alpha',
            'ffs': 'one',
            'snapshot': 'b'
        })
        self.assertEqual(len(outgoing_messages), 1)

        outgoing_messages.clear()
        e.incoming_node({
            'msg': 'send_snapshot_done',
            'from': 'beta',
            'ffs': 'one',
            'target_node': 'gamma',
            'target_host': 'gamma',
            'snapshot': sn
        })

        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove_snapshot',
            'to': 'gamma',
            'ffs': 'one',
            'snapshot': 'b'
        })
        self.assertEqual(len(outgoing_messages), 1)

    def test_pruning_after_capture_always_leaves_at_least_one_snapshot(self):
        cfg = self._get_test_config()

        def decide(sffs, snapshots):
            return []
        cfg.decide_snapshots_to_keep = decide
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['a', ]},
            'beta':  {'one': ['a', ]},
        }, config=cfg)
        self.assertFalse(outgoing_messages)
        e.incoming_client({'msg': 'capture', 'ffs': 'one'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]), {
            'msg': 'capture',
            'ffs': 'one',
            'to': 'alpha',
        })
        sn = outgoing_messages[0]['snapshot']
        e.incoming_node({'msg': 'capture_done',
                         'from': 'alpha',
                         'ffs': 'one',
                         'snapshot': sn
                         })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[1], {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'ffs': 'one',
            'snapshot': sn,
            'target_host': 'beta'
        })
        self.assertMsgEqual(outgoing_messages[2], {
            'msg': 'remove_snapshot',
            'to': 'alpha',
            'ffs': 'one',
            'snapshot': 'a'
        })
        self.assertEqual(e.model['one']['alpha']['snapshots'], [sn])
        self.assertEqual(len(outgoing_messages), 3)
        # prune on targets only happens after sucessfull snapshot_done
        outgoing_messages.clear()
        e.incoming_node({
            'msg': 'send_snapshot_done',
            'from': 'alpha',
            'ffs': 'one',
            'target_host': 'beta',
            'target_node': 'beta',
            'snapshot': sn
        })

        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove_snapshot',
            'to': 'beta',
            'ffs': 'one',
            'snapshot': 'a'
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertEqual(e.model['one']['beta']['snapshots'], [sn])
        self.assertEqual(e.model['one']['alpha']['snapshots'], [sn])

    def test_never_prune_snapshots_in_outgoing_que(self):
        cfg = self._get_test_config()

        def decide(sffs, snapshots):
            return []
        cfg.decide_snapshots_to_keep = decide
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['a', ]},
            'beta':  {'one': ['a', ]},
            'gamma': {}
        }, config=cfg)
        self.assertFalse(outgoing_messages)
        e.incoming_client(
            {'msg': 'add_targets', 'ffs': 'one', 'targets': ['gamma']})
        outgoing_messages.clear()
        e.incoming_node({
            'msg': 'new_done',
            'from': 'gamma',
            'ffs': 'one',
            'properties': {'ffs:main': 'off', 'readonly': 'on'}
        })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[0], {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'ffs': 'one',
            'snapshot': 'a',
            'target_host': 'gamma'
        })

        e.incoming_client({'msg': 'capture', 'ffs': 'one'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[1]), {
            'msg': 'capture',
            'ffs': 'one',
            'to': 'alpha',
        })
        sn = outgoing_messages[1]['snapshot']

        e.incoming_node({'msg': 'capture_done',
                         'from': 'alpha',
                         'ffs': 'one',
                         'snapshot': sn
                         })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[2], {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'ffs': 'one',
            'snapshot': sn,
            'target_host': 'beta'
        })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[3], {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'ffs': 'one',
            'snapshot': sn,
            'target_host': 'gamma'
        })
        # no removal of a at this point in time.
        self.assertEqual(len(outgoing_messages), 4)
        e.incoming_node({'msg': 'send_snapshot_done',
                         'from': 'alpha',
                         'ffs': 'one',
                         'snapshot': 'a',
                         'target_host': 'gamma',
                         'target_node': 'gamma',
                         })

        # now at this point, we can remove the a snapshot from alpha
        # but not yet from beta or gamma
        # because both would not have a snapshot remaining otherwise
        self.assertMsgEqual(outgoing_messages[4], {
            'msg': 'remove_snapshot',
            'to': 'alpha',
            'ffs': 'one',
            'snapshot': 'a'
        })
        self.assertEqual(len(outgoing_messages), 5)
        e.incoming_node({'msg': 'send_snapshot_done',
                         'from': 'alpha',
                         'ffs': 'one',
                         'snapshot': sn,
                         'target_host': 'beta',
                         'target_node': 'beta',
                         })

        self.assertMsgEqual(outgoing_messages[5], {
            'msg': 'remove_snapshot',
            'to': 'beta',
            'ffs': 'one',
            'snapshot': 'a'
        })
        self.assertEqual(len(outgoing_messages), 6)

        e.incoming_node({'msg': 'send_snapshot_done',
                         'from': 'alpha',
                         'ffs': 'one',
                         'snapshot': sn,
                         'target_host': 'gamma',
                         'target_node': 'gamma',
                         })

        self.assertMsgEqual(outgoing_messages[6], {
            'msg': 'remove_snapshot',
            'to': 'gamma',
            'ffs': 'one',
            'snapshot': 'a'
        })
        self.assertEqual(len(outgoing_messages), 7)

    def test_capture_sends_capture_message(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'capture', 'ffs': 'one'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]),
                            {
            'to': 'beta',
            'msg': 'capture',
            'ffs': 'one'
        }
        )

    def test_unexpected_capture_explodes(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'capture', 'ffs': 'one'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]),
                            {
            'to': 'beta',
            'msg': 'capture',
            'ffs': 'one'
        }
        )
        outgoing_messages.clear()

        def inner():
            e.incoming_node({'msg': 'capture_done',
                             'from': 'alpha',
                             'ffs': 'one',
                             'snapshot': '1243'
                             })
        self.assertRaises(engine.ManualInterventionNeeded, inner),
        self.assertTrue(e.faulted)

    def test_capture_done_triggers_sync(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'capture', 'ffs': 'one'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]),
                            {
            'to': 'beta',
            'msg': 'capture',
            'ffs': 'one'
        }
        )
        snapshot_name = outgoing_messages[0]['snapshot']
        outgoing_messages.clear()
        e.incoming_node({'msg': 'capture_done',
                         'from': 'beta',
                         'ffs': 'one',
                         'snapshot': snapshot_name
                         })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[0],
                                         {
            'to': 'beta',
            'msg': 'send_snapshot',
            'ffs': 'one',
            'target_host': 'alpha',
            'snapshot': snapshot_name,
        }

        )
        outgoing_messages.clear()
        e.incoming_node({'msg': 'send_snapshot_done',
                         'from': 'beta',
                         'ffs': 'one',
                         'target_node': 'alpha',
                         'target_host': 'alpha',
                         'snapshot': snapshot_name,
                         })
        self.assertTrue(snapshot_name in e.model['one']['alpha']['snapshots'])

    def test_capture_done_adds_to_snapshot_list(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'capture', 'ffs': 'one'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]),
                            {
            'to': 'beta',
            'msg': 'capture',
            'ffs': 'one'
        }
        )
        snapshot_name = outgoing_messages[0]['snapshot']
        outgoing_messages.clear()
        e.incoming_node({'msg': 'capture_done',
                         'from': 'beta',
                         'ffs': 'one',
                         'snapshot': snapshot_name
                         })
        self.assertTrue(snapshot_name in e.model['one']['beta']['snapshots'])

    def test_capture_with_postfix(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'capture', 'ffs': 'one', 'postfix': 'test'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]),
                            {
            'to': 'beta',
            'msg': 'capture',
            'ffs': 'one'
        }
        )
        self.assertTrue(outgoing_messages[0]['snapshot'].endswith('-test'))

    def test_capture_with_triggers_matching_syncs(self):
        config = collections.OrderedDict()
        config['alpha'] = {'_one': ['1']}
        config['beta'] = {'one': ['1']}
        config['gamma'] = {'one': ['1', ('ffs:postfix_only', 'test')]}

        e, outgoing_messages = self.get_engine(config)
        e.incoming_client({"msg": 'capture', 'ffs': 'one', 'postfix': 'test'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]),
                            {
            'to': 'alpha',
            'msg': 'capture',
            'ffs': 'one'
        }
        )
        snapshot_test = outgoing_messages[0]['snapshot']
        self.assertTrue(snapshot_test.endswith('-test'))
        e.incoming_node({
            'msg': 'capture_done',
            'from': 'alpha',
            'ffs': 'one',
            'snapshot': snapshot_test
        })

        self.assertMsgEqualMinusSnapshot(outgoing_messages[1], {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'target_host': 'beta',
            'ffs': 'one',
            'snapshot': snapshot_test,
        }
        )
        self.assertMsgEqualMinusSnapshot(outgoing_messages[2],
                                         {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'target_host': 'gamma',
            'ffs': 'one',
            'snapshot': snapshot_test
        }
        )

        e.incoming_client({"msg": 'capture', 'ffs': 'one'})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[3]),
                            {
            'to': 'alpha',
            'msg': 'capture',
            'ffs': 'one'
        })
        snapshot_no_test = outgoing_messages[3]['snapshot']
        outgoing_messages.clear()
        e.incoming_node({
            'msg': 'capture_done',
            'from': 'alpha',
            'ffs': 'one',
            'snapshot': snapshot_no_test
        })

        self.assertMsgEqualMinusSnapshot(outgoing_messages[0],
                                         {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'target_host': 'beta',
            'ffs': 'one',
            'snapshot': snapshot_no_test
        }
        )
        self.assertEqual(len(outgoing_messages), 1)

    def test_no_capture_during_rename(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })
        e.incoming_client({
            'msg': 'rename', 'ffs': 'one', 'new_name': 'two'
        })

        def inner():
            e.incoming_client({
                'msg': 'capture', 'ffs': 'one'
            })
        self.assertRaises(engine.RenameInProgress, inner)


class RemoveTarget(PostStartupTests):

    def test_basic(self):
        e, outgoing_messages = self.ge()
        self.assertTrue('one' in e.model)
        self.assertFalse(e.model['one']['alpha']['removing'])
        e.incoming_client(
            {"msg": 'remove_target', 'ffs': 'one', 'target': 'alpha'})
        self.assertTrue(e.model['one']['alpha']['removing'])
        self.assertMsgEqual(outgoing_messages[0],
                            {'msg': 'remove',
                             'ffs': 'one',
                             'to': 'alpha'}
                            )
        self.assertEqual(len(outgoing_messages), 1)
        e.incoming_node({
            'from': 'alpha',
            'msg': 'remove_done',
            'ffs': 'one',
        })
        self.assertFalse('alpha' in e.model['one'])

    def test_raises_main(self):
        e, outgoing_messages = self.ge()
        self.assertTrue('one' in e.model)
        self.assertFalse(e.model['one']['alpha']['removing'])

        def inner():
            e.incoming_client(
                {"msg": 'remove_target', 'ffs': 'one', 'target': 'beta'})
        self.assertRaises(ValueError, inner)

    def test_raises_missing_target(self):
        e, outgoing_messages = self.ge()
        self.assertTrue('one' in e.model)
        self.assertFalse(e.model['one']['alpha']['removing'])

        def inner():
            e.incoming_client(
                {"msg": 'remove_target', 'ffs': 'one', 'target': 'gamma'})
        self.assertRaises(engine.InvalidTarget, inner)

    def test_raises_missing_ffs(self):
        e, outgoing_messages = self.ge()
        self.assertTrue('one' in e.model)
        self.assertFalse(e.model['one']['alpha']['removing'])

        def inner():
            e.incoming_client(
                {"msg": 'remove_target', 'ffs': 'two', 'target': 'alpha'})
        self.assertRaises(ValueError, inner)

    def test_remove_while_pulling(self):
        """What will actually happen?"""
        # target is busy is what the ffs destroy will say...
        e, outgoing_messages = self.ge()
        e.incoming_client(
            {"msg": 'remove_target', 'ffs': 'one', 'target': 'alpha'})
        self.assertTrue(e.model['one']['alpha']['removing'])
        self.assertMsgEqual(outgoing_messages[0],
                            {'msg': 'remove',
                             'ffs': 'one',
                             'to': 'alpha'}
                            )
        self.assertEqual(len(outgoing_messages), 1)

        e.incoming_node({
            'from': 'alpha',
            'msg': 'remove_failed',
            'reason': 'target_is_busy',
            'ffs': 'one',
        })

    def test_remove_asap_triggers_removal_upon_startup(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', ('ffs:main', 'on'), ]},
            'beta':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', ), ('ffs:remove_asap', 'on')]},
        })
        self.assertTrue(e.startup_done)
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove',
            'ffs': 'one',
            'to': 'beta'
        })
        self.assertTrue('beta' in e.model['one'])
        self.assertTrue(e.model['one']['beta']['removing'])

    def test_remove_asap_on_main_triggers_fault(self):
        def inner():
            e, outgoing_messages = self.get_engine({
                'alpha': {'_one': ['1', ('ffs:main', 'on'), ('ffs:remove_asap', 'on')]},
                'beta':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', )]},
            })
        self.assertRaises(engine.ManualInterventionNeeded, inner)

    def test_readding_while_removing_causes_user_error(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', ('ffs:main', 'on'), ]},
            'beta':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', ), ('ffs:remove_asap', 'on')]},
        })
        self.assertTrue(e.startup_done)
        self.assertEqual(len(outgoing_messages), 1)

        def inner():
            e.incoming_client(
                {'msg': 'add_targets', 'ffs': 'one', 'target': ['beta']})
        # technically, an 'already in list' error
        self.assertRaises(ValueError, inner)

    def test_no_sync_during_startup_to_remove_asap(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2', ('ffs:main', 'on'), ]},
            'beta':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', ), ('ffs:remove_asap', 'on')]},
        })
        self.assertTrue(e.startup_done)
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove',
            'ffs': 'one',
            'to': 'beta'
        })
        self.assertTrue('beta' in e.model['one'])
        self.assertTrue(e.model['one']['beta']['removing'])

    def test_no_syncs_to_currently_removing_target(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', ('ffs:main', 'on'), ]},
            'beta':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', ), ('ffs:remove_asap', 'on')]},
            'gamma': {'one': ['1']}
        })
        self.assertTrue(e.startup_done)
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove',
            'ffs': 'one',
            'to': 'beta'
        })
        self.assertTrue('beta' in e.model['one'])
        self.assertTrue(e.model['one']['beta']['removing'])
        snapshot = e.incoming_client(
            {'msg': 'capture', 'ffs': 'one'})['snapshot']
        outgoing_messages.clear()
        e.incoming_node({
            'from': 'alpha',
            'msg': 'capture_done',
            'ffs': 'one',
            'snapshot': snapshot,
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqualMinusSnapshot(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'send_snapshot',
            'target_host': 'gamma',
            'ffs': 'one',
            'snapshot': snapshot,
        })

    def test_cant_remove_during_move(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', ('ffs:main', 'on'), ]},
            'beta':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', ), ]},
            'gamma':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', ), ]},
        })
        self.assertTrue(e.startup_done)
        self.assertEqual(len(outgoing_messages), 0)
        e.incoming_client({'msg': 'move_main', 'ffs': 'one', 'target': 'beta'})
        # this one should explode anyhow... since we're removing the main

        def inner():
            e.incoming_client(
                {'msg': 'remove_target', 'ffs': 'one', 'target': 'gamma'})
        self.assertRaises(engine.MoveInProgress, inner)

        def inner():
            e.incoming_client(
                {'msg': 'remove_target', 'ffs': 'one', 'target': 'beta'})
        self.assertRaises(engine.MoveInProgress, inner)

        def inner():
            e.incoming_client(
                {'msg': 'remove_target', 'ffs': 'one', 'target': 'gamma'})
        self.assertRaises(engine.MoveInProgress, inner)

    def test_cant_move_during_remove_if_target_is_being_removed(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', ('ffs:main', 'on'), ]},
            'beta':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', ), ('ffs:remove_asap', 'on')]},
            'gamma':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', ), ]},
        })
        self.assertTrue(e.startup_done)
        self.assertEqual(len(outgoing_messages), 1)

        def inner():
            e.incoming_client(
                {'msg': 'move_main', 'ffs': 'one', 'target': 'beta'})
        self.assertRaises(ValueError, inner)

    def test_cant_move_during_remove_if_target_is_not_the_one_being_removed(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', ('ffs:main', 'on'), ]},
            'beta':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', ), ('ffs:remove_asap', 'on')]},
            'gamma':  {'one': ['1', ('ffs:main', 'off'), ('readonly', 'on', ), ]},
        })
        self.assertTrue(e.startup_done)
        self.assertEqual(len(outgoing_messages), 1)

        def inner():
            e.incoming_client(
                {'msg': 'move_main', 'ffs': 'one', 'target': 'gamma'})
        self.assertRaises(ValueError, inner)

    def test_raises_invalid_target(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client(
                {'msg': 'remove_target', 'ffs': 'one', 'target': 'shu'})
        self.assertRaises(engine.InvalidTarget, inner)

    def test_double_delete(self):
        e, outgoing_messages = self.ge()
        e.incoming_client(
            {'msg': 'remove_target', 'ffs': 'one', 'target': 'alpha'})
        self.assertTrue('removing' in e.model['one']['alpha'])
        # ignored
        e.incoming_client(
            {'msg': 'remove_target', 'ffs': 'one', 'target': 'alpha'})
        # now remove...
        e.incoming_node({'msg': 'remove_done', 'ffs': 'one', 'from': 'alpha'})

        def inner():
            e.incoming_client(
                {'msg': 'remove_target', 'ffs': 'one', 'target': 'alpha'})
        self.assertRaises(ValueError, inner)

    def test_find_node(self):
        def find_node(name):
            if name == 'shu':
                return 'gamma'
            else:
                return default_config.DefaultConfig.find_node(cfg, name)
        cfg = self._get_test_config()
        cfg.find_node = find_node
        nodes = {}
        nodes['alpha'] = {'storage_prefix': '/alpha',
                          'hostname': 'alpha', 'public_key': b'a'}
        nodes['beta'] = {'storage_prefix': '/beta',
                         'hostname': 'beta', 'public_key': b'a'}
        nodes['gamma'] = {'storage_prefix': '/gamma',
                          'hostname': 'gamma', 'public_key': b'a'}
        cfg._nodes = nodes
        e, outgoing_messages = self.ge(cfg)
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'gamma',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'off',
                             }
                             }
                         }
                         })
        self.assertTrue('gamma' in e.model['one'])
        self.assertFalse(e.model['one']['gamma'].get('removing', False))
        outgoing_messages.clear()
        e.incoming_client(
            {'msg': 'remove_target', 'ffs': 'one', 'target': 'shu'})
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove', 'ffs': 'one', 'to': 'gamma'})
        self.assertEqual(e.model['one']['gamma'], {'removing': True})


class AddTargetTests(PostStartupTests):

    def ge(self, cfg=None):
        nodes = collections.OrderedDict()
        nodes['alpha'] = {'storage_prefix': '/alpha',
                          'hostname': 'alpha', 'public_key': b'a'}
        nodes['beta'] = {'storage_prefix': '/beta',
                         'hostname': 'beta', 'public_key': b'a'}
        nodes['gamma'] = {'storage_prefix': '/gamma',
                          'hostname': 'gamma', 'public_key': b'a'}
        fm = FakeMessageSender()
        if cfg is None:
            cfg = self._get_test_config()
        if not hasattr(cfg, '_nodes'):
            cfg._nodes = nodes

        e = engine.Engine(
            default_config.CheckedConfig(cfg),
            fm
        )
        e.incoming_client({'msg': 'startup', })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': ['1', '2'],
                                     'properties': {
                                 'ffs:main': 'on',
                             }
                             }
                         }
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {
                             'one': {'snapshots': ['1'],
                                     'properties': {
                                 'ffs:main': 'off',
                             }
                             }
                         }
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'gamma',
                         'ffs': {}
                         })

        fm.outgoing.clear()
        return e, fm.outgoing

    def test_non_list_targets_raises(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client({'msg':
                               'add_targets',
                               'ffs': 'one',
                               'targets': 'gamma'})
        self.assertRaises(ValueError, inner)

    def test_empty_list_raises(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client({'msg':
                               'add_targets',
                               'ffs': 'one',
                               'targets': []})
        self.assertRaises(ValueError, inner)

    def test_non_target_raises(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client({'msg':
                               'add_targets',
                               'ffs': 'one',
                               'targets': ['shu']})
        self.assertRaises(engine.InvalidTarget, inner)

    def test_basic(self):
        e, outgoing_messages = self.ge()
        self.assertTrue('one' in e.model)
        self.assertFalse('gamma' in e.model['one'])
        e.incoming_client({'msg':
                           'add_targets',
                           'ffs': 'one',
                           'targets': ['gamma']})
        self.assertMsgEqual(outgoing_messages[0],
                            {'msg': 'new',
                             'to': 'gamma',
                             'ffs': 'one',
                             'properties': {'ffs:main': 'off', 'readonly': 'on'}}
                            )
        self.assertEqual(1, len(outgoing_messages))
        outgoing_messages.clear()
        e.incoming_node({'msg': 'new_done',
                         'from': 'gamma',
                         'ffs': 'one',
                         'properties': {
                             'ffs:main': 'off',
                             'readonly': 'on'
                         }

                         })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[0], {
            'msg': 'send_snapshot',
            'ffs': 'one',
            'to': 'beta',
            'target_host': 'gamma',
            'snapshot': '1'
        })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[1], {
            'msg': 'send_snapshot',
            'ffs': 'one',
            'to': 'beta',
            'target_host': 'gamma',
            'snapshot': '2'
        })
        self.assertEqual(len(outgoing_messages), 2)

    def test_add_multiple_targets(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({'msg': 'new', 'ffs': 'two', 'targets': ['beta']})
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'beta',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off'
             }
             }
        )
        outgoing_messages.clear()
        e.incoming_client({'msg': 'add_targets', 'ffs': 'two',
                           'targets': ['gamma', 'alpha']})
        self.assertEqual(2, len(outgoing_messages))
        # alphabetical order!
        self.assertMsgEqual(outgoing_messages[0],
                            {
            'to': 'alpha',
            'msg': 'new',
            'ffs': 'two',
            'properties': {'ffs:main': 'off', 'readonly': 'on'}
        }
        )
        self.assertMsgEqual(outgoing_messages[1],
                            {
            'to': 'gamma',
            'msg': 'new',
            'ffs': 'two',
            'properties': {'ffs:main': 'off', 'readonly': 'on'}
        }
        )

    def test_add_target_find_node(self):
        def find_node(name):
            if name == 'shu':
                return 'gamma'
            else:
                return default_config.DefaultConfig.find_node(cfg, name)
        cfg = self._get_test_config()
        cfg.find_node = find_node
        e, outgoing_messages = self.ge(cfg)
        e.incoming_client({'msg': 'new', 'ffs': 'two', 'targets': ['beta']})
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'beta',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off'
             }
             }
        )
        outgoing_messages.clear()
        e.incoming_client(
            {'msg': 'add_targets', 'ffs': 'two', 'targets': ['shu']})
        self.assertEqual(len(outgoing_messages), 1)
        self.assertTrue(e.model['two']['gamma']['_new'])

    def test_add_target_sends_only_snapshots_positive_in_config_to_send(self):
        nodes = collections.OrderedDict()
        nodes['alpha'] = {'storage_prefix': '/alpha',
                          'hostname': 'alpha', 'public_key': b'a'}
        nodes['beta'] = {'storage_prefix': '/beta',
                         'hostname': 'beta', 'public_key': b'a'}
        nodes['gamma'] = {'storage_prefix': '/gamma',
                          'hostname': 'gamma', 'public_key': b'a'}
        fm = FakeMessageSender()
        cfg = self._get_test_config()

        def decide_snapshots_to_send(ffs, snapshots):
            return [x for x in snapshots if x.startswith('send-')]
        cfg.decide_snapshots_to_send = decide_snapshots_to_send
        cfg._nodes = nodes
        cfg.do_deploy = lambda: False

        e = engine.Engine(
            default_config.CheckedConfig(cfg),
            fm
        )
        e.incoming_client({'msg': 'startup', })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': ['1', 'send-2', 'nosend-2', 'send-3'],
                                     'properties': {
                                 'ffs:main': 'on', 'readonly': 'off',
                             }
                             }
                         }
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {
                             'one': {'snapshots': ['1'],
                                     'properties': {
                                 'ffs:main': 'off',
                                 'readonly': 'on',
                             }
                             }
                         }
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'gamma',
                         'ffs': {}
                         })
        self.assertMsgEqualMinusSnapshot(fm.outgoing[-2], {
            'msg': 'send_snapshot',
            'ffs': 'one',
            'snapshot': 'send-2',
            'to': 'beta',
            'target_host': 'alpha',
        })
        self.assertMsgEqualMinusSnapshot(fm.outgoing[-1], {
            'msg': 'send_snapshot',
            'ffs': 'one',
            'snapshot': 'send-3',
            'to': 'beta',
            'target_host': 'alpha',
        })
        fm.outgoing.clear()
        e.incoming_client(
            {'msg': 'add_targets', 'ffs': 'one', 'targets': ['gamma']})
        self.assertEqual(len(fm.outgoing), 1)
        self.assertMsgEqual(fm.outgoing[0], {
            'msg': 'new',
            'ffs': 'one',
            'properties': {'ffs:main': 'off', 'readonly': 'on'},
            'to': 'gamma',
        })
        fm.outgoing.clear()
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'one',
             'from': 'gamma',
             'properties': {
                 'ffs:main': 'off',
                 'readonly': 'on'
             }
             }
        )
        self.assertEqual(len(fm.outgoing), 2)
        self.assertMsgEqualMinusSnapshot(fm.outgoing[0], {
            'msg': 'send_snapshot',
            'ffs': 'one',
            'snapshot': 'send-2',
            'to': 'beta',
            'target_host': 'gamma',
        })
        self.assertMsgEqualMinusSnapshot(fm.outgoing[1], {
            'msg': 'send_snapshot',
            'ffs': 'one',
            'snapshot': 'send-3',
            'to': 'beta',
            'target_host': 'gamma',
        })

    def test_capture_faults_if_new_snapshot_is_not_in_decide_to_send(self):
        nodes = collections.OrderedDict()
        nodes['alpha'] = {'storage_prefix': '/alpha',
                          'hostname': 'alpha', 'public_key': b'a'}
        nodes['beta'] = {'storage_prefix': '/beta',
                         'hostname': 'beta', 'public_key': b'a'}
        nodes['gamma'] = {'storage_prefix': '/gamma',
                          'hostname': 'gamma', 'public_key': b'a'}
        fm = FakeMessageSender()
        cfg = self._get_test_config()
        cfg._nodes = nodes
        cfg.do_deploy = lambda: False

        def decide_snapshots_to_send(ffs, snapshots):
            return [x for x in snapshots if x.startswith('send-')]
        cfg.decide_snapshots_to_send = decide_snapshots_to_send

        e = engine.Engine(
            default_config.CheckedConfig(cfg),
            fm
        )
        e.incoming_client({'msg': 'startup', })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'on', 'readonly': 'off',
                             }
                             }
                         }
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {}
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'gamma',
                         'ffs': {}
                         })
        self.assertEqual(len(fm.outgoing), 3)  # the list_ffs messages
        fm.outgoing.clear()
        e.incoming_client(
            {'msg': 'add_targets', 'ffs': 'one', 'targets': ['gamma']})
        self.assertMsgEqual(fm.outgoing[0], {
            'msg': 'new',
            'ffs': 'one',
            'properties': {'ffs:main': 'off', 'readonly': 'on'},
            'to': 'gamma',
        })
        fm.outgoing.clear()
        def inner():
            e.incoming_node(
                {'msg': 'new_done',
                'ffs': 'one',
                'from': 'gamma',
                'properties': {
                    'ffs:main': 'off',
                    'readonly': 'on'
                }
                }
            )  #which triggers a 'capture' message, see test_add_targets_no_snapshots_to_send_captures_after_add_target
        self.assertRaises(engine.ManualInterventionNeeded, inner)

    def test_add_targets_no_snapshots_to_send_captures_after_add_target(self):
        nodes = collections.OrderedDict()
        nodes['alpha'] = {'storage_prefix': '/alpha',
                          'hostname': 'alpha', 'public_key': b'a'}
        nodes['beta'] = {'storage_prefix': '/beta',
                         'hostname': 'beta', 'public_key': b'a'}
        nodes['gamma'] = {'storage_prefix': '/gamma',
                          'hostname': 'gamma', 'public_key': b'a'}
        fm = FakeMessageSender()
        cfg = self._get_test_config()
        cfg._nodes = nodes
        cfg.do_deploy = lambda: False

        e = engine.Engine(
            default_config.CheckedConfig(cfg),
            fm
        )
        e.incoming_client({'msg': 'startup', })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': [],
                                     'properties': {
                                 'ffs:main': 'on', 'readonly': 'off',
                             }
                             }
                         }
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {}
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'gamma',
                         'ffs': {}
                         })
        self.assertEqual(len(fm.outgoing), 3)  # the list_ffs messages
        fm.outgoing.clear()
        e.incoming_client(
            {'msg': 'add_targets', 'ffs': 'one', 'targets': ['gamma']})
        self.assertMsgEqual(fm.outgoing[0], {
            'msg': 'new',
            'ffs': 'one',
            'properties': {'ffs:main': 'off', 'readonly': 'on'},
            'to': 'gamma',
        })
        fm.outgoing.clear()
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'one',
             'from': 'gamma',
             'properties': {
                 'ffs:main': 'off',
                 'readonly': 'on'
             }
             }
        )
        self.assertEqual(len(fm.outgoing), 1)
        self.assertMsgEqual(remove_snapshot_from_message(fm.outgoing[0]), {
            'msg': 'capture',
            'ffs': 'one',
            'to': 'beta',
        })
        sn = fm.outgoing[0]['snapshot']
        e.incoming_node({
            'msg': 'capture_done',
            'from': 'beta',
            'ffs': 'one',
            'snapshot': sn,
        })
        self.assertEqual(len(fm.outgoing), 2)
        self.assertMsgEqualMinusSnapshot(fm.outgoing[1], {
            'msg': 'send_snapshot',
            'snapshot': sn,
            'ffs': 'one',
            'to': 'beta',
            'target_host': 'gamma'
        })

    def test_new_multiple_targets_does_not_capture_straight_away(self):
        # basically, the compaignon test to
        # test_add_targets_no_snapshots_to_send_captures_after_add_target(
        nodes = collections.OrderedDict()
        nodes['alpha'] = {'storage_prefix': '/alpha',
                          'hostname': 'alpha', 'public_key': b'a'}
        nodes['beta'] = {'storage_prefix': '/beta',
                         'hostname': 'beta', 'public_key': b'a'}
        nodes['gamma'] = {'storage_prefix': '/gamma',
                          'hostname': 'gamma', 'public_key': b'a'}
        fm = FakeMessageSender()
        cfg = self._get_test_config()
        cfg._nodes = nodes
        cfg.do_deploy = lambda: False

        def decide_snapshots_to_send(ffs, snapshots):
            return [x for x in snapshots if x.startswith('send-')]
        cfg.decide_snapshots_to_send = decide_snapshots_to_send

        e = engine.Engine(
            default_config.CheckedConfig(cfg),
            fm
        )
        e.incoming_client({'msg': 'startup', })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {}
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'alpha',
                         'ffs': {}
                         })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'gamma',
                         'ffs': {}
                         })
        self.assertEqual(len(fm.outgoing), 3)  # the list_ffs messages
        fm.outgoing.clear()
        e.incoming_client(
            {'msg': 'new', 'ffs': 'one', 'targets': ['beta', 'gamma']})
        self.assertMsgEqual(fm.outgoing[0], {
            'msg': 'new',
            'ffs': 'one',
            'properties': {'ffs:main': 'on', 'readonly': 'off'},
            'to': 'beta',
        })
        self.assertMsgEqual(fm.outgoing[1], {
            'msg': 'new',
            'ffs': 'one',
            'properties': {'ffs:main': 'off', 'readonly': 'on'},
            'to': 'gamma'
        })
        self.assertEqual(len(fm.outgoing), 2)
        fm.outgoing.clear()
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'one',
             'from': 'gamma',
             'properties': {
                 'ffs:main': 'off',
                 'readonly': 'on'
             }
             }
        )
        self.assertEqual(len(fm.outgoing), 0)
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'one',
             'from': 'beta',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off'
             }
             }
        )
        self.assertEqual(len(fm.outgoing), 0)

    def test_repeated_target_handled(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({'msg': 'new', 'ffs': 'two', 'targets': ['beta']})
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'beta',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off'
             }
             }
        )
        outgoing_messages.clear()
        e.incoming_client({'msg': 'add_targets', 'ffs': 'two',
                           'targets': ['gamma', 'gamma']})
        self.assertEqual(1, len(outgoing_messages))

    def test_add_target_default_properties(self):
        cfg = self._get_test_config()
        cfg.get_default_properties = lambda: {
            'ffs:test': 23}  # also tests stringification
        e, outgoing_messages = self.ge(cfg)
        e.incoming_client({'msg': "new", 'ffs': 'two', 'targets': ['alpha']})
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'alpha',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off',
                 'ffs:test': '23'
             }
             }
        )
        outgoing_messages.clear()
        e.incoming_client(
            {'msg': 'add_targets', 'ffs': 'two', 'targets': ['beta']})
        self.assertEqual(1, len(outgoing_messages))
        self.assertEqual(outgoing_messages[0]['properties'], {
            'ffs:main': 'off',
            'readonly': 'on',
            'ffs:test': '23'
        })

    def test_add_target_enforced_properties(self):
        cfg = self._get_test_config()
        cfg.get_enforced_properties = lambda: {
            'ffs:test': 24}  # also tests stringification
        e, outgoing_messages = self.ge(cfg)
        e.incoming_client({'msg': "new", 'ffs': 'two', 'targets': ['alpha']})
        e.incoming_node(
            {'msg': 'new_done',
             'ffs': 'two',
             'from': 'alpha',
             'properties': {
                 'ffs:main': 'on',
                 'readonly': 'off',
                 'ffs:test': '24'
             }
             }
        )
        outgoing_messages.clear()
        e.incoming_client(
            {'msg': 'add_targets', 'ffs': 'two', 'targets': ['beta']})
        self.assertEqual(1, len(outgoing_messages))
        self.assertEqual(outgoing_messages[0]['properties'], {
            'ffs:main': 'off',
            'readonly': 'on',
            'ffs:test': '24'
        })

    def test_add_target_if_parent_is_not_on_target_fails(self):
        raise NotImplementedError()

class MoveTest(PostStartupTests):

    def test_move_raises_on_moving_to_main(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ('ffs:test', '2')]},
            'gamma': {},
        })
        self.assertEqual(engine.model['one']['beta'][
                         'properties']['ffs:test'], '2')
        self.assertEqual(engine.model['one']['_main'], 'alpha')

        def inner():
            engine.incoming_client(
                {"msg": "move_main", 'ffs': 'one', 'target': 'alpha'})
        self.assertRaises(ValueError, inner)

    def test_move_raises_on_moving_to_non_target(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ('ffs:test', '2')]},
            'gamma': {},
        })
        self.assertEqual(e.model['one']['beta'][
                         'properties']['ffs:test'], '2')
        self.assertEqual(e.model['one']['_main'], 'alpha')

        def inner():
            e.incoming_client(
                {"msg": "move_main", 'ffs': 'one', 'target': 'gamma'})
        self.assertRaises(engine.InvalidTarget, inner)

    def test_move_raises_on_moving_to_non_existing_ffs(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ('ffs:test', '2')]},
            'gamma': {},
        })
        self.assertEqual(engine.model['one']['beta'][
                         'properties']['ffs:test'], '2')
        self.assertEqual(engine.model['one']['_main'], 'alpha')

        def inner():
            engine.incoming_client(
                {"msg": "move_main", 'ffs': 'two', 'target': 'gamma'})
        self.assertRaises(ValueError, inner)

    def test_no_move_during_new(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ]},
            'gamma': {},
        })
        e.incoming_client({'msg': 'new', 'ffs': 'two',
                           'targets': ['alpha', 'beta']})

        def inner():
            e.incoming_client(
                {"msg": "move_main", 'ffs': 'two', 'target': 'beta'})
        self.assertRaises(engine.NewInProgress, inner)

    def test_no_move_during_new_main_done(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ]},
            'gamma': {},
        })
        e.incoming_client({'msg': 'new', 'ffs': 'two',
                           'targets': ['alpha', 'beta']})
        e.incoming_node({"msg": 'new_done', 'from': 'alpha', 'ffs': 'two', 'properties': {
                        'ffs:main': 'on', 'readonly': 'off'}, 'snapshots': []})

        def inner():
            e.incoming_client(
                {"msg": "move_main", 'ffs': 'two', 'target': 'beta'})
        self.assertRaises(engine.NewInProgress, inner)

    def test_no_move_during_new_non_main_done(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ]},
            'gamma': {},
        })
        e.incoming_client({'msg': 'new', 'ffs': 'two',
                           'targets': ['alpha', 'beta']})
        e.incoming_node({"msg": 'new_done', 'from': 'beta', 'ffs': 'two', 'properties': {
                        'ffs:main': 'off', 'readonly': 'on'}, 'snapshots': []})

        def inner():
            e.incoming_client(
                {"msg": "move_main", 'ffs': 'two', 'target': 'beta'})
        self.assertRaises(engine.NewInProgress, inner)

    def test_no_move_during_remove(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ]},
            'gamma': {},
        })
        e.incoming_client({'msg': 'remove_target', 'ffs': 'one',
                           'target': 'beta'})

        def inner():
            e.incoming_client(
                {"msg": "move_main", 'ffs': 'one', 'target': 'beta'})
        self.assertRaises(engine.RemoveInProgress, inner)

    def test_no_move_during_rename(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'one': ['1']},
            'beta':  {'_one': ['1']},
            'gamma': {'one': ['1']},
        })
        self.assertEqual(0, len(outgoing_messages))
        e.incoming_client({"msg": 'rename',
                           'ffs': 'one',
                           'new_name': 'two'})

        def inner():
            e.incoming_client({
                "msg": "move_main",
                'ffs': 'one',
                'target': 'alpha'
            })
        self.assertRaises(engine.RenameInProgress, inner)

    def test_move_basic(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ('ffs:test', '2')]},
            'gamma': {},
        })
        # flow is as follows
        # 1 - set ffs:moving=target on old main, set read only.
        # 2 - capture on old main
        # 3 - replicate
        # 4 - set ffs:main = False on old main
        # 5 - set main and remove ro on new main
        # 6 - remove ffs:moving on old_main

        self.assertEqual(engine.model['one']['beta'][
                         'properties']['ffs:test'], '2')
        self.assertEqual(engine.model['one']['_main'], 'alpha')
        engine.incoming_client(
            {"msg": "move_main", 'ffs': 'one', 'target': 'beta'})
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'readonly': 'on', 'ffs:moving_to': 'beta'}
        })
        self.assertEqual(len(outgoing_messages), 1)
        engine.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'readonly': 'on', 'ffs:moving_to': 'beta'}
        })
        self.assertEqual(len(outgoing_messages), 2)
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[1]), {
            'to': 'alpha',
            'msg': 'capture',
            'ffs': 'one',
        })
        engine.incoming_node({
            'msg': 'capture_done',
            'from': 'alpha',
            'ffs': 'one',
            'snapshot': outgoing_messages[1]['snapshot'],
        })
        self.assertEqual(len(outgoing_messages), 3)
        self.assertMsgEqualMinusSnapshot(outgoing_messages[2], {
            'to': 'alpha',
            'msg': 'send_snapshot',
            'ffs': 'one',
            'target_node': 'beta',
            'target_host': 'beta',
            'snapshot': outgoing_messages[1]['snapshot']
        })
        engine.incoming_node({
            'msg': 'send_snapshot_done',
            'from': 'alpha',
            'target_host': 'beta',
            'target_node': 'beta',
            'ffs': 'one',
            'snapshot': outgoing_messages[1]['snapshot']
        })
        self.assertEqual(len(outgoing_messages), 4)
        self.assertMsgEqual(outgoing_messages[3], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:main': 'off'}
        })
        engine.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:main': 'off', 'ffs:moving_to': 'beta'}
        })
        self.assertEqual(len(outgoing_messages), 5)
        self.assertMsgEqual(outgoing_messages[4], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:main': 'on', 'readonly': 'off'}
        })
        engine.incoming_node({
            'msg': 'set_properties_done',
            'from': 'beta',
            'ffs': 'one',
            'properties': {'ffs:main': 'on', 'readonly': 'off'}
        })
        self.assertEqual(len(outgoing_messages), 6)
        self.assertMsgEqual(outgoing_messages[5], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:moving_to': '-'}
        })
        engine.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:moving_to': '-', 'ffs:main': 'off', 'readonly': 'on'}
        })
        self.assertEqual(engine.model['one']['_main'], 'beta')
        self.assertEqual(engine.model['one']['alpha'][
                         'properties']['ffs:main'], 'off')
        self.assertEqual(engine.model['one']['beta'][
                         'properties']['ffs:main'], 'on')
        self.assertEqual(engine.model['one']['alpha'][
                         'properties']['readonly'], 'on')
        self.assertEqual(engine.model['one']['beta'][
                         'properties']['readonly'], 'off')
        self.assertEqual(engine.model['one']['alpha'][
                         'properties']['ffs:moving_to'], '-')
        self.assertFalse('_moving' in engine.model['one'])

    def test_pruning_only_after_move(self):
        cfg = self._get_test_config()
        cfg.decide_snapshots_to_keep = lambda ffs, snapshots: []
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ]},
            'gamma': {},
        }, config=cfg)
        # flow is as follows
        # 1 - set ffs:moving=target on old main, set read only.
        # 2 - capture on old main
        # 3 - replicate
        # 4 - set ffs:main = False on old main
        # 5 - set main and remove ro on new main
        # 6 - remove ffs:moving on old_main

        self.assertEqual(engine.model['one']['_main'], 'alpha')
        engine.incoming_client(
            {"msg": "move_main", 'ffs': 'one', 'target': 'beta'})
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'readonly': 'on', 'ffs:moving_to': 'beta'}
        })
        self.assertEqual(len(outgoing_messages), 1)
        engine.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'readonly': 'on', 'ffs:moving_to': 'beta'}
        })
        self.assertEqual(len(outgoing_messages), 2)
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[1]), {
            'to': 'alpha',
            'msg': 'capture',
            'ffs': 'one',
        })
        engine.incoming_node({
            'msg': 'capture_done',
            'from': 'alpha',
            'ffs': 'one',
            'snapshot': outgoing_messages[1]['snapshot'],
        })
        self.assertEqual(len(outgoing_messages), 3)
        self.assertMsgEqualMinusSnapshot(outgoing_messages[2], {
            'to': 'alpha',
            'msg': 'send_snapshot',
            'ffs': 'one',
            'target_host': 'beta',
            'snapshot': outgoing_messages[1]['snapshot']
        })
        engine.incoming_node({
            'msg': 'send_snapshot_done',
            'from': 'alpha',
            'target_host': 'beta',
            'ffs': 'one',
            'snapshot': outgoing_messages[1]['snapshot'],
            'target_node': "beta",
        })
        self.assertEqual(len(outgoing_messages), 4)
        self.assertMsgEqual(outgoing_messages[3], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:main': 'off'}
        })
        engine.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:main': 'off',  'ffs:moving_to': 'beta'}
        })
        self.assertEqual(len(outgoing_messages), 5)
        self.assertMsgEqual(outgoing_messages[4], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:main': 'on', 'readonly': 'off'}
        })
        engine.incoming_node({
            'msg': 'set_properties_done',
            'from': 'beta',
            'ffs': 'one',
            'properties': {'ffs:main': 'on', 'readonly': 'off'}
        })
        self.assertEqual(len(outgoing_messages), 6)
        self.assertMsgEqual(outgoing_messages[5], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:moving_to': '-'}
        })
        engine.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:moving_to': '-'}
        })
        self.assertEqual(engine.model['one']['_main'], 'beta')
        self.assertEqual(engine.model['one']['alpha'][
                         'properties']['ffs:main'], 'off')
        self.assertEqual(engine.model['one']['beta'][
                         'properties']['ffs:main'], 'on')
        self.assertEqual(engine.model['one']['alpha'][
                         'properties']['readonly'], 'on')
        self.assertEqual(engine.model['one']['beta'][
                         'properties']['readonly'], 'off')
        self.assertEqual(engine.model['one']['alpha'][
                         'properties']['ffs:moving_to'], '-')
        self.assertFalse('_moving' in engine.model['one'])

        self.assertMsgEqual(outgoing_messages[6], {
            'msg': 'remove_snapshot',
            'to': 'beta',
            'ffs': 'one',
            'snapshot': '1'
        })
        self.assertMsgEqual(outgoing_messages[7], {
            'msg': 'remove_snapshot',
            'to': 'alpha',
            'ffs': 'one',
            'snapshot': '1'
        })


class SnapshotPruningTests(EngineTests):

    def test_simple(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['1', '2']},
            'gamma': {},
        })
        # can't have it missing by default, would trigger startup pruning,
        # and I want to test it...
        engine.model['one']['alpha']['snapshots'].remove('1')
        engine._prune_snapshots()
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1',
            'to': 'beta'
        })


    def test_snapshot_removal_fails_due_to_clones(self):
        raise NotImplementedError()

class ZpoolStatusChecks(EngineTests):

    def ge(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
        })
        errors = []
        e.error_callback = lambda x: errors.append(x)
        return e, outgoing_messages, errors
        #

    def test_all_ok_no_response(self):
        e, outgoing, errors = self.ge()
        e.do_zpool_status_check()
        self.assertEqual(len(outgoing), 1)
        self.assertMsgEqual(outgoing[0], {
            'msg': 'zpool_status',
            'to': 'alpha',
        })
        e.incoming_node({
            'from': 'alpha',
            'msg': 'zpool_status',
            'status': '''  pool: alpha
 state: ONLINE
  scan: scrub repaired 0 in 57h36m with 0 errors on Tue Jan 16 10:00:25 2018
config:

	NAME                                                STATE     READ WRITE CKSUM
	martha                                              ONLINE       0     0     0
	  raidz2-0                                          ONLINE       0     0     0
	    sda        ONLINE       0     0     0
	    sdb         ONLINE       0     0     0
	    sdc         ONLINE       0     0     0
	    sdd         ONLINE       0     0     0
	    sde         ONLINE       0     0     0
	    sdf         ONLINE       0     0     0
	  raidz2-1                                          ONLINE       0     0     0
	    sdg         ONLINE       0     0     0
	    sdh         ONLINE       0     0     0
	    sdi         ONLINE       0     0     0
	    sdj         ONLINE       0     0     0
	    sdk         ONLINE       0     0     0
	    sdl         ONLINE       0     0     0
	  raidz2-2                                          ONLINE       0     0     0
	    sdm         ONLINE       0     0     0
	    sdn         ONLINE       0     0     0
	    sdo         ONLINE       0     0     0
	    sdp         ONLINE       0     0     0
	    sdq         ONLINE       0     0     0
	    sdr         ONLINE       0     0     0
	  raidz2-3                                          ONLINE       0     0     0
	    sds         ONLINE       0     0     0
	    sdt         ONLINE       0     0     0
	    sdu         ONLINE       0     0     0
	    sdv         ONLINE       0     0     0
	    sdw         ONLINE       0     0     0
	    sdx         ONLINE       0     0     0
	  raidz2-4                                          ONLINE       0     0     0
	    sdy         ONLINE       0     0     0
	    sdz         ONLINE       0     0     0
	    sdaa         ONLINE       0     0     0
	    sdab         ONLINE       0     0     0
	    sdac         ONLINE       0     0     0
	    sdad         ONLINE       0     0     0
	  raidz2-5                                          ONLINE       0     0     0
	    sdae         ONLINE       0     0     0
	    sdaf         ONLINE       0     0     0
	    sdag   ONLINE       0     0     0
	    sdah         ONLINE       0     0     0
	    sdai         ONLINE       0     0     0
	    sdaj   ONLINE       0     0     0
	logs
	  sdak-part6  ONLINE       0     0     0
	cache
	  sdak-part7  ONLINE       0     0     0
'''
        })
        self.assertEqual(len(errors), 0)

    def test_failure_in_first(self):
        e, outgoing, errors = self.ge()
        e.do_zpool_status_check()
        self.assertEqual(len(outgoing), 1)
        self.assertMsgEqual(outgoing[0], {
            'msg': 'zpool_status',
            'to': 'alpha',
        })
        e.incoming_node({
            'from': 'alpha',
            'msg': 'zpool_status',
            'status': '''  pool: alpha
 state: DEGRADED
status: One or more devices could not be used because the label is missing or
	invalid.  Sufficient replicas exist for the pool to continue
	functioning in a degraded state.
action: Replace the device using 'zpool replace'.
   see: http://zfsonlinux.org/msg/ZFS-8000-4J
  scan: resilvered 82.8G in 10h24m with 0 errors on Tue Jan 23 19:52:23 2018
config:

	NAME                                                STATE     READ WRITE CKSUM
	rose                                                DEGRADED     0     0     0
	  raidz2-0                                          ONLINE       0     0     0
	    sdy                                             ONLINE       0     0     0
	    sde                                             ONLINE       0     0     0
	    sdg1                                            ONLINE       0     0     0
	    sdag                                            ONLINE       0     0     0
	    sdaj                                            ONLINE       0     0     0
	    sdae1                                           ONLINE       0     0     0
	  raidz2-1                                          DEGRADED     0     0     0
	    sdx                                             ONLINE       0     0     0
	    sdh                                             ONLINE       0     0     0
	    sdaa                                            ONLINE       0     0     0
	    sdr                                             UNAVAIL      0     0     0
	    sdl                                             ONLINE       0     0     0
	    sdad                                            UNAVAIL      0     0     0
	  raidz2-2                                          DEGRADED     0     0     0
	    sdq                                             ONLINE       0     0     0
	    sdw                                             UNAVAIL      0     0     0
	    sdd                                             ONLINE       0     0     0
	    sdaf                                            ONLINE       0     0     0
	    sdc                                             ONLINE       0     0     0
	    sdo                                             ONLINE       0     0     0
	  raidz2-3                                          ONLINE       0     0     0
	    sdai                                            ONLINE       0     0     0
	    sdp                                             ONLINE       0     0     0
	    sdu                                             ONLINE       0     0     0
	    sds                                             ONLINE       0     0     0
	    sdac                                            ONLINE       0     0     0
	    sdj                                             ONLINE       0     0     0
	  raidz2-4                                          ONLINE       0     0     0
	    sdz                                             ONLINE       0     0     0
	    sdt                                             ONLINE       0     0     0
	    sdm                                             ONLINE       0     0    12
	    sdak                                            ONLINE       0     0     0
	    sdv                                             ONLINE       0     0     0
	    sdab                                            ONLINE       0     0     0
	  raidz2-5                                          ONLINE       0     0     0
	    sdb                                             ONLINE       0     0     0
	    sdi1                                            ONLINE       0     0     0
	    sdf                                             ONLINE       0     0     0
	    sdn                                             ONLINE       0     0     0
	    sdk                                             ONLINE       0     0     0
	    sdah                                            ONLINE       0     0     0
	logs
	  sdak-part6  ONLINE       0     0     0
	cache
	  sda7                                              ONLINE       0     0     0
'''})
        self.assertEqual(len(errors), 1)
        outgoing.clear()

        e.do_zpool_status_check()
        self.assertEqual(len(outgoing), 1)
        e.incoming_node({
            'from': 'alpha',
            'msg': 'zpool_status',
            'status': '''  pool: alpha
 state: DEGRADED
status: One or more devices could not be used because the label is missing or
	invalid.  Sufficient replicas exist for the pool to continue
	functioning in a degraded state.
action: Replace the device using 'zpool replace'.
   see: http://zfsonlinux.org/msg/ZFS-8000-4J
  scan: resilvered 82.8G in 10h24m with 0 errors on Tue Jan 23 19:52:23 2018
config:

	NAME                                                STATE     READ WRITE CKSUM
	rose                                                DEGRADED     0     0     0
	  raidz2-0                                          ONLINE       0     0     0
	    sdy                                             ONLINE       0     0     0
	    sde                                             ONLINE       0     0     0
	    sdg1                                            ONLINE       0     0     0
	    sdag                                            ONLINE       0     0     0
	    sdaj                                            ONLINE       0     0     0
	    sdae1                                           ONLINE       0     0     0
	  raidz2-1                                          DEGRADED     0     0     0
	    sdx                                             ONLINE       0     0     0
	    sdh                                             ONLINE       0     0     0
	    sdaa                                            ONLINE       0     0     0
	    sdr                                             UNAVAIL      0     0     0
	    sdl                                             ONLINE       0     0     0
	    sdad                                            UNAVAIL      0     0     0
	  raidz2-2                                          DEGRADED     0     0     0
	    sdq                                             ONLINE       0     0     0
	    sdw                                             UNAVAIL      0     0     0
	    sdd                                             ONLINE       0     0     0
	    sdaf                                            ONLINE       0     0     0
	    sdc                                             ONLINE       0     0     0
	    sdo                                             ONLINE       0     0     0
	  raidz2-3                                          ONLINE       0     0     0
	    sdai                                            ONLINE       0     0     0
	    sdp                                             ONLINE       0     0     0
	    sdu                                             ONLINE       0     0     0
	    sds                                             ONLINE       0     0     0
	    sdac                                            ONLINE       0     0     0
	    sdj                                             ONLINE       0     0     0
	  raidz2-4                                          ONLINE       0     0     0
	    sdz                                             ONLINE       0     0     0
	    sdt                                             ONLINE       0     0     0
	    sdm                                             ONLINE       0     0    12
	    sdak                                            ONLINE       0     0     0
	    sdv                                             ONLINE       0     0     0
	    sdab                                            ONLINE       0     0     0
	  raidz2-5                                          ONLINE       0     0     0
	    sdb                                             ONLINE       0     0     0
	    sdi1                                            ONLINE       0     0     0
	    sdf                                             ONLINE       0     0     0
	    sdn                                             ONLINE       0     0     0
	    sdk                                             ONLINE       0     0     0
	    sdah                                            ONLINE       0     0     0
	logs
	  sdak-part6  ONLINE       0     0     0
	cache
	  sda7                                              ONLINE       0     0     0
'''})
        self.assertEqual(len(errors), 1)  # no repetition
        outgoing.clear()

        e.do_zpool_status_check()
        self.assertEqual(len(outgoing), 1)
        e.incoming_node({
            'from': 'alpha',
            'msg': 'zpool_status',
            'status': '''  pool: alpha
 state: DEGRADED
status: One or more devices could not be used because the label is missing or
	invalid.  Sufficient replicas exist for the pool to continue
	functioning in a degraded state.
action: Replace the device using 'zpool replace'.
   see: http://zfsonlinux.org/msg/ZFS-8000-4J
  scan: resilvered 82.8G in 10h24m with 0 errors on Tue Jan 23 19:52:23 2018
config:

	NAME                                                STATE     READ WRITE CKSUM
	rose                                                DEGRADED     0     0     0
	  raidz2-0                                          DEGRADED       0     0     0
	    sdy                                             ONLINE       0     0     0
	    sde                                             ONLINE       0     0     0
	    sdg1                                            ONLINE       0     0     0
	    sdag                                            ONLINE       0     0     0
	    sdaj                                            DEGRADED       0     0     0
	    sdae1                                           ONLINE       0     0     0
	  raidz2-1                                          DEGRADED     0     0     0
	    sdx                                             ONLINE       0     0     0
	    sdh                                             ONLINE       0     0     0
	    sdaa                                            ONLINE       0     0     0
	    sdr                                             UNAVAIL      0     0     0
	    sdl                                             ONLINE       0     0     0
	    sdad                                            UNAVAIL      0     0     0
	  raidz2-2                                          DEGRADED     0     0     0
	    sdq                                             ONLINE       0     0     0
	    sdw                                             UNAVAIL      0     0     0
	    sdd                                             ONLINE       0     0     0
	    sdaf                                            ONLINE       0     0     0
	    sdc                                             ONLINE       0     0     0
	    sdo                                             ONLINE       0     0     0
	  raidz2-3                                          ONLINE       0     0     0
	    sdai                                            ONLINE       0     0     0
	    sdp                                             ONLINE       0     0     0
	    sdu                                             ONLINE       0     0     0
	    sds                                             ONLINE       0     0     0
	    sdac                                            ONLINE       0     0     0
	    sdj                                             ONLINE       0     0     0
	  raidz2-4                                          ONLINE       0     0     0
	    sdz                                             ONLINE       0     0     0
	    sdt                                             ONLINE       0     0     0
	    sdm                                             ONLINE       0     0    12
	    sdak                                            ONLINE       0     0     0
	    sdv                                             ONLINE       0     0     0
	    sdab                                            ONLINE       0     0     0
	  raidz2-5                                          ONLINE       0     0     0
	    sdb                                             ONLINE       0     0     0
	    sdi1                                            ONLINE       0     0     0
	    sdf                                             ONLINE       0     0     0
	    sdn                                             ONLINE       0     0     0
	    sdk                                             ONLINE       0     0     0
	    sdah                                            ONLINE       0     0     0
	logs
	  sdak-part6  ONLINE       0     0     0
	cache
	  sda7                                              ONLINE       0     0     0
'''})
        self.assertEqual(
            len(errors), 2)  # DEGRADING something else is another error

        outgoing.clear()
        e.do_zpool_status_check()
        self.assertEqual(len(outgoing), 1)
        e.incoming_node({
            'from': 'alpha',
            'msg': 'zpool_status',
            'status': '''  pool: alpha
 state: UNAVAIL
status: One or more devices could not be used because the label is missing or
	invalid.  Sufficient replicas exist for the pool to continue
	functioning in a degraded state.
action: Replace the device using 'zpool replace'.
   see: http://zfsonlinux.org/msg/ZFS-8000-4J
  scan: resilvered 82.8G in 10h24m with 0 errors on Tue Jan 23 19:52:23 2018
config:

	NAME                                                STATE     READ WRITE CKSUM
	rose                                                DEGRADED     0     0     0
	  raidz2-0                                          DEGRADED       0     0     0
	    sdy                                             ONLINE       0     0     0
	    sde                                             ONLINE       0     0     0
	    sdg1                                            ONLINE       0     0     0
	    sdag                                            ONLINE       0     0     0
	    sdaj                                            DEGRADED       0     0     0
	    sdae1                                           ONLINE       0     0     0
	  raidz2-1                                          DEGRADED     0     0     0
	    sdx                                             ONLINE       0     0     0
	    sdh                                             UNAVAIL       0     0     0
	    sdaa                                            ONLINE       0     0     0
	    sdr                                             UNAVAIL      0     0     0
	    sdl                                             ONLINE       0     0     0
	    sdad                                            UNAVAIL      0     0     0
	  raidz2-2                                          DEGRADED     0     0     0
	    sdq                                             ONLINE       0     0     0
	    sdw                                             UNAVAIL      0     0     0
	    sdd                                             ONLINE       0     0     0
	    sdaf                                            ONLINE       0     0     0
	    sdc                                             ONLINE       0     0     0
	    sdo                                             ONLINE       0     0     0
	  raidz2-3                                          ONLINE       0     0     0
	    sdai                                            ONLINE       0     0     0
	    sdp                                             ONLINE       0     0     0
	    sdu                                             ONLINE       0     0     0
	    sds                                             ONLINE       0     0     0
	    sdac                                            ONLINE       0     0     0
	    sdj                                             ONLINE       0     0     0
	  raidz2-4                                          ONLINE       0     0     0
	    sdz                                             ONLINE       0     0     0
	    sdt                                             ONLINE       0     0     0
	    sdm                                             ONLINE       0     0    12
	    sdak                                            ONLINE       0     0     0
	    sdv                                             ONLINE       0     0     0
	    sdab                                            ONLINE       0     0     0
	  raidz2-5                                          ONLINE       0     0     0
	    sdb                                             ONLINE       0     0     0
	    sdi1                                            ONLINE       0     0     0
	    sdf                                             ONLINE       0     0     0
	    sdn                                             ONLINE       0     0     0
	    sdk                                             ONLINE       0     0     0
	    sdah                                            ONLINE       0     0     0
	logs
	  sdak-part6  ONLINE       0     0     0
	cache
	  sda7                                              ONLINE       0     0     0
'''})
        self.assertEqual(
            len(errors), 3)  # failing is most certainly an error again

        outgoing.clear()
        e.do_zpool_status_check()
        self.assertEqual(len(outgoing), 1)
        e.incoming_node({
            'from': 'alpha',
            'msg': 'zpool_status',
            'status': '''  pool: alpha
 state: ONLINE
status: One or more devices is currently being resilvered.  The pool will
	continue to function, possibly in a degraded state.
action: Wait for the resilver to complete.
  scan: resilver in progress since Fri Jan 26 17:44:49 2018
	5.13G scanned out of 65.0T at 9.25M/s, (scan is slow, no estimated time)
	448M resilvered, 0.01% done
config:

	NAME                                                STATE     READ WRITE CKSUM
	rose                                                ONLINE       0     0     0
	  raidz2-0                                          ONLINE       0     0     0
	    sdy                                             ONLINE       0     0     0
	    sde                                             ONLINE       0     0     0
	    sdg1                                            ONLINE       0     0     0
	    sdag                                            ONLINE       0     0     0
	    sdaj                                            ONLINE       0     0     0
	    sdae1                                           ONLINE       0     0     0
	  raidz2-1                                          ONLINE       0     0     0
	    sdx                                             ONLINE       0     0     0
	    sdh                                             ONLINE       0     0     0
	    sdaa                                            ONLINE       0     0     0
	    sdr                                             ONLINE       0     0     0  (resilvering)
	    sdl                                             ONLINE       0     0     0
	    sdad                                            ONLINE       0     0     0  (resilvering)
	  raidz2-2                                          ONLINE       0     0     0
	    sdq                                             ONLINE       0     0     0
	    sdw                                             ONLINE       0     0     5  (resilvering)
	    sdd                                             ONLINE       0     0     0
	    sdaf                                            ONLINE       0     0     0
	    sdc                                             ONLINE       0     0     0
	    sdo                                             ONLINE       0     0     0
	  raidz2-3                                          ONLINE       0     0     0
	    sdai                                            ONLINE       0     0     0
	    sdp                                             ONLINE       0     0     0
	    sdu                                             ONLINE       0     0     0
	    sds                                             ONLINE       0     0     0
	    sdac                                            ONLINE       0     0     0
	    sdj                                             ONLINE       0     0     0
	  raidz2-4                                          ONLINE       0     0     0
	    sdz                                             ONLINE       0     0     0
	    sdt                                             ONLINE       0     0     0
	    sdm                                             ONLINE       0     0     0
	    sdak                                            ONLINE       0     0     0
	    sdv                                             ONLINE       0     0     0
	    sdab                                            ONLINE       0     0     0
	  raidz2-5                                          ONLINE       0     0     0
	    sdb                                             ONLINE       0     0     0
	    sdi1                                            ONLINE       0     0     0
	    sdf                                             ONLINE       0     0     0
	    sdn                                             ONLINE       0     0     0
	    sdk                                             ONLINE       0     0     0
	    sdah                                            ONLINE       0     0     0
	logs
	  ata-INTEL_SSDSC2CW120A3_CVCV321604Z5120BGN-part6  ONLINE       0     0     0
	cache
	  sda7                                              ONLINE       0     0     0
'''})
        self.assertEqual(len(
            errors), 4)  # for now, the all clear also comes via the error reporting mechanism

    def test_no_repeated_requests_if_outstanding(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['1', '2']},
        })
        e.do_zpool_status_check()
        x = len(outgoing_messages)
        self.assertTrue(x > 0)
        e.do_zpool_status_check()
        self.assertEqual(len(outgoing_messages), x)
        outgoing_messages.clear()
        self.assertEqual(len(outgoing_messages), 0)
        e.do_zpool_status_check()
        self.assertEqual(len(outgoing_messages), x)


class ChownTests(PostStartupTests):

    def test_chown_standalone(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['1', '2']},
            'gamma': {},
        })
        e.incoming_client({
            'msg': 'chown_and_chmod',
            'ffs': 'one',
            'sub_path': '/',
        })
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'chown_and_chmod',
            'ffs': 'one',
            'sub_path': '/',
            'to': 'alpha',
            'user': e.config.get_chown_user('one'),
            'rights': e.config.get_chmod_rights('one'),
        })

    def test_capture_and_chown(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['1', '2']},
            'gamma': {},
        })
        e.incoming_client(
            {"msg": 'capture', 'ffs': 'one', 'chown_and_chmod': True})
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]),
                            {
            'to': 'alpha',
            'msg': 'capture',
            'ffs': 'one',
            'chown_and_chmod': True,
            'user': e.config.get_chown_user('one'),
            'rights': e.config.get_chmod_rights('one'),
        }
        )

        pass


class TestStartupTriggeringActions(EngineTests):

    def test_missing_snapshot_triggers_send(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['1', ]},
            'gamma': {},
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqualMinusSnapshot(outgoing_messages[0], {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'target_host': 'beta',
            'ffs': 'one',
            'snapshot': '2'
        })

    def test_multiple_missing_snapshots(self):
        engine, outgoing_messages = self.get_engine({
            # thes that we stick to this order. Nodes return snapshots in
            # creation order!
            'alpha': {'_one': ['2', '1']},
            'beta':  {'one': []},
            'gamma': {},
        })
        self.assertEqual(len(outgoing_messages), 2)
        self.assertMsgEqualMinusSnapshot(outgoing_messages[0], {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'target_host': 'beta',
            'ffs': 'one',
            'snapshot': '2'
        })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[1], {
            'msg': 'send_snapshot',
            'to': 'alpha',
            'target_host': 'beta',
            'ffs': 'one',
            'snapshot': '1'
        })

    def test_missing_earlier_snapshot(self):
        engine, outgoing_messages = self.get_engine({
            # thes that we stick to this order. Nodes return snapshots in
            # creation order!
            'alpha': {'_one': ['3', '2', '1']},
            'beta':  {'one': ['3', '1']},
            'gamma': {},
        })
        # nothing send - we only send prev snapshots if there's an unbroken line to the current one.
        # otherwise we'd have to discuss how and whether to roll back the state first
        # and rollback to the current snapshot later, and rolling back
        # eats later snapshots

        self.assertEqual(len(outgoing_messages), 0)

    def test_additional_snapshots_on_target_trigger_snapshot_removal(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['2', '1', ]},
            'gamma': {},
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'remove_snapshot',
            'to': 'beta',
            'ffs': 'one',
            'snapshot': '2'
        })

    def test_move_interrupted_stage_1(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', ('readonly', 'on'), ('ffs:moving_to', 'beta')]},
            'beta':  {'one': ['1', ]},
            'gamma': {},
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]), {
            'to': 'alpha',
            'msg': 'capture',
            'ffs': 'one',
        })

    def test_move_interrupted_stage_2_after_capture(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2', ('readonly', 'on'), ('ffs:moving_to', 'beta')]},
            'beta':  {'one': ['1']},
            'gamma': {},
        })
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]), {
            'to': 'alpha',
            'msg': 'capture',
            'ffs': 'one',
        })

    def test_move_interrupted_stage_3_after_replication(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2', ('readonly', 'on'), ('ffs:moving_to', 'beta')]},
            'beta':  {'one': ['1', '2']},
            'gamma': {},
        })

    def test_move_interrupted_stage_4_after_remove_main(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'one': ['1', '2',
                              ('ffs:main', 'off'),
                              ('readonly', 'on'),
                              ('ffs:moving_to', 'beta')]},
            'beta':  {'one': ['1', '2']},
            'gamma': {},
        })
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:main': 'on', 'readonly': 'off'}
        })

    def test_move_interrupted_stage_5_after_set_main(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'one': ['1', '2',
                              ('ffs:main', 'off'),
                              ('readonly', 'on'),
                              ('ffs:moving_to', 'beta')]},
            'beta':  {'one': ['1', '2',
                              ('ffs:main', 'on'),
                              ('readonly', 'off')
                              ]},
            'gamma': {},
        })
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:moving_to': '-'}
        })

    def test_purge_snapshots(self):
        cfg = self._get_test_config()
        cfg.decide_snapshots_to_keep = lambda a, b: ['2']
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['1', '2']},
            'gamma': {},
        }, config=cfg)
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1'})
        self.assertMsgEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1'})
        self.assertEqual(len(outgoing_messages), 2)

    def test_snapshot_on_target_too_much(self):
        cfg = self._get_test_config()
        cfg.decide_snapshots_to_keep = lambda a, b: ['2']
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['0', '1', '2']},
            'gamma': {},
        }, config=cfg)
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1'})
        self.assertMsgEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '0'})
        self.assertMsgEqual(outgoing_messages[2], {
            'to': 'beta',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1'})
        self.assertEqual(len(outgoing_messages), 3)

    def test_purge_and_send_combined(self):
        cfg = self._get_test_config()
        cfg.decide_snapshots_to_keep = lambda a, b: ['2', '3']
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2', '3', ]},
            'beta':  {'one': ['0', '1', '2']},
            'gamma': {},
        }, config=cfg)
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1'})
        self.assertMsgEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '0'})
        self.assertMsgEqual(outgoing_messages[2], {
            'to': 'beta',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1'})
        self.assertMsgEqualMinusSnapshot(outgoing_messages[3], {
            'to': 'alpha',
            'msg': 'send_snapshot',
            'ffs': 'one',
            'snapshot': '3',
            'target_host': 'beta',
        })
        self.assertEqual(len(outgoing_messages), 4)

    def test_send_only_some(self):
        cfg = self._get_test_config()
        cfg.decide_snapshots_to_keep = lambda a, b: ['2', '3']
        cfg.decide_snapshots_to_send = lambda dummy_ffs, snapshots: ['3']

        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '1b', '2', '3', ]},
            'beta':  {'one': ['-1', '0']},
            'gamma': {},
        },
            config=cfg)
        self.assertMsgEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1'})
        self.assertMsgEqual(outgoing_messages[1], {
            'to': 'alpha',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1b'})
        self.assertMsgEqual(outgoing_messages[2], {
            'to': 'beta',
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '-1'})
        # no removal of 0 - it would be the last snapshot
        # self.assertMsgEqual(outgoing_messages[1], {
        #'to': 'beta',
        #'msg': 'remove_snapshot',
        #'ffs': 'one',
        #'snapshot': '0'})
        self.assertMsgEqualMinusSnapshot(outgoing_messages[3], {
            'to': 'alpha',
            'msg': 'send_snapshot',
            'ffs': 'one',
            'snapshot': '3',
            'target_host': 'beta',
        })
        self.assertEqual(len(outgoing_messages), 4)

    def test_capture_if_no_snapshots_to_send_but_replicates(self):
        engine, outgoing_messages = self.get_engine({
            # thes that we stick to this order. Nodes return snapshots in
            # creation order!
            'alpha': {'_one': []},
            'beta':  {'one': []},
            'gamma': {},
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]), {
            'msg': 'capture',
            'ffs': 'one',
            'to': 'alpha'
        })

    def test_capture_if_no_snapshots_to_send_but_replicates_decide_on_snapshots(self):
        cfg = self._get_test_config()
        cfg.decide_snapshots_to_send = lambda ffs, snapshots: [x for x in snapshots if x.startswith('ffs-')]
        engine, outgoing_messages = self.get_engine({
            # thes that we stick to this order. Nodes return snapshots in
            # creation order!
            'alpha': {'_one': ['nosend-1']},
            'beta':  {'one': []},
            'gamma': {},
        }, config=cfg)
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]), {
            'msg': 'capture',
            'ffs': 'one',
            'to': 'alpha'
        })

    def test_prio_ordinging_during_send_missing(self):
        beta_def = {
            '_A0':['1', '2', ('ffs:priority', '1001')],
            '_A101':['1', '2', ('ffs:priority', '1')],
        }
        alpha_def = {
            'A0':['1', ('ffs:priority', '1001')],
            'A101':['1', ('ffs:priority', '1')],
        }
        for i in range(1, 100):
            beta_def['_A%i'%i] = ['1', '2']
            alpha_def['A%i'%i] = ['1', '2']
        e, outgoing_messages = self.get_engine({
            'beta':  beta_def,
            'alpha':  alpha_def,
        })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[0], {
            'msg': 'send_snapshot',
            'to': 'beta',
            'ffs': 'A101',
            'snapshot': '2',
            'target_host': 'alpha',
            'priority': 1,
        })
        self.assertMsgEqualMinusSnapshot(outgoing_messages[-1], {
            'msg': 'send_snapshot',
            'to': 'beta',
            'ffs': 'A0',
            'snapshot': '2',
            'target_host': 'alpha',
            'priority': 1001,
        })





class CrossTalkTest(EngineTests):

    def test_capture_during_move(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1']},
            'gamma': {},
        })
        e.incoming_client(
            {"msg": "move_main", 'ffs': 'one', 'target': 'beta'})

        def inner():
            e.incoming_client({
                'msg': 'capture',
                'ffs': 'one'
            })
        self.assertRaises(engine.MoveInProgress, inner)
        e.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'readonly': 'on', 'ffs:moving_to': 'beta'}
        })
        snapshot_name = outgoing_messages[-1]['snapshot']
        self.assertRaises(engine.MoveInProgress, inner)
        e.incoming_node({
            'msg': 'capture_done',
            'from': 'alpha',
            'ffs': 'one',
            'snapshot': snapshot_name,
        })
        self.assertRaises(engine.MoveInProgress, inner)
        e.incoming_node({
            'msg': 'send_snapshot_done',
            'from': 'alpha',
            'target_host': 'beta',
            'target_node': 'beta',
            'ffs': 'one',
            'snapshot': snapshot_name,
        })
        self.assertRaises(engine.MoveInProgress, inner)
        e.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:main': 'off'}
        })
        self.assertRaises(engine.MoveInProgress, inner)
        e.incoming_node({
            'msg': 'set_properties_done',
            'from': 'beta',
            'ffs': 'one',
            'properties': {'ffs:main': 'on', 'readonly': 'off'}
        })
        self.assertRaises(engine.MoveInProgress, inner)
        e.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:moving_to': '-'}
        })
        # veryify move worked
        self.assertEqual(e.model['one']['_main'], 'beta')
        self.assertEqual(e.model['one']['alpha'][
                         'properties']['ffs:main'], 'off')
        self.assertEqual(e.model['one']['beta'][
                         'properties']['ffs:main'], 'on')
        self.assertEqual(e.model['one']['alpha'][
                         'properties']['readonly'], 'on')
        self.assertEqual(e.model['one']['beta'][
                         'properties']['readonly'], 'off')
        self.assertEqual(e.model['one']['alpha'][
                         'properties']['ffs:moving_to'], '-')
        self.assertFalse('_moving' in e.model['one'])
        outgoing_messages.clear()
        e.incoming_client({
            'msg': 'capture',
            'ffs': 'one'
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]), {
            'msg': 'capture',
            'to': 'beta',
            'ffs': 'one'
        })

    def test_move_during_capture(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1']},
            'gamma': {},
        })
        e.incoming_client({
            'msg': 'capture',
            'ffs': 'one'
        })
        self.assertMsgEqual(remove_snapshot_from_message(outgoing_messages[0]), {
            'msg': 'capture',
            'to': 'alpha',
            'ffs': 'one'
        })
        wrong_snapshot_name = outgoing_messages[0]['snapshot']
        e.incoming_client(
            {"msg": "move_main", 'ffs': 'one', 'target': 'beta'})

        e.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'readonly': 'on', 'ffs:moving_to': 'beta'}
        })
        right_snapshot_name = outgoing_messages[-1]['snapshot']
        e.incoming_node({
            'msg': 'capture_done',
            'from': 'alpha',
            'ffs': 'one',
            'snapshot': wrong_snapshot_name,
        })
        outgoing_messages.clear()
        e.incoming_node({
            'msg': 'send_snapshot_done',
            'from': 'alpha',
            'target_host': 'beta',
            'target_node': 'beta',
            'ffs': 'one',
            'snapshot': wrong_snapshot_name,
        })
        # this one is not supposed to trigger the move...
        self.assertEqual(len(outgoing_messages), 0)
        e.incoming_node({
            'msg': 'capture_done',
            'from': 'alpha',
            'ffs': 'one',
            'snapshot': right_snapshot_name,
        })
        self.assertEqual(len(outgoing_messages), 1)
        outgoing_messages.clear()
        e.incoming_node({
            'msg': 'send_snapshot_done',
            'from': 'alpha',
            'target_node': 'beta',
            'target_host': 'beta',
            'ffs': 'one',
            'snapshot': right_snapshot_name,
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:main': 'off'}
        })

        #

    def test_error_return_from_node(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1']},
            'gamma': {},
        })

        def inner():
            e.incoming_node({'error': 'test_error', 'content': 'undefined'})
        self.assertRaises(engine.ManualInterventionNeeded, inner)

        self.assertTrue(e.faulted)


class MockEngine:

    def __init__(self):
        self.node_messages = []
        self.config = default_config.CheckedConfig(
            default_config.DefaultConfig())

    def incoming_node(self, msg):
        self.node_messages.append(message)


class OutgoingMessageForTesting(ssh_message_que.OutgoingMessages):

    def __init__(self):
        engine = MockEngine()
        ssh_cmd = 'shu'
        import logging
        logger = logging.Logger(name='Dummy')
        logger.addHandler(logging.NullHandler())
        super().__init__(logger, engine, ssh_cmd)

    def do_send(self, msg):
        pass


class OutgoingMessageTests(unittest.TestCase):

    def test_max_per_host(self):
        m = {'msg': 'deploy'}
        om = OutgoingMessageForTesting()
        for i in range(om.max_per_host + 1):
            mx = m.copy()
            mx['i'] = i
            om.send_message('alpha', {}, mx)
        out = om.outgoing['alpha']
        self.assertEqual(len(out), om.max_per_host + 1)
        sent = [x for x in out if x.status != 'unsent']
        unsent = [x for x in out if x.status == 'unsent']
        self.assertEqual(len(sent), om.max_per_host)
        self.assertEqual(len(unsent), 1)
        s = set()
        for x in sent:
            s.add(x.msg['i'])
        self.assertEqual(s, set(range(om.max_per_host)))
        self.assertEqual(unsent[0].msg['i'], om.max_per_host)
        om.job_returned(out[0].job_id, {'msg': 'deploy_done'})
        sent = [x for x in out if x.status != 'unsent']
        unsent = [x for x in out if x.status == 'unsent']
        self.assertEqual(len(sent), om.max_per_host)
        self.assertEqual(len(unsent), 0)

    def test_max_one_send_snapshot(self):
        m = {'msg': 'send_snapshot'}
        om = OutgoingMessageForTesting()
        self.assertTrue(om.max_per_host > 1)
        for i in range(om.max_per_host + 1):
            mx = m.copy()
            mx['i'] = i
            om.send_message('alpha', {}, mx)
        out = om.outgoing['alpha']
        self.assertEqual(len(out), om.max_per_host + 1)
        sent = [x for x in out if x.status != 'unsent']
        unsent = [x for x in out if x.status == 'unsent']
        self.assertEqual(len(sent), 1)
        self.assertEqual(len(unsent), om.max_per_host)
        self.assertEqual(unsent[0].msg['i'], 1)
        om.job_returned(out[0].job_id, {'msg': 'send_snapshot_done'})
        sent = [x for x in out if x.status != 'unsent']
        unsent = [x for x in out if x.status == 'unsent']
        self.assertEqual(len(sent), 1)
        self.assertEqual(len(unsent), om.max_per_host - 1)

    def test_mixed(self):
        om = OutgoingMessageForTesting()
        self.assertTrue(om.max_per_host > 1)
        om.send_message('alpha', {}, {'msg': 'send_snapshot'})
        om.send_message('alpha', {}, {'msg': 'deploy'})
        om.send_message('alpha', {}, {'msg': 'deploy'})
        out = om.outgoing['alpha']
        self.assertEqual(len(out), 3)

        sent = [x for x in out if x.status != 'unsent']
        unsent = [x for x in out if x.status == 'unsent']
        self.assertEqual(len(sent), 3)
        self.assertEqual(len(unsent), 0)

        om.send_message('alpha', {}, {'msg': 'send_snapshot'})
        sent = [x for x in out if x.status != 'unsent']
        unsent = [x for x in out if x.status == 'unsent']
        self.assertEqual(len(sent), 3)
        self.assertEqual(len(unsent), 1)

        om.send_message('alpha', {}, {'msg': 'deploy'})
        sent = [x for x in out if x.status != 'unsent']
        unsent = [x for x in out if x.status == 'unsent']
        self.assertEqual(len(sent), 4)
        self.assertEqual(len(unsent), 1)
        self.assertEqual(unsent[0].msg['msg'], 'send_snapshot')

        om.job_returned(out[0].job_id, {'msg': 'send_snapshot_done'})
        sent = [x for x in out if x.status != 'unsent']
        unsent = [x for x in out if x.status == 'unsent']
        self.assertEqual(len(sent), 4)
        self.assertEqual(len(unsent), 0)
        # this reflects the submission order, not the send order!
        self.assertEqual(sent[2].msg['msg'], 'send_snapshot')


class RenameTests(PostStartupTests):

    def test_rename_non_replicated(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {},
            'gamma': {},
        })
        self.assertEqual(0, len(outgoing_messages))
        e.incoming_client({"msg": 'rename',
                           'to': 'alpha',
                           'ffs': 'one',
                           'new_name': 'two'})
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'rename',
            'to': 'alpha',
            'ffs': 'one',
            'new_name': 'two'
        })
        self.assertTrue('_renaming' in e.model['one'])
        self.assertTrue('_renaming' in e.model['two'])
        self.assertEqual(1, len(outgoing_messages))
        e.incoming_node({"msg": 'rename_done',
                         'ffs': 'one',
                         'from': 'alpha',
                         'new_name': 'two'})
        self.assertFalse('one' in e.model)
        self.assertEqual(e.model['two']['alpha']['snapshots'], ['1'])
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'}
        })

    def test_rename_raises_duplicate_name(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1'], '_two': ['1']},
            'beta':  {},
            'gamma': {},
        })
        self.assertEqual(0, len(outgoing_messages))

        def inner():
            e.incoming_client({"msg": 'rename',
                               'ffs': 'one',
                               'new_name': 'two'})
        self.assertRaises(ValueError, inner)
        self.assertEqual(0, len(outgoing_messages))

    def test_rename_replicated(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'one': ['1']},
            'beta':  {'_one': ['1']},
            'gamma': {'one': ['1']},
        })
        self.assertEqual(0, len(outgoing_messages))
        e.incoming_client({"msg": 'rename',
                           'ffs': 'one',
                           'new_name': 'two'})
        # no need to send main first
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'rename',
            'to': 'alpha',
            'ffs': 'one',
            'new_name': 'two'
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'rename',
            'to': 'beta',
            'ffs': 'one',
            'new_name': 'two'
        })
        self.assertMsgEqual(outgoing_messages[2], {
            'msg': 'rename',
            'to': 'gamma',
            'ffs': 'one',
            'new_name': 'two'
        })
        self.assertTrue(e.model['one'].get('_renaming', False))
        self.assertTrue(e.model['two'].get('_renaming', False))

        self.assertEqual(len(outgoing_messages), 3)

        e.incoming_node({"msg": 'rename_done',
                         'ffs': 'one',
                         'from': 'gamma',
                         'new_name': 'two'})
        self.assertFalse('gamma' in e.model['one'])
        self.assertTrue('gamma' in e.model['two'])
        self.assertEqual(len(outgoing_messages), 3)

        e.incoming_node({"msg": 'rename_done',
                         'ffs': 'one',
                         'from': 'beta',
                         'new_name': 'two'})
        self.assertFalse('beta' in e.model['one'])
        self.assertFalse('_main' in e.model['one'])
        self.assertTrue('beta' in e.model['two'])
        self.assertTrue('_main' in e.model['two'])
        self.assertEqual(len(outgoing_messages), 3)

        e.incoming_node({"msg": 'rename_done',
                         'ffs': 'one',
                         'from': 'alpha',
                         'new_name': 'two'})
        self.assertFalse('one' in e.model)
        self.assertTrue('alpha' in e.model['two'])
        self.assertEqual(len(outgoing_messages), 6)
        self.assertMsgEqual(outgoing_messages[3], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'}
        })
        self.assertMsgEqual(outgoing_messages[4], {
            'msg': 'set_properties',
            'to': 'beta',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'}
        })
        self.assertMsgEqual(outgoing_messages[5], {
            'msg': 'set_properties',
            'to': 'gamma',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'}
        })

    def test_rename_replicated_interrupted_main_was_renamed(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'one': ['1']},
            'beta':  {'_two': ['1', ('ffs:renamed_from', 'one')]},
            'gamma': {'one': ['1']},
        })
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'rename',
            'to': 'alpha',
            'ffs': 'one',
            'new_name': 'two'
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'rename',
            'to': 'gamma',
            'ffs': 'one',
            'new_name': 'two'
        })
        self.assertEqual(len(outgoing_messages), 2)
        self.assertTrue(e.model['one'].get('_renaming', False))
        self.assertTrue(e.model['two'].get('_renaming', False))

        e.incoming_node({"msg": 'rename_done',
                         'ffs': 'one',
                         'from': 'alpha',
                         'new_name': 'two'})
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({"msg": 'rename_done',
                         'ffs': 'one',
                         'from': 'gamma',
                         'new_name': 'two'})
        self.assertEqual(len(outgoing_messages), 3)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'set_properties',
            'to': 'beta',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })
        self.assertMsgEqual(outgoing_messages[2], {
            'msg': 'set_properties',
            'to': 'gamma',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })
        e.incoming_node({
            'msg': 'set_properties_done',
            'ffs': 'two',
            'from': 'alpha',
            'properties': {"ffs:renamed_from": '-'}
        })
        e.incoming_node({
            'msg': 'set_properties_done',
            'ffs': 'two',
            'from': 'beta',
            'properties': {"ffs:renamed_from": '-'}
        })
        e.incoming_node({
            'msg': 'set_properties_done',
            'ffs': 'two',
            'from': 'gamma',
            'properties': {"ffs:renamed_from": '-'}
        })

        self.assertFalse('one' in e.model)
        self.assertTrue('two' in e.model)
        self.assertEqual(e.model['two']['beta']['properties'].get(
            'ffs:renamed_from', '-'), '-')
        self.assertEqual(e.model['two']['_main'], 'beta')

    def test_rename_replicated_interrupted_non_main_was_renamed(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'two': ['1', ('ffs:renamed_from', 'one')]},
            'beta':  {'_one': ['1', ]},
            'gamma': {'one': ['1']},
        })
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'rename',
            'to': 'beta',
            'ffs': 'one',
            'new_name': 'two'
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'rename',
            'to': 'gamma',
            'ffs': 'one',
            'new_name': 'two'
        })
        self.assertEqual(len(outgoing_messages), 2)
        e.incoming_node({"msg": 'rename_done',
                         'ffs': 'one',
                         'from': 'beta',
                         'new_name': 'two'})
        self.assertEqual(len(outgoing_messages), 2)
        outgoing_messages.clear()
        e.incoming_node({"msg": 'rename_done',
                         'ffs': 'one',
                         'from': 'gamma',
                         'new_name': 'two'})
        self.assertEqual(len(outgoing_messages), 3)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'set_properties',
            'to': 'beta',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })
        self.assertMsgEqual(outgoing_messages[2], {
            'msg': 'set_properties',
            'to': 'gamma',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })
        self.assertFalse('one' in e.model)
        self.assertTrue('two' in e.model)
        self.assertEqual(e.model['two']['beta']['properties'].get(
            'ffs:renamed_from', '-'), '-')
        self.assertEqual(e.model['two']['_main'], 'beta')

    def test_rename_interrupted_after_rename_before_any_set_properties(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'two': ['1', ('ffs:renamed_from', 'one')]},
            'beta':  {'_two': ['1', ('ffs:renamed_from', 'one')]},
            'gamma': {'two': ['1', ('ffs:renamed_from', 'one')]},
        })
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'set_properties',
            'to': 'beta',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })
        self.assertMsgEqual(outgoing_messages[2], {
            'msg': 'set_properties',
            'to': 'gamma',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })

        self.assertEqual(len(outgoing_messages), 3)
        self.assertFalse('one' in e.model)
        self.assertFalse(e.model['two'].get('_renaming', False))

    def test_rename_interrupted_after_rename_before_some_set_properties(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'two': ['1', ('ffs:renamed_from', 'one')]},
            'beta':  {'_two': ['1', ('ffs:renamed_from', '-')]},
            'gamma': {'two': ['1', ('ffs:renamed_from', 'one')]},
        })
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'set_properties',
            'to': 'gamma',
            'ffs': 'two',
            'properties': {'ffs:renamed_from': '-'},
        })

        self.assertEqual(len(outgoing_messages), 2)
        self.assertFalse('one' in e.model)
        self.assertFalse(e.model['two'].get('_renaming', False))

    def test_conflicting_interrupted_rename_faults_engine(self):
        def inner():
            e, outgoing_messages = self.get_engine({
                'alpha': {'two': ['1', ('ffs:renamed_from', 'one')]},
                'beta':  {'_two': ['1', ('ffs:renamed_from', 'shu')]},
                'gamma': {'one': ['1']},
            })
        self.assertRaises(engine.InconsistencyError, inner)

    def test_rename_but_other_already_without_rename_from(self):
        def inner():
            e, outgoing_messages = self.get_engine({
                'alpha': {'two': ['1', ('ffs:renamed_from', 'one')]},
                'beta':  {'_two': ['1', ]},
                # this one never saw the rename-command
                'gamma': {'one': ['1']},
            })
        self.assertRaises(engine.InconsistencyError, inner)

    def test_moving_and_renaming_faults_engine(self):
        def inner():
            e, outgoing_messages = self.get_engine({
                'alpha': {'two': ['1', ('ffs:renamed_from', 'one')]},
                'beta':  {'_two': ['1', ('ffs:moving_to', 'alpha')]},
                'gamma': {'one': ['1']},
            })
        self.assertRaises(engine.InconsistencyError, inner)

    def test_rename_and_having_to_restore_main_readonly_faults_engine(self):
        def inner():
            e, outgoing_messages = self.get_engine({
                'alpha': {'two': ['1', ('ffs:renamed_from', 'one')]},
                'beta':  {'two': ['1', ('readonly', 'off'), ('ffs:main', 'off')]},
                'gamma': {'one': ['1']},
            })
        self.assertRaises(engine.InconsistencyError, inner)

    def test_no_remove_during_rename(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'one': ['1']},
            'beta':  {'_one': ['1']},
            'gamma': {'one': ['1']},
        })
        self.assertEqual(0, len(outgoing_messages))
        e.incoming_client({"msg": 'rename',
                           'ffs': 'one',
                           'new_name': 'two'})

        def inner():
            e.incoming_client({
                "msg": "remove_target", 'ffs': 'one', 'target': 'alpha'
            })
        self.assertRaises(engine.RenameInProgress, inner)

    def test_no_rename_during_remove(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'one': ['1']},
            'beta':  {'_one': ['1']},
            'gamma': {'one': ['1']},
        })
        self.assertEqual(0, len(outgoing_messages))
        e.incoming_client({"msg": 'remove_target',
                           'ffs': 'one',
                           'target': 'alpha'})

        def inner():
            e.incoming_client({
                "msg": "rename", 'ffs': 'one', 'new_name': 'two'
            })
        self.assertRaises(engine.RemoveInProgress, inner)

    def test_no_rename_during_move(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'one': ['1']},
            'beta':  {'_one': ['1']},
            'gamma': {'one': ['1']},
        })
        self.assertEqual(0, len(outgoing_messages))
        e.incoming_client({"msg": 'move_main',
                           'ffs': 'one',
                           'target': 'alpha'})

        def inner():
            e.incoming_client({
                "msg": "rename", 'ffs': 'one', 'new_name': 'two'
            })
        self.assertRaises(engine.MoveInProgress, inner)

    def test_no_rename_during_new(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {},
            'beta':  {'_one': ['1']},
            'gamma': {},
        })
        self.assertEqual(0, len(outgoing_messages))
        e.incoming_client({"msg": 'add_targets',
                           'ffs': 'one',
                           'targets': ['alpha']})

        def inner():
            e.incoming_client({
                "msg": "rename", 'ffs': 'one', 'new_name': 'two'
            })
        self.assertRaises(engine.NewInProgress, inner)

    def test_rename_requires_ffs(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                'msg': 'rename', 'new_name': 'two'
            })
        self.assertRaises(engine.CodingError, inner)

    def test_rename_requires_new_name(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                'msg': 'rename', 'ffs': 'one'
            })
        self.assertRaises(engine.CodingError, inner)

    def test_rename_invalid_ffs(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                'msg': 'rename', 'ffs': 'two', 'new_name': 'three'
            })
        self.assertRaises(ValueError, inner)

    def test_duplicate_rename_raises(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })
        e.incoming_client({
            'msg': 'rename', 'ffs': 'one', 'new_name': 'two'
        })

        def inner():
            e.incoming_client({
                'msg': 'rename', 'ffs': 'one', 'new_name': 'three'
            })
        self.assertRaises(engine.RenameInProgress, inner)


class ChmodTests(PostStartupTests):

    def test_basic(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })
        r = e.incoming_client({
            'msg': 'chown_and_chmod',
            'ffs': 'one',
            'sub_path': '/code'
        })
        self.assertEqual(r, {'ok': True})
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0],
                            {
            'msg': 'chown_and_chmod',
            'ffs': 'one',
            'sub_path': '/code',
            'to': 'beta',
            'user': e.config.get_chown_user('one'),
            'rights': e.config.get_chmod_rights('one')
        }
        )

    def test_raises_no_ffs(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                'msg': 'chown_and_chmod',
                'sub_path': 'code'
            })
        self.assertRaises(engine.CodingError, inner)

    def test_raises_invalid_ffs(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                'msg': 'chown_and_chmod',
                'ffs': 'two',
                'sub_path': 'code',
            })
        self.assertRaises(ValueError, inner)

    def test_raises_no_sub_path(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                'msg': 'chown_and_chmod',
                'ffs': 'one',
            })
        self.assertRaises(engine.CodingError, inner)

    def test_raises_invalid_sub_path(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                'msg': 'chown_and_chmod',
                'ffs': 'one',
                'sub_path': '../code'
            })
        self.assertRaises(ValueError, inner)


class TimeBasedSnapshotTests(PostStartupTests):

    def test_setting_interval_on(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
            'alpha':  {'one': ['1']},
        })
        e.incoming_client({
            "msg": 'set_snapshot_interval',
            'ffs': 'one',
            'interval': 34
        })
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:snapshot_interval': '34'}
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'set_properties',
            'to': 'beta',
            'ffs': 'one',
            'properties': {'ffs:snapshot_interval': '34'}
        })

    def test_setting_interval_no_ffs(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                "msg": 'set_snapshot_interval',
                'interval': 34
            })
        self.assertRaises(ValueError, inner)

    def test_setting_interval_invalid_ffs(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                "msg": 'set_snapshot_interval',
                'ffs': 'four',
                'interval': 34
            })
        self.assertRaises(ValueError, inner)

    def test_setting_interval_invalid_interval(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                "msg": 'set_snapshot_interval',
                'ffs': 'one',
                'interval': '34',
            })
        self.assertRaises(ValueError, inner)

    def test_setting_interval_no_interval(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
        })

        def inner():
            e.incoming_client({
                "msg": 'set_snapshot_interval',
                'ffs': 'one',
            })
        self.assertRaises(ValueError, inner)

    def test_invalid_interval_during_startup_faults(self):
        def inner():
            e, outgoing_messages = self.get_engine({
                'beta':  {'_one': ['1', ('ffs:snapshot_interval', 'shu')]},
            })
        self.assertRaises(engine.ManualInterventionNeeded, inner)

    def test_negative_interval_during_startup(self):
        def inner():
            e, outgoing_messages = self.get_engine({
                'beta':  {'_one': ['1', ('ffs:snapshot_interval', '-1')]},
            })
        self.assertRaises(engine.ManualInterventionNeeded, inner)

    def test_auto_snapshot(self):
        def name_snapshot(offset_for_testing):
            t = time.gmtime(time.time() + offset_for_testing)
            t = ["%.4i" % t.tm_year, "%.2i" % t.tm_mon, "%.2i" % t.tm_mday,
                 "%.2i" % t.tm_hour, "%.2i" % t.tm_min, "%.2i" % t.tm_sec]
            return 'ffs-' + '-'.join(t)

        e, outgoing_messages = self.get_engine({
            'beta':  {
                '_one': ['1', ('ffs:snapshot_interval', 15)],
                '_two': [('ffs:snapshot_interval', 30)],
                '_three': ['ffs-shubidudh', ('ffs:snapshot_interval', 15)],
                '_four': [name_snapshot(0), ('ffs:snapshot_interval', 15)],
                '_five': [name_snapshot(-161), ('ffs:snapshot_interval', 1)],
                '_six': [name_snapshot(-161), ('ffs:snapshot_interval', 1)],
            },
        })
        self.assertFalse(e.model['one']['beta']['upcoming_snapshots'])
        self.assertFalse(e.model['two']['beta']['upcoming_snapshots'])
        self.assertFalse(e.model['three']['beta']['upcoming_snapshots'])
        self.assertFalse(e.model['four']['beta']['upcoming_snapshots'])
        self.assertFalse(e.model['five']['beta']['upcoming_snapshots'])
        self.assertFalse(e.model['six']['beta']['upcoming_snapshots'])
        # fake an outgoing snapshot
        e.model['six']['beta']['upcoming_snapshots'] = ['blocks_auto']
        e.one_minute_passed()
        # no snapshot before
        self.assertTrue(e.model['two']['beta']['upcoming_snapshots'])
        # unparsable - not ffs before.
        self.assertTrue(e.model['one']['beta']['upcoming_snapshots'])
        self.assertTrue(e.model['three']['beta']['upcoming_snapshots'])
        # time has not passed
        self.assertFalse(e.model['four']['beta']['upcoming_snapshots'])
        # time has passed
        self.assertTrue(e.model['five']['beta']['upcoming_snapshots'])
        # not if there's a currently outgoing snapshot
        self.assertEqual(len(e.model['six']['beta']['upcoming_snapshots']), 1)

    def test_no_interval_if_faulted(self):
        def name_snapshot(offset_for_testing):
            t = time.gmtime(time.time() + offset_for_testing)
            t = ["%.4i" % t.tm_year, "%.2i" % t.tm_mon, "%.2i" % t.tm_mday,
                 "%.2i" % t.tm_hour, "%.2i" % t.tm_min, "%.2i" % t.tm_sec]
            return 'ffs-' + '-'.join(t)


        e, outgoing_messages = self.get_engine({
            'beta':  {
                '_five': [name_snapshot(-161), ('ffs:snapshot_interval', 1)],
            }
        })
        e.one_minute_passed()
        # time has passed
        self.assertTrue(e.model['five']['beta']['upcoming_snapshots'])
        e.model['five']['beta']['upcoming_snapshots'].clear()
        e.one_minute_passed()
        self.assertTrue(e.model['five']['beta']['upcoming_snapshots'])
        e.model['five']['beta']['upcoming_snapshots'].clear()
        def inner():
            e.fault("test")
        self.assertRaises(engine.ManualInterventionNeeded, inner)
        self.assertFalse(e.model['five']['beta']['upcoming_snapshots'])


    def test_main_has_no_interval_others_do(self):
        def inner():
            e, outgoing_messages = self.get_engine({
                'beta':  {'_one': ['1']},
                'alpha':  {'one': ['1', ('ffs:snapshot_interval', '50')]},
            })
        self.assertRaises(engine.ManualInterventionNeeded, inner)

    def test_conflicting_intervals_between_main_and_target(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1', ('ffs:snapshot_interval', '100')]},
            'alpha':  {'one': ['1', ('ffs:snapshot_interval', '50')]},
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:snapshot_interval': '100'},
            'to': 'alpha'
        })
        
 



class ClientFacingTests(PostStartupTests):

    def test_list_ffs(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
            'alpha':  {'one': ['1']},
        })
        l = e.client_list_ffs()
        self.assertEqual(l, {'one': ['beta', 'alpha']})

    def test_client_list_ffs_in_case_of_new(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
            'alpha':  {'one': ['1']},
        })
        l = e.client_list_ffs()
        self.assertEqual(l, {'one': ['beta', 'alpha']})
        e.incoming_client({'msg': 'new', 'ffs': 'two', 'targets': ['beta']})
        l = e.client_list_ffs()
        self.assertEqual(l, {'one': ['beta', 'alpha'], 'two': ['beta']})

    def test_client_list_ffs_in_case_of_add_target(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
            'alpha': {},
        })
        l = e.client_list_ffs()
        self.assertEqual(l, {'one': ['beta']})
        e.incoming_client(
            {'msg': 'add_targets', 'ffs': 'one', 'targets': ['alpha']})
        l = e.client_list_ffs()
        self.assertEqual(l, {'one': ['beta', 'alpha']})


class FailureTests(unittest.TestCase):

    def test_deploy_failed(self):
        raise NotImplementedError("Make sure complaint is spot on")


    def test_ssh_connect_failed(self):
        raise NotImplementedError()

    def test_rsync_went_away_because_receiving_host_died(self):
        raise NotImplementedError("""   ssh_message_que:154 Node processing error in job_id return 137 {'error': 'rsync_failure', 'ssh_process               _return_code': 1, 'content': "stdout:\nb''\n\nstderr:\nb'rsync\\n\\nsudo rsync --rsync-path=rprsync --delete --delay-updates --om               it-dir-times -ltx -perms --super --owner --group --recursive -e ssh -p 223 -o StrictHostKeyChecking=no -i /home/ffs/.ssh/id_rsa /               martha/ffs/.ffs_sync_clones/1522834093.737729_aecc9d2d8fd5a0a0f2881ca8511a1fa7/results/ ffs@rose:/rose/ffs/e/20161012_AG_Mermoud_               SMARCAD_H3K9Me3_ChIP_mouse_ES/results/\\n rsync returncode: 255packet_write_wait: Connection to 192.168.153.1 port 223: Broken pi               pe\\r\\nrsync: [sender] write error: Broken pipe (32)\\nrsync error: unexplained error (code 255) at io.c(820) [sender=3.1.1]\\n'               ", 'from': 'martha'} No message in msg, outgoing was: {'ffs': 'e/20161012_AG_Mermoud_SMARCAD_H3K9Me3_ChIP_mouse_ES',
        """)



class PriorityTests(PostStartupTests):
    def test_non_int_prio_raises_on_starutp(self):
        def inner():
            e, outgoing_messages = self.get_engine({
                'beta':  {'_one': ['1', ('ffs:priority', 'shu')]},
            })
        self.assertRaises(engine.ManualInterventionNeeded, inner)

    def test_prio_different_between_main_and_other(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1', ('ffs:priority', '100')]},
            'alpha':  {'one': ['1', ('ffs:priority', '50')]},
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:priority': '100'},
            'to': 'alpha'
        })
        
 

    def test_prio_only_set_on_non_main(self):
        def inner():
            e, outgoing_messages = self.get_engine({
                'beta':  {'_one': ['1']},
                'alpha':  {'one': ['1', ('ffs:priority', '50')]},
            })
        self.assertRaises(engine.ManualInterventionNeeded, inner)

     
    def test_client_set_prio_not_specified(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
            'alpha':  {'one': ['1']},
        })
        def inner():
            e.incoming_client({
                "msg": 'set_priority',
                'ffs': 'one',
                'interval': 34
            })
        self.assertRaises(engine.CodingError, inner)


    def test_client_set_prio(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
            'alpha':  {'one': ['1']},
        })
        e.incoming_client({
            "msg": 'set_priority',
            'ffs': 'one',
            'priority': 34
        })
        self.assertMsgEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:priority': '34'}
        })
        self.assertMsgEqual(outgoing_messages[1], {
            'msg': 'set_properties',
            'to': 'beta',
            'ffs': 'one',
            'properties': {'ffs:priority': '34'}
        })

    def test_prio_reorder(self):
        e, outgoing_messages = self.get_engine({
            'beta':  {'_one': ['1']},
            'alpha':  {'one': ['1']},
        })

        o = ssh_message_que.OutgoingMessages(None, e, None)
        msgs = [
            ssh_message_que.MessageInProgress('node1', {}, {'msg': 'remove_snapshot', 'priority': 1000, 'snapshot': 'b'}),
            ssh_message_que.MessageInProgress('node1', {}, {'msg': 'remove_snapshot', 'priority': 900, 'snapshot': 'a'}),
            ssh_message_que.MessageInProgress('node1', {}, {'msg': 'capture', 'ffs': 'b'}),
            ssh_message_que.MessageInProgress('node1', {}, {'msg': 'new', 'ffs': 'a'}),
            ssh_message_que.MessageInProgress('node1', {}, {'msg': 'send_snapshot', 'ffs': 'b', 'snapshot': 'a'}),
        ]
        ordered = list(o.prioritize(msgs))
        self.assertEqual(ordered[0], msgs[3])
        self.assertEqual(ordered[1], msgs[2])
        self.assertEqual(ordered[2], msgs[4])
        self.assertEqual(ordered[3], msgs[1])
        self.assertEqual(ordered[4], msgs[0])



if __name__ == '__main__':
    unittest.main()

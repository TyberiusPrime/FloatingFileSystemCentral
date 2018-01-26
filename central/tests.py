import unittest
from pprint import pprint
import collections
import engine


class EngineTests(unittest.TestCase):

    def get_engine(self, quick_ffs_definition):
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
        config = {}
        for name in quick_ffs_definition:
            config[name] = {'storage_prefix': '/' + name, 'hostname': name}

        outgoing_messages = []

        def send(receiver, msg):
            msg['to'] = receiver['hostname']
            outgoing_messages.append(msg)
        e = engine.Engine(
            config,
            send
        )
        e.incoming_client({'msg': 'startup', })
        outgoing_messages.clear()
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
        return e, outgoing_messages

    def ge(self):
        nodes = collections.OrderedDict()
        nodes['alpha'] = {'storage_prefix': '/alpha', 'hostname': 'alpha'}
        nodes['beta'] = {'storage_prefix': '/beta', 'hostname': 'beta'}
        outgoing_messages = []

        def send(receiver, msg):
            msg['to'] = receiver['hostname']
            outgoing_messages.append(msg)
        return engine.Engine(
            nodes,
            send
        ), outgoing_messages


class StartupTests(EngineTests):

    def test_nodes_may_not_start_with_dash(self):
        nodes = {}
        nodes['_alpha'] = {'storage_prefix': '/alpha', 'hostname': 'alpha'}

        def ignore(*args, **kwargs):
            pass

        def inner():
            engine.Engine(
                nodes,
                ignore
            )
        self.assertRaises(ValueError, inner)

    def test_startup_sends_list_ffs(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertTrue(len(outgoing_messages), 2)
        self.assertEqual(outgoing_messages[0],
                         {
            'to': 'alpha',
            'msg': 'list_ffs'
        })
        self.assertEqual(outgoing_messages[1],
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

    def test_restoring_ro_from_main(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertTrue(len(outgoing_messages), 2)
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
        self.assertEqual(outgoing_messages[0], {
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

    def test_no_main_multiple_non_ro_raises_inconsistency(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertTrue(len(outgoing_messages), 2)
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

    def test_multiple_main_raises(self):
        e, outgoing_messages = self.ge()
        self.assertFalse(e.startup_done)
        e.incoming_client({'msg': 'startup', })
        self.assertFalse(e.startup_done)
        self.assertTrue(len(outgoing_messages), 2)
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
        self.assertTrue(len(outgoing_messages), 2)
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
        self.assertEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'readonly': 'on'}
        })
        self.assertEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'readonly': 'off'}
        })


class PostStartupTests(EngineTests):

    def ge(self):
        nodes = collections.OrderedDict()
        nodes['alpha'] = {'storage_prefix': '/alpha', 'hostname': 'alpha'}
        nodes['beta'] = {'storage_prefix': '/beta', 'hostname': 'beta'}
        outgoing_messages = []

        def send(receiver, msg):
            msg['to'] = receiver['hostname']
            outgoing_messages.append(msg)
        e = engine.Engine(
            nodes,
            send
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
        outgoing_messages.clear()
        return e, outgoing_messages


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

    def test_new_raises_on_empty_targets(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client({"msg": 'new', 'ffs': 'one', 'targets': []})
        self.assertRaises(ValueError, inner)

    def test_new_raises_on_non_existing_targets(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client(
                {"msg": 'new', 'ffs': 'one', 'targets': ['gamma']})
        self.assertRaises(ValueError, inner)

    def test_new_single_node(self):
        e, outgoing_messages = self.ge()
        e.incoming_client(
            {"msg": 'new', 'ffs': 'two', 'targets': ['alpha']})
        self.assertTrue(len(outgoing_messages), 1)
        self.assertEqual(outgoing_messages[0], {
            'to': 'alpha',
            'msg': 'new',
            'ffs': 'two',
            'properties': {
                'ffs:main': 'on',
                'readonly': 'off',
            }})
        self.assertTrue('two' in e.model)
        self.assertEqual(e.model['two']['_main'], 'alpha')
        self.assertEqual(e.model['two']['alpha'], {})
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
        self.assertEqual(e.model['two']['alpha'][
                         'properties']['ffs:main'], 'on')
        self.assertEqual(e.model['two']['alpha'][
                         'properties']['readonly'], 'off')

    def test_new_multi_nodes(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'new', 'ffs': 'two',
                           'targets': ['beta', 'alpha']})
        self.assertTrue(len(outgoing_messages), 2)
        self.assertEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'new',
            'ffs': 'two',
            'properties': {
                'ffs:main': 'on',
                'readonly': 'off',
            }})
        self.assertEqual(outgoing_messages[0], {
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


def remove_snapshot_from_message(msg):
    msg = msg.copy()
    if 'snapshot' in msg:
        del msg['snapshot']
    return msg

class CaptureTest(PostStartupTests):

    def test_new_raises_on_non_existing(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client({"msg": 'capture', 'ffs': 'two'})
        self.assertRaises(ValueError, inner)

    def test_raises_on_no_ffs(self):
        e, outgoing_messages = self.ge()

        def inner():
            e.incoming_client({"msg": 'capture'})
        self.assertRaises(ValueError, inner)

    def test_capture_sends_capture_message(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'capture', 'ffs': 'one'})
        self.assertEqual(remove_snapshot_from_message(outgoing_messages[0]), 
            {
                'to': 'beta',
                'msg': 'capture',
                'ffs': 'one'
            }
        )

    def test_unexpected_capture_explodes(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'capture', 'ffs': 'one'})
        self.assertEqual(remove_snapshot_from_message(outgoing_messages[0]), 
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
        self.assertEqual(remove_snapshot_from_message(outgoing_messages[0]), 
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
        self.assertEqual(outgoing_messages,
                         [{
                             'to': 'alpha',
                             'msg': 'pull_snapshot',
                             'ffs': 'one',
                             'pull_from': 'beta',
                             'snapshot': snapshot_name,
                         }]

                         )
        outgoing_messages.clear()
        e.incoming_node({'msg': 'pull_done',
                         'from': 'alpha',
                         'ffs': 'one',
                         'snapshot': snapshot_name,
                         })
        self.assertTrue(snapshot_name in e.model['one']['alpha']['snapshots'])


class RemoveTarget(PostStartupTests):

    def test_basic(self):
        e, outgoing_messages = self.ge()
        self.assertTrue('one' in e.model)
        self.assertFalse(e.model['one']['alpha']['removing'])
        e.incoming_client(
            {"msg": 'remove_target', 'ffs': 'one', 'target': 'alpha'})
        self.assertTrue(e.model['one']['alpha']['removing'])
        self.assertEqual(outgoing_messages, [
            {'msg': 'remove',
             'ffs': 'one',
             'to': 'alpha'}
        ])
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
        self.assertRaises(ValueError, inner)

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
        # I believe the answer is
        #cannot destroy 'ffs/dataset': dataset already exists
        # or
        #cannot destroy 'ffs/dataset': dataset is busy
        raise NotImplementedError()


class AddTarget(PostStartupTests):

    def ge(self):
        nodes = collections.OrderedDict()
        nodes['alpha'] = {'storage_prefix': '/alpha', 'hostname': 'alpha'}
        nodes['beta'] = {'storage_prefix': '/beta', 'hostname': 'beta'}
        nodes['gamma'] = {'storage_prefix': '/gamma', 'hostname': 'gamma'}
        outgoing_messages = []

        def send(receiver, msg):
            msg['to'] = receiver['hostname']
            outgoing_messages.append(msg)
        e = engine.Engine(
            nodes,
            send
        )
        e.incoming_client({'msg': 'startup', })
        e.incoming_node({'msg': 'ffs_list',
                         'from': 'beta',
                         'ffs': {
                             'one': {'snapshots': ['1'],
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

        outgoing_messages.clear()
        return e, outgoing_messages

    def test_basic(self):
        e, outgoing_messages = self.ge()
        self.assertTrue('one' in e.model)
        self.assertFalse('gamma' in e.model['one'])
        e.incoming_client({'msg':
                           'add_target',
                           'ffs': 'one',
                           'target': 'gamma'})
        self.assertEqual(outgoing_messages, [
            {'msg': 'new',
             'to': 'gamma',
             'ffs': 'one',
             'properties': {'ffs:main': 'off', 'readonly': 'on'}}
        ])
        outgoing_messages.clear()
        e.incoming_node({'msg': 'new_done',
                         'from': 'gamma',
                         'ffs': 'one',
                         'properties': {
                             'ffs:main': 'off',
                             'readonly': 'on'
                         }

                         })
        self.assertEqual(outgoing_messages, [
            {
                'msg': 'pull_snapshot',
                'ffs': 'one',
                'to': 'gamma',
                'pull_from': 'beta',
                'snapshot': '1'
            }


        ])


class TestMove(PostStartupTests):

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
                {"msg": "move_main", 'ffs': 'one', 'target': 'gamma'})
        self.assertRaises(ValueError, inner)

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

    def test_move_basic(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ('ffs:test', '2')]},
            'gamma': {},
        })
        # flow is as follows
        # 1 - set ffs:moving=target on old main, set reoad only.
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
        self.assertEqual(outgoing_messages[0], {
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
        self.assertEqual(remove_snapshot_from_message(outgoing_messages[1]), {
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
        self.assertEqual(outgoing_messages[2], {
            'to': 'beta',
            'msg': 'pull_snapshot',
            'ffs': 'one',
            'pull_from': 'alpha',
            'snapshot': outgoing_messages[1]['snapshot']
        })
        engine.incoming_node({
            'msg': 'pull_done',
            'from': 'beta',
            'ffs': 'one',
            'snapshot': outgoing_messages[1]['snapshot']
        })
        self.assertEqual(len(outgoing_messages), 4)
        self.assertEqual(outgoing_messages[3], {
            'to': 'alpha',
            'msg': 'set_properties',
            'ffs': 'one',
            'properties': {'ffs:main': 'off'}
        })
        engine.incoming_node({
            'msg': 'set_properties_done',
            'from': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:main': 'off'}
        })
        self.assertEqual(len(outgoing_messages), 5)
        self.assertEqual(outgoing_messages[4], {
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
        self.assertEqual(outgoing_messages[5], {
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


class SnapshotPruningTests(EngineTests):

    def test_simple(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['1', '2']},
            'gamma': {},
        })
        #can't have it missing by default, would trigger startup pruning,
        #and I want to test it...
        engine.model['one']['alpha']['snapshots'].remove('1')
        engine.prune_snapshots()
        self.assertEqual(len(outgoing_messages), 1)
        self.assertEqual(outgoing_messages[0], {
            'msg': 'remove_snapshot',
            'ffs': 'one',
            'snapshot': '1',
            'to': 'beta'
        })
        
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
        self.assertEqual(len(outgoing),1)
        self.assertEqual(outgoing[0], {
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
        self.assertEqual(len(outgoing),1)
        self.assertEqual(outgoing[0], {
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

        e.do_zpool_status_check()
        self.assertEqual(len(outgoing),2)
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
        self.assertEqual(len(errors), 1)  #no repetition

        e.do_zpool_status_check()
        self.assertEqual(len(outgoing),3)
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
        self.assertEqual(len(errors), 2)  #DEGRADING something else is another error

        e.do_zpool_status_check()
        self.assertEqual(len(outgoing),4)
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
        self.assertEqual(len(errors), 3)  #failing is most certainly an error again

        e.do_zpool_status_check()
        self.assertEqual(len(outgoing),5)
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
        self.assertEqual(len(errors), 4)  #for now, the all clear also comes via the error reporting mechanism


class ChownTests(PostStartupTests):

    def test_chown_standalone(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['1', '2']},
            'gamma': {},
        })
        e.incoming_client({
            'msg': 'chown_and_chmod',
            'ffs': 'one'
        })
        self.assertEqual(outgoing_messages[0], {
            'msg': 'chown_and_chmod',
            'ffs': 'one',
            'to': 'alpha'
        })


    def test_capture_and_chown(self):
        e, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['1', '2']},
            'gamma': {},
        })
        e.incoming_client({"msg": 'capture', 'ffs': 'one', 'chown_and_chmod': True})
        self.assertEqual(remove_snapshot_from_message(outgoing_messages[0]), 
            {
                'to': 'alpha',
                'msg': 'capture',
                'ffs': 'one',
                'chown_and_chmod': True,
            }
        )

        pass


class TestStartupTriggeringActions(EngineTests):

    def test_missing_snapshot_triggers_pull(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': ['1', ]},
            'gamma': {},
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertEqual(outgoing_messages[0], {
            'msg': 'pull_snapshot',
            'to': 'beta',
            'pull_from': 'alpha',
            'ffs': 'one',
            'snapshot': '2'
        })
        #
    def test_multiple_missing_snapshots(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1', '2']},
            'beta':  {'one': [ ]},
            'gamma': {},
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertEqual(outgoing_messages[0], {
            'msg': 'pull_snapshot',
            'to': 'beta',
            'pull_from': 'alpha',
            'ffs': 'one',
            'snapshot': '2'
        })
        #
 
    def test_additional_snapshots_on_target_trigger_snapshot_removal(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['2', '1', ]},
            'gamma': {},
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertEqual(outgoing_messages[0], {
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
        self.assertEqual(remove_snapshot_from_message(outgoing_messages[0]), {
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
        self.assertEqual(remove_snapshot_from_message(outgoing_messages[0]), {
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
        self.assertEqual(outgoing_messages[0], {
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
        self.assertEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:moving_to': '-'}
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
            'msg': 'pull_done',
            'from': 'beta',
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
        self.assertEqual(remove_snapshot_from_message(outgoing_messages[0]), {
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
        self.assertEqual(remove_snapshot_from_message(outgoing_messages[0]), {
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
            'msg': 'pull_done',
            'from': 'beta',
            'ffs': 'one',
            'snapshot': wrong_snapshot_name,
        })
        #this one is not supposed to trigger the move...
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
            'msg': 'pull_done',
            'from': 'beta',
            'ffs': 'one',
            'snapshot': right_snapshot_name,
        })
        self.assertEqual(len(outgoing_messages), 1)
        self.assertEqual(outgoing_messages[0], {
            'msg': 'set_properties',
            'to': 'alpha',
            'ffs': 'one',
            'properties': {'ffs:main': 'off'}
        })

        # 





if __name__ == '__main__':
    unittest.main()

import unittest
from pprint import pprint
import collections
import engine


class EngineTests(unittest.TestCase):

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


class PostStartupTests(unittest.TestCase):

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
        self.assertEqual(outgoing_messages, [
            {
                'to': 'beta',
                'msg': 'capture',
                'ffs': 'one'
            }
        ])

    def test_unexpected_capture_explodes(self):
        e, outgoing_messages = self.ge()
        e.incoming_client({"msg": 'capture', 'ffs': 'one'})
        self.assertEqual(outgoing_messages, [
            {
                'to': 'beta',
                'msg': 'capture',
                'ffs': 'one'
            }
        ])
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
        self.assertEqual(outgoing_messages, [
            {
                'to': 'beta',
                'msg': 'capture',
                'ffs': 'one'
            }
        ])
        outgoing_messages.clear()
        e.incoming_node({'msg': 'capture_done',
                         'from': 'beta',
                         'ffs': 'one',
                         'snapshot': '1'
                         })
        self.assertEqual(outgoing_messages,
                         [{
                             'to': 'alpha',
                             'msg': 'pull_snapshot',
                             'ffs': 'one',
                             'pull_from': 'beta',
                             'snapshot': '1',
                         }]

                         )
        outgoing_messages.clear()
        e.incoming_node({'msg': 'pull_done',
                         'from': 'alpha',
                         'ffs': 'one',
                         'snapshot': '1'
                         })
        self.assertTrue('1' in e.model['one']['alpha']['snapshots'])


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

    def get_engine(self, quick_ffs_definition):
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
        for node_name, ffs_to_snapshots in quick_ffs_definition.items():
            ffs = {}
            for ffs_name, snapshots_and_props in ffs_to_snapshots.items():
                if ffs_name.startswith('_'):
                    main = True
                    ffs_name = ffs_name[1:]
                    props = {'ffs:main': 'on', 'readonly': 'off'}
                else:
                    main = False
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
        outgoing_messages.clear()
        return e, outgoing_messages


    def test_move_raises_on_moving_to_main(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ('ffs:test', '2')]},
            'gamma': {},
        })
        self.assertEqual(engine.model['one']['beta']['properties']['ffs:test'], '2')
        self.assertEqual(engine.model['one']['_main'], 'alpha')
        def inner():
            engine.incoming_client({"msg": "move_main", 'ffs': 'one', 'target': 'alpha'})
        self.assertRaises(ValueError, inner)

    def test_move_raises_on_moving_to_non_target(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ('ffs:test', '2')]},
            'gamma': {},
        })
        self.assertEqual(engine.model['one']['beta']['properties']['ffs:test'], '2')
        self.assertEqual(engine.model['one']['_main'], 'alpha')
        def inner():
            engine.incoming_client({"msg": "move_main", 'ffs': 'one', 'target': 'gamma'})
        self.assertRaises(ValueError, inner)

    def test_move_raises_on_moving_to_non_existing_ffs(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ('ffs:test', '2')]},
            'gamma': {},
        })
        self.assertEqual(engine.model['one']['beta']['properties']['ffs:test'], '2')
        self.assertEqual(engine.model['one']['_main'], 'alpha')
        def inner():
            engine.incoming_client({"msg": "move_main", 'ffs': 'two', 'target': 'gamma'})
        self.assertRaises(ValueError, inner)

    def test_move_basic(self):
        engine, outgoing_messages = self.get_engine({
            'alpha': {'_one': ['1']},
            'beta':  {'one': ['1', ('ffs:test', '2')]},
            'gamma': {},
        })
        #flow is as follows
        #1 - set ffs:moving=target on old main, set reoad only.
        #2 - capture on old main
        #3 - replicate
        #4 - set ffs:main = False on old main
        #5 - set main and remove ro on new main
        #6 - remove ffs:moving on old_main

        self.assertEqual(engine.model['one']['beta']['properties']['ffs:test'], '2')
        self.assertEqual(engine.model['one']['_main'], 'alpha')
        engine.incoming_client({"msg": "move_main", 'ffs': 'one', 'target': 'beta'})
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
        self.assertEqual(outgoing_messages[1], {
            'to': 'alpha',
            'msg': 'capture',
            'ffs': 'one',
        })
        engine.incoming_node({
            'msg': 'capture_done',
            'from': 'alpha',
            'ffs': 'one',
            'snapshot': '2',
        })
        self.assertEqual(len(outgoing_messages), 3) 
        self.assertEqual(outgoing_messages[2], {
            'to': 'beta',
            'msg': 'pull_snapshot',
            'ffs': 'one',
            'pull_from': 'alpha',
            'snapshot': '2',
        })
        engine.incoming_node({
            'msg': 'pull_done',
            'from': 'beta',
            'ffs': 'one',
            'snapshot': '2',
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
        self.assertEqual(engine.model['one']['alpha']['properties']['ffs:main'], 'off')
        self.assertEqual(engine.model['one']['beta']['properties']['ffs:main'], 'on')
        self.assertEqual(engine.model['one']['alpha']['properties']['readonly'], 'on')
        self.assertEqual(engine.model['one']['beta']['properties']['readonly'], 'off')
        self.assertEqual(engine.model['one']['alpha']['properties']['ffs:moving_to'], '-')
        self.assertFalse('_moving' in engine.model['one'])
  
  

  





if __name__ == '__main__':
    unittest.main()

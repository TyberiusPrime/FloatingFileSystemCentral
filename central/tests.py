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
            'msg': 'set_property',
            'ffs': 'one',
            'property': 'ffs:main',
            'value': 'on',
        })
        outgoing_messages.clear()
        e.incoming_node({
            "msg": 'set_property_done',
            'from': 'beta',
            'ffs': 'one',
            'property': 'ffs:main',
            'value': 'on'
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
            'msg': 'set_property',
            'ffs': 'one',
            'property': 'readonly',
            'value': 'on',
        })
        self.assertEqual(outgoing_messages[1], {
            'to': 'beta',
            'msg': 'set_property',
            'ffs': 'one',
            'property': 'readonly',
            'value': 'off',
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
        self.assertEqual(e.model['two']['alpha']['properties']['ffs:main'], 'on')
        self.assertEqual(e.model['two']['alpha']['properties']['readonly'], 'off')


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
        self.assertEqual(e.model['two']['beta']['properties']['ffs:main'], 'on')
        self.assertEqual(e.model['two']['beta']['properties']['readonly'], 'off')

        self.assertEqual(e.model['two']['alpha']['properties']['ffs:main'], 'off')
        self.assertEqual(e.model['two']['alpha']['properties']['readonly'], 'on')



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
        e.incoming_client({"msg": 'remove_target', 'ffs': 'one', 'target': 'alpha'})
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
            e.incoming_client({"msg": 'remove_target', 'ffs': 'one', 'target': 'beta'})
        self.assertRaises(ValueError, inner)
        
    def test_raises_missing_target(self):
        e, outgoing_messages = self.ge()
        self.assertTrue('one' in e.model)
        self.assertFalse(e.model['one']['alpha']['removing'])
        def inner():
            e.incoming_client({"msg": 'remove_target', 'ffs': 'one', 'target': 'gamma'})
        self.assertRaises(ValueError, inner)

    def test_raises_missing_ffs(self):
        e, outgoing_messages = self.ge()
        self.assertTrue('one' in e.model)
        self.assertFalse(e.model['one']['alpha']['removing'])
        def inner():
            e.incoming_client({"msg": 'remove_target', 'ffs': 'two', 'target': 'alpha'})
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

         
   
if __name__ == '__main__':
    unittest.main()

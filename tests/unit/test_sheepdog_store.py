# Copyright 2013 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import StringIO

import mock
from oslo_concurrency import processutils

from glance_store._drivers import sheepdog
from glance_store import exceptions
from glance_store import location
from glance_store.tests import base
from tests.unit import test_store_capabilities


class TestSheepdogStore(base.StoreBaseTest,
                        test_store_capabilities.TestStoreCapabilitiesChecking):

    def setUp(self):
        """Establish a clean test environment."""
        super(TestSheepdogStore, self).setUp()

        def _fake_execute(*cmd, **kwargs):
            pass

        self.config(default_store='sheepdog',
                    group='glance_store')

        execute = mock.patch.object(processutils, 'execute').start()
        execute.side_effect = _fake_execute
        self.addCleanup(execute.stop)
        self.store = sheepdog.Store(self.conf)
        self.store.configure()

        self.called_commands = []
        self.loc = location.Location('test_sheepdog_store',
                                     sheepdog.StoreLocation,
                                     self.conf,
                                     store_specs={'image': 'fake_image'})

    def test_cleanup_when_add_image_exception(self):

        def _fake_run_command(command, data, *params):
            self.called_commands.append(command)

        with mock.patch.object(sheepdog.SheepdogImage, '_run_command') as cmd:
            cmd.side_effect = _fake_run_command
            data = StringIO.StringIO('xx')
            self.store.add('fake_image_id', data, 2)
            self.assertEqual(self.called_commands,
                             ['list -r', 'create', 'write',
                              'snapshot -s snap', 'delete'])

    def test_partial_get(self):
        self.assertRaises(exceptions.StoreRandomGetNotSupported,
                          self.store.get, self.loc, chunk_size=1)

    def test_delete(self):

        def _fake_exist_true():
            self.called_commands.append('exist')
            return True

        def _fake_delete_snapshot():
            self.called_commands.append('delete_snapshot')
            pass

        with mock.patch.object(sheepdog.SheepdogImage, 'exist') as exist:
            with mock.patch.object(sheepdog.SheepdogImage,
                                   'delete_snapshot') as del_snap:
                exist.side_effect = _fake_exist_true
                del_snap.side_effect = _fake_delete_snapshot

                self.store.delete(self.loc)

                self.assertEqual(self.called_commands,
                                 ['exist', 'delete_snapshot'])

    def test_delete_image_not_found(self):

        def _fake_exist_false():
            return False

        with mock.patch.object(sheepdog.SheepdogImage, 'exist') as exist:
            exist.side_effect = _fake_exist_false
            self.assertRaises(exceptions.NotFound,
                              self.store.delete, self.loc)

    def test_delete_image_failed(self):

        def _fake_exist_true():
            return True

        def _fake_run_command(command, data, *params):
            raise exceptions.BackendException()

        with mock.patch.object(sheepdog.SheepdogImage, 'exist') as exist:
            with mock.patch.object(sheepdog.SheepdogImage,
                                   '_run_command') as cmd:
                with mock.patch.object(sheepdog, 'LOG') as fake_logger:
                    exist.side_effect = _fake_exist_true
                    cmd.side_effect = _fake_run_command

                    self.assertRaises(exceptions.BackendException,
                                           self.store.delete, self.loc)

                    expected = 'Error in delete snapshot image'
                    fake_logger.error.assert_called_with(expected)

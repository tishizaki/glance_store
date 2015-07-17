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

import hashlib
import StringIO

import mock
from oslo_concurrency import processutils

import glance_store
from glance_store._drivers import sheepdog
from glance_store import exceptions
from glance_store import location
from glance_store.tests import base
from glance_store.tests.unit import test_store_capabilities

SHEEP_ADDR = '127.0.0.1'
SHEEP_PORT = 7000
VDI_NAME = 'image-id'
SHEEP_CHUNK_SIZE = 64


class TestSheepdogStore(base.StoreBaseTest,
                        test_store_capabilities.TestStoreCapabilitiesChecking):

    def setUp(self):
        """Establish a clean test environment."""
        super(TestSheepdogStore, self).setUp()
        self.config(default_store='sheepdog', group='glance_store')

        def _fake_execute(*cmd, **kwargs):
            pass

        self.store = sheepdog.Store(self.conf)
        self.store.configure()
        self.image = sheepdog.SheepdogImage(SHEEP_ADDR, SHEEP_PORT,
                                            VDI_NAME, SHEEP_CHUNK_SIZE)
        self.called_commands = []
        self.data = StringIO.StringIO('xx')
        self.loc = location.Location('test_sheepdog_store',
                                     sheepdog.StoreLocation,
                                     self.conf,
                                     store_specs={'image': 'fake_image_id'})

    def _fake_run_command(self, command, data, *params):
        self.called_commands.append(command)

    def _fake_run_command_true(self, command, data, *params):
        self.called_commands.append(command)
        return True

    @mock.patch.object(processutils, 'execute')
    def test_run_command(self, fake_execute):
        dummy_cmd = 'write'
        data = self.data
        offset = 1
        count = 2
        args = (dummy_cmd, self.data, str(offset), str(count))

        # Test1: argument check
        expected_cmd = (('dog vdi %(cmd)s -a %(addr)s -p %(port)s '
                        '%(vdiname)s %(offset)s %(count)s') %
                        {'cmd': dummy_cmd, 'addr': SHEEP_ADDR,
                         'port': SHEEP_PORT, 'vdiname': VDI_NAME,
                         'offset': str(offset), 'count': str(count)})
        self.image._run_command(*args)
        fake_execute.assert_called_once_with(expected_cmd,
                                             process_input=data, shell=True)

        # Test2: fail to execute command
        fake_execute.reset_mock()
        fake_execute.side_effect = processutils.ProcessExecutionError
        self.assertRaises(glance_store.BackendException,
                          self.image._run_command, *args)

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_read_from_snapshot(self, fake_run_command):
        read_offset = 1
        read_count = 1

        expected_cmd = ('read -s glance-image', None,
                        str(read_offset), str(read_count))
        self.image.read(read_offset, read_count)
        fake_run_command.assert_called_once_with(*expected_cmd)

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_create_snapshot(self, fake_run_command):

        fake_run_command.side_effect = self._fake_run_command
        self.image.create_snapshot()
        self.assertEqual(self.called_commands,
                         ['snapshot -s glance-image'])

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_delete_snapshot(self, fake_run_command):

        fake_run_command.side_effect = self._fake_run_command
        self.image.delete_snapshot()
        self.assertEqual(self.called_commands, ['delete -s glance-image'])

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_exist(self, fake_run_command):

        fake_run_command.side_effect = self._fake_run_command_true
        self.assertTrue(self.image.exist())
        self.assertEqual(self.called_commands, ['list -r'])

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_add(self, fake_run_command):

        fake_run_command.side_effect = self._fake_run_command
        ret = self.store.add('fake_image_id', self.data, 2)
        self.assertEqual(self.called_commands,
                         ['list -r', 'create', 'write',
                          'snapshot -s glance-image', 'delete'])
        self.assertEqual(('sheepdog://fake_image_id', 2,
                         hashlib.md5(self.data.getvalue()).hexdigest(),
                         {}), ret)

    @mock.patch.object(sheepdog.SheepdogImage, 'exist')
    def test_add_image_already_exist(self, fake_exist):

        def _fake_exist():
            self.called_commands.append('exist')
            return True
        fake_exist.side_effect = _fake_exist
        exc = self.assertRaises(exceptions.Duplicate,
                                self.store.add, 'fake_image_id',
                                self.data, 2)
        expect = "Image fake_image_id already exists"
        self.assertEqual(expect, exc.msg)
        self.assertEqual(self.called_commands, ['exist'])

    @mock.patch.object(sheepdog.SheepdogImage, 'create')
    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    @mock.patch.object(sheepdog, 'LOG')
    def test_add_image_create_fail(self, fake_logger, fake_run_command,
                                   fake_create):

        def _fake_create(size):
            self.called_commands.append('create')
            raise exceptions.BackendException()

        fake_run_command.side_effect = self._fake_run_command
        fake_create.side_effect = _fake_create
        self.assertRaises(exceptions.BackendException,
                          self.store.add, 'fake_image_id', self.data, 2)
        self.assertEqual(self.called_commands, ['list -r', 'create'])
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(sheepdog.SheepdogImage, 'create_snapshot')
    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    @mock.patch.object(sheepdog, 'LOG')
    def test_add_image_snapshot_fail(self, fake_logger, fake_run_command,
                                     fake_create_snapshot):

        def _fake_create_snapshot():
            self.called_commands.append('snapshot -s glance-image')
            raise exceptions.BackendException()

        fake_run_command.side_effect = self._fake_run_command
        fake_create_snapshot.side_effect = _fake_create_snapshot
        self.assertRaises(exceptions.BackendException,
                          self.store.add, 'fake_image_id', self.data, 2)
        self.assertEqual(self.called_commands,
                         ['list -r', 'create', 'write',
                          'snapshot -s glance-image', 'delete'])
        self.assertTrue(fake_logger.error.called)

    @mock.patch.object(sheepdog.SheepdogImage, 'delete')
    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    @mock.patch.object(sheepdog, 'LOG')
    def test_add_image_delete_fail(self, fake_logger, fake_run_command,
                                   fake_delete):
        def _fake_delete():
            self.called_commands.append('delete')
            raise exceptions.BackendException()

        fake_run_command.side_effect = self._fake_run_command
        fake_delete.side_effect = _fake_delete
        self.assertRaises(exceptions.BackendException,
                          self.store.add, 'fake_image_id', self.data, 2)
        self.assertEqual(self.called_commands,
                         ['list -r', 'create', 'write',
                          'snapshot -s glance-image', 'delete',
                          'delete -s glance-image'])
        self.assertTrue(fake_logger.error.called)

    def test_partial_get(self):
        self.assertRaises(exceptions.StoreRandomGetNotSupported,
                          self.store.get, self.loc, chunk_size=1)

    @mock.patch.object(sheepdog.SheepdogImage, 'exist')
    @mock.patch.object(sheepdog.SheepdogImage, 'delete_snapshot')
    def test_delete(self, fake_delete_snapshot, fake_exist):

        def _fake_exist_true():
            self.called_commands.append('exist')
            return True

        def _fake_delete_snapshot():
            self.called_commands.append('delete_snapshot')

        fake_exist.side_effect = _fake_exist_true
        fake_delete_snapshot.side_effect = _fake_delete_snapshot
        self.store.delete(self.loc)
        self.assertEqual(self.called_commands, ['exist', 'delete_snapshot'])

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_delete_image_not_found(self, fake_run_command):

        fake_run_command.return_value = False
        self.assertRaises(exceptions.NotFound, self.store.delete, self.loc)

    @mock.patch.object(sheepdog.SheepdogImage, 'exist')
    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_image_failed(self, fake_logger, fake_run_command,
                                 fake_exist):

        fake_exist.return_value = True
        fake_run_command.side_effect = exceptions.BackendException()
        self.assertRaises(exceptions.BackendException,
                          self.store.delete, self.loc)
        self.assertTrue(fake_logger.error.called)

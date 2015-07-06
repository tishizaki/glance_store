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

        def _fake_execute(*cmd, **kwargs):
            pass

        self.addr = 'localhost'
        self.port = 7000
        self.name = '_fake_image_'
        self.chunk_size = 64
        """Establish a clean test environment."""
        super(TestSheepdogStore, self).setUp()
        self.config(default_store='sheepdog',
                    group='glance_store')
        execute = mock.patch.object(processutils, 'execute').start()
        execute.side_effect = _fake_execute
        self.addCleanup(execute.stop)
        self.store = sheepdog.Store(self.conf)
        self.store.configure()
        self.image = sheepdog.SheepdogImage(self.addr, self.port, self.name,
                                            self.chunk_size)
        self.called_commands = []
        self.read_offset = 1
        self.read_count = 1
        self.data = StringIO.StringIO('xx')
        self.loc = location.Location('test_sheepdog_store',
                                     sheepdog.StoreLocation,
                                     self.conf,
                                     store_specs={'image': 'fake_image_id'})

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_read_from_snapshot(self, cmd):

        def _fake_run_command(command, data, *params):
            self.called_commands.append(command)

        cmd.side_effect = _fake_run_command
        self.image.read(self.read_offset, self.read_count)
        self.assertEqual(self.called_commands, ['read -s glance-image'])

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_read_from_snapshot_fail(self, cmd):

        def _fake_run_command(command, data, *params):
            actual = 'fake error log'
            raise exceptions.BackendException(actual)
        cmd.side_effect = _fake_run_command
        ex = self.assertRaises(exceptions.BackendException,
                               self.image.read,
                               self.read_offset, self.read_count)
        expected = 'fake error log'
        self.assertEqual(ex.message, expected)

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_create_snapshot(self, cmd):

        def _fake_run_command(command, data, *params):
            self.called_commands.append(command)
        cmd.side_effect = _fake_run_command
        self.image.create_snapshot()
        self.assertEqual(self.called_commands,
                         ['snapshot -s glance-image'])

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_create_snapshot_fail(self, cmd):

        def _fake_run_command(command, data, *params):
            actual = 'fake error log'
            raise exceptions.BackendException(actual)
        cmd.side_effect = _fake_run_command
        ex = self.assertRaises(exceptions.BackendException,
                               self.image.create_snapshot)
        expected = 'fake error log'
        self.assertEqual(ex.message, expected)

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_delete_snapshot(self, cmd):

        def _fake_run_command(command, data, *params):
            self.called_commands.append(command)
        cmd.side_effect = _fake_run_command
        self.image.delete_snapshot()
        self.assertEqual(self.called_commands, ['delete -s glance-image'])

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_delete_snapshot_fail(self, cmd):

        def _fake_run_command(command, data, *params):
            actual = 'fake error log'
            raise exceptions.BackendException(actual)
        cmd.side_effect = _fake_run_command
        ex = self.assertRaises(exceptions.BackendException,
                               self.image.delete_snapshot)
        expected = 'fake error log'
        self.assertEqual(ex.message, expected)

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_exist(self, cmd):

        def _fake_run_command(command, data, *params):
            self.called_commands.append(command)
            return True
        cmd.side_effect = _fake_run_command
        ret = self.image.exist()
        self.assertTrue(ret)
        self.assertEqual(self.called_commands, ['list -r'])

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_exist_fail(self, cmd):

        def _fake_run_command(command, data, *params):
            self.called_commands.append(command)
            actual = 'fake error log'
            raise exceptions.BackendException(actual)
        cmd.side_effect = _fake_run_command
        ex = self.assertRaises(exceptions.BackendException,
                               self.image.exist)
        expected = 'fake error log'
        self.assertEqual(ex.message, expected)

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_add(self, cmd):

        def _fake_run_command(command, data, *params):
            self.called_commands.append(command)
        cmd.side_effect = _fake_run_command
        ret = self.store.add('fake_image_id', self.data, 2)
        self.assertEqual(self.called_commands,
                         ['list -r', 'create', 'write',
                          'snapshot -s glance-image', 'delete'])
        self.assertEqual(ret[0], 'sheepdog://fake_image_id')
        self.assertEqual(ret[1], 2)
        self.assertIsInstance(ret[2], str)
        self.assertEqual(len(ret[2]), 32)

    @mock.patch.object(sheepdog.SheepdogImage, 'exist')
    def test_add_image_already_exist(self, exist):

        def _fake_exist():
            self.called_commands.append('exist')
            return True

        exist.side_effect = _fake_exist
        exc = self.assertRaises(exceptions.Duplicate,
                                self.store.add, 'fake_image_id',
                                self.data, 2)
        expect = "Image fake_image_id already exists"
        self.assertEqual(expect, exc.msg)
        self.assertEqual(self.called_commands, ['exist'])

    @mock.patch.object(sheepdog.SheepdogImage, 'create')
    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    @mock.patch.object(sheepdog, 'LOG')
    def test_add_image_create_fail(self, fake_logger, cmd, create):

        def _fake_run_command(command, data, *params):
            self.called_commands.append(command)

        def _fake_create(size):
            self.called_commands.append('create')
            raise exceptions.BackendException()

        cmd.side_effect = _fake_run_command
        create.side_effect = _fake_create
        self.assertRaises(exceptions.BackendException,
                          self.store.add, 'fake_image_id', self.data, 2)
        self.assertEqual(self.called_commands, ['list -r', 'create', 'delete'])
        expected = 'Error in create image'
        fake_logger.error.assert_called_with(expected)

    @mock.patch.object(sheepdog.SheepdogImage, 'create_snapshot')
    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    @mock.patch.object(sheepdog, 'LOG')
    def test_add_image_snapshot_fail(self, fake_logger, cmd, snap):

        def _fake_run_command(command, data, *params):
            self.called_commands.append(command)

        def _fake_create_snapshot():
            self.called_commands.append('snapshot -s glance-image')
            raise exceptions.BackendException()

        cmd.side_effect = _fake_run_command
        snap.side_effect = _fake_create_snapshot
        self.assertRaises(exceptions.BackendException,
                          self.store.add, 'fake_image_id', self.data, 2)
        self.assertEqual(self.called_commands,
                         ['list -r', 'create', 'write',
                          'snapshot -s glance-image', 'delete'])
        expected = 'Error in create image'
        fake_logger.error.assert_called_with(expected)

    @mock.patch.object(sheepdog.SheepdogImage, 'delete')
    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    @mock.patch.object(sheepdog, 'LOG')
    def test_add_image_delete_fail(self, fake_logger, cmd, delete):
        def _fake_run_command(command, data, *params):
            self.called_commands.append(command)

        def _fake_delete():
            self.called_commands.append('delete')
            raise exceptions.BackendException()

        cmd.side_effect = _fake_run_command
        delete.side_effect = _fake_delete
        self.assertRaises(exceptions.BackendException,
                          self.store.add, 'fake_image_id', self.data, 2)
        self.assertEqual(self.called_commands,
                         ['list -r', 'create', 'write',
                          'snapshot -s glance-image', 'delete',
                          'delete -s glance-image'])
        expected = 'Error in delete image'
        fake_logger.error.assert_called_with(expected)

    def test_partial_get(self):
        self.assertRaises(exceptions.StoreRandomGetNotSupported,
                          self.store.get, self.loc, chunk_size=1)

    @mock.patch.object(sheepdog.SheepdogImage, 'exist')
    @mock.patch.object(sheepdog.SheepdogImage, 'delete_snapshot')
    def test_delete(self, del_snap, exist):

        def _fake_exist_true():
            self.called_commands.append('exist')
            return True

        def _fake_delete_snapshot():
            self.called_commands.append('delete_snapshot')

        exist.side_effect = _fake_exist_true
        del_snap.side_effect = _fake_delete_snapshot
        self.store.delete(self.loc)
        self.assertEqual(self.called_commands, ['exist', 'delete_snapshot'])

    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    def test_delete_image_not_found(self, cmd):

        def _fake_run_command(command, data, *params):
            return False
        cmd.side_effect = _fake_run_command
        self.assertRaises(exceptions.NotFound, self.store.delete, self.loc)

    @mock.patch.object(sheepdog.SheepdogImage, 'exist')
    @mock.patch.object(sheepdog.SheepdogImage, '_run_command')
    @mock.patch.object(sheepdog, 'LOG')
    def test_delete_image_failed(self, fake_logger, cmd, exist):

        def _fake_exist_true():
            return True

        def _fake_run_command(command, data, *params):
            raise exceptions.BackendException()

        exist.side_effect = _fake_exist_true
        cmd.side_effect = _fake_run_command
        self.assertRaises(exceptions.BackendException,
                          self.store.delete, self.loc)
        expected = 'Error in delete snapshot image'
        fake_logger.error.assert_called_with(expected)

from __future__ import absolute_import
from unittest import TestCase
from ddsc.cmdparser import CommandParser


def no_op():
    pass


class TestCommandParser(TestCase):
    def setUp(self):
        self.parsed_args = None

    def set_parsed_args(self, args):
        self.parsed_args = args

    def test_register_add_user_command_no_msg(self):
        command_parser = CommandParser()
        command_parser.register_add_user_command(no_op)
        self.assertEqual(['add-user'], list(command_parser.subparsers.choices.keys()))

    def test_register_remove_user_command(self):
        command_parser = CommandParser()
        command_parser.register_remove_user_command(no_op)
        self.assertEqual(['remove-user'], list(command_parser.subparsers.choices.keys()))

    def test_deliver_no_msg(self):
        command_parser = CommandParser()
        command_parser.register_deliver_command(self.set_parsed_args)
        self.assertEqual(['deliver'], list(command_parser.subparsers.choices.keys()))
        command_parser.run_command(['deliver', '-p', 'someproject', '--user', 'joe123'])
        self.assertEqual(None, self.parsed_args.msg_file)

    def test_deliver_with_msg(self):
        command_parser = CommandParser()
        command_parser.register_deliver_command(self.set_parsed_args)
        self.assertEqual(['deliver'], list(command_parser.subparsers.choices.keys()))
        command_parser.run_command(['deliver', '-p', 'someproject', '--user', 'joe123', '--msg-file', 'setup.py'])
        self.assertIn('setup(', self.parsed_args.msg_file.read())

    def test_share_no_msg(self):
        command_parser = CommandParser()
        command_parser.register_share_command(self.set_parsed_args)
        self.assertEqual(['share'], list(command_parser.subparsers.choices.keys()))
        command_parser.run_command(['share', '-p', 'someproject', '--user', 'joe123'])
        self.assertEqual(None, self.parsed_args.msg_file)

    def test_share_with_msg(self):
        command_parser = CommandParser()
        command_parser.register_share_command(self.set_parsed_args)
        self.assertEqual(['share'], list(command_parser.subparsers.choices.keys()))
        command_parser.run_command(['share', '-p', 'someproject', '--user', 'joe123', '--msg-file', 'setup.py'])
        self.assertIn('setup(', self.parsed_args.msg_file.read())

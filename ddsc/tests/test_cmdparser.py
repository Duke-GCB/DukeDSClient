from __future__ import absolute_import
from unittest import TestCase
from ddsc.cmdparser import CommandParser


def no_op():
    pass


class TestCommandParser(TestCase):
    def test_register_add_user_command(self):
        command_parser = CommandParser()
        command_parser.register_add_user_command(no_op)
        self.assertEqual(['add-user'], list(command_parser.subparsers.choices.keys()))

    def test_register_remove_user_command(self):
        command_parser = CommandParser()
        command_parser.register_remove_user_command(no_op)
        self.assertEqual(['remove-user'], list(command_parser.subparsers.choices.keys()))

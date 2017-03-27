from __future__ import absolute_import
from unittest import TestCase
from ddsc.cmdparser import CommandParser


class TestCommandParser(TestCase):
    def test_register_add_user_command(self):
        command_parser = CommandParser()
        def call_func():
            pass
        command_parser.register_add_user_command(call_func)
        self.assertEqual(['add-user'], command_parser.subparsers.choices.keys())

    def test_register_remove_user_command(self):
        command_parser = CommandParser()
        def call_func():
            pass
        command_parser.register_remove_user_command(call_func)
        self.assertEqual(['remove-user'], command_parser.subparsers.choices.keys())


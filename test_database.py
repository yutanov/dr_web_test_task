import unittest
from io import StringIO
from unittest.mock import patch

from database import (
    Database,
    CommandProcessor,
    SetCommand,
    GetCommand,
    CountsCommand,
    EndCommand,
    main,
)


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = Database()

    def test_set_get(self):
        self.db.set("A", "10")
        self.assertEqual(self.db.get("A"), "10")
        self.assertEqual(self.db.get("B"), "NULL")

    def test_unset(self):
        self.db.set("A", "10")
        self.db.unset("A")
        self.assertEqual(self.db.get("A"), "NULL")

    def test_counts(self):
        self.db.set("A", "10")
        self.db.set("B", "20")
        self.db.set("C", "10")
        self.assertEqual(self.db.get_counts("10"), 2)
        self.assertEqual(self.db.get_counts("20"), 1)
        self.assertEqual(self.db.get_counts("30"), 0)

    def test_find(self):
        self.db.set("A", "10")
        self.db.set("B", "20")
        self.db.set("C", "10")
        self.assertEqual(self.db.find("10"), ["A", "C"])
        self.assertEqual(self.db.find("20"), ["B"])
        self.assertEqual(self.db.find("30"), [])


class TestCommands(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.processor = CommandProcessor()
        self.processor.db = self.db

    def test_set_command(self):
        cmd = SetCommand()
        cmd.execute(self.db, ["A", "10"])
        self.assertEqual(self.db.get("A"), "10")
        self.assertEqual(
            cmd.execute(self.db, ["A"]), "Wrong number of arguments for SET"
        )

    def test_get_command(self):
        self.db.set("A", "10")
        cmd = GetCommand()
        self.assertEqual(cmd.execute(self.db, ["A"]), "10")
        self.assertEqual(cmd.execute(self.db, []), "Wrong number of arguments for GET")

    def test_counts_command(self):
        self.db.set("A", "10")
        self.db.set("B", "10")
        cmd = CountsCommand()
        self.assertEqual(cmd.execute(self.db, ["10"]), "2")
        self.assertEqual(
            cmd.execute(self.db, ["10", "20"]), "Wrong number of arguments for COUNTS"
        )

    def test_end_command(self):
        cmd = EndCommand()
        with self.assertRaises(SystemExit):
            cmd.execute(self.db, [])


class TestCommandProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = CommandProcessor()

    def test_valid_commands(self):
        self.assertEqual(self.processor.process("SET A 10"), None)
        self.assertEqual(self.processor.process("GET A"), "10")
        self.assertEqual(self.processor.process("COUNTS 10"), "1")
        self.assertEqual(self.processor.process("UNSET A"), None)
        self.assertEqual(self.processor.process("GET A"), "NULL")

    def test_invalid_commands(self):
        self.assertEqual(
            self.processor.process("SET A"), "Wrong number of arguments for SET"
        )
        self.assertEqual(
            self.processor.process("GET"), "Wrong number of arguments for GET"
        )
        self.assertEqual(self.processor.process("UNKNOWN"), "Unknown command")

    def test_transaction_commands(self):
        self.assertEqual(self.processor.process("SET A 10"), None)
        self.assertEqual(self.processor.process("GET A"), "10")
        self.assertEqual(self.processor.process("UNSET A"), None)
        self.assertEqual(self.processor.process("GET A"), "NULL")


class TestIntegration(unittest.TestCase):
    @patch("sys.stdout", new_callable=StringIO)
    def test_integration(self, mock_stdout):
        inputs = [
            "GET A",
            "SET A 10",
            "GET A",
            "COUNTS 10",
            "SET B 20",
            "SET C 10",
            "COUNTS 10",
            "UNSET B",
            "GET B",
            "END",
        ]

        expected_output = ["NULL", "10", "1", "2", "NULL"]

        with patch("builtins.input", side_effect=inputs):
            try:
                main()
            except SystemExit:
                pass

        output = mock_stdout.getvalue().strip().split("\n")
        self.assertEqual(output, expected_output)


if __name__ == "__main__":
    unittest.main()

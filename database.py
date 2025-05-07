import sys
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Type


class Database:
    def __init__(self):
        self.data: Dict[str, str] = {}
        self._counts: Dict[str, int] = {}

    def set(self, name: str, value: str) -> None:
        old_value = self.data.get(name)

        if old_value is not None:
            self._decrement_count(old_value)

        self.data[name] = value
        self._increment_count(value)

    def get(self, name: str) -> str:
        return self.data.get(name, "NULL")

    def unset(self, name: str) -> None:
        if name in self.data:
            value = self.data[name]

            self._decrement_count(value)
            del self.data[name]

    def get_counts(self, value: str) -> int:
        return self._counts.get(value, 0)

    def find(self, value: str) -> List[str]:
        return sorted(name for name, val in self.data.items() if val == value)

    def _increment_count(self, value: str) -> None:
        self._counts[value] = self._counts.get(value, 0) + 1

    def _decrement_count(self, value: str) -> None:
        self._counts[value] -= 1
        if self._counts[value] == 0:
            del self._counts[value]


class Command(ABC):
    @abstractmethod
    def execute(self, db: Database, args: List[str]) -> Optional[str]:
        pass

    @staticmethod
    def validate_args(args: List[str], expected: int) -> bool:
        return len(args) == expected


class SetCommand(Command):
    def execute(self, db: Database, args: List[str]) -> Optional[str]:
        if not self.validate_args(args, 2):
            return "Wrong number of arguments for SET"
        db.set(args[0], args[1])
        return None


class GetCommand(Command):
    def execute(self, db: Database, args: List[str]) -> Optional[str]:
        if not self.validate_args(args, 1):
            return "Wrong number of arguments for GET"
        return db.get(args[0])


class UnsetCommand(Command):
    def execute(self, db: Database, args: List[str]) -> Optional[str]:
        if not self.validate_args(args, 1):
            return "Wrong number of arguments for UNSET"
        db.unset(args[0])
        return None


class CountsCommand(Command):
    def execute(self, db: Database, args: List[str]) -> Optional[str]:
        if not self.validate_args(args, 1):
            return "Wrong number of arguments for COUNTS"
        return str(db.get_counts(args[0]))


class FindCommand(Command):
    def execute(self, db: Database, args: List[str]) -> Optional[str]:
        if not self.validate_args(args, 1):
            return "Wrong number of arguments for FIND"
        result = db.find(args[0])
        return " ".join(result) if result else "NULL"


class EndCommand(Command):
    def execute(self, db: Database, args: List[str]) -> Optional[str]:
        if not self.validate_args(args, 0):
            return "END takes no arguments"
        sys.exit(0)


class CommandProcessor:
    def __init__(self):
        self.db = Database()
        self.commands: Dict[str, Type[Command]] = {
            "SET": SetCommand,
            "GET": GetCommand,
            "UNSET": UnsetCommand,
            "COUNTS": CountsCommand,
            "FIND": FindCommand,
            "END": EndCommand,
        }

    def process(self, line: str) -> Optional[str]:
        parts = line.strip().split()
        if not parts:
            return None

        cmd, *args = parts
        cmd = cmd.upper()

        if cmd not in self.commands:
            return "Unknown command"

        command = self.commands[cmd]()
        return command.execute(self.db, args)


def prompt(prefix: str = "> ") -> Optional[str]:
    try:
        return input(prefix).strip()
    except (EOFError, KeyboardInterrupt):
        return None


def main():
    processor = CommandProcessor()

    while (line := prompt()) is not None:
        if result := processor.process(line):
            print(result)


if __name__ == "__main__":
    main()

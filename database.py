import sys
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Type


class Database:
    def __init__(self):
        self.data: Dict[str, str] = {}
        self._counts: Dict[str, int] = {}
        self.transactions: List[Dict[str, Optional[str]]] = []

    def set(self, name: str, value: str) -> None:
        old_value = self.data.get(name)

        if self.transactions:
            self.transactions[-1][name] = old_value

        if old_value is not None:
            self._decrement_count(old_value)

        self.data[name] = value
        self._increment_count(value)

    def get(self, name: str) -> str:
        return self.data.get(name, "NULL")

    def unset(self, name: str) -> None:
        if name in self.data:
            value = self.data[name]

            if self.transactions:
                self.transactions[-1][name] = value

            self._decrement_count(value)
            del self.data[name]

    def get_counts(self, value: str) -> int:
        return self._counts.get(value, 0)

    def find(self, value: str) -> List[str]:
        return sorted(name for name, val in self.data.items() if val == value)

    def begin(self) -> None:
        self.transactions.append({})

    def rollback(self) -> Optional[str]:
        if not self.transactions:
            return None

        current_txn = self.transactions[-1]
        for name, old_value in current_txn.items():
            if old_value is None:
                if name in self.data:
                    self._decrement_count(self.data[name])
                    del self.data[name]
            else:
                if name in self.data:
                    self._decrement_count(self.data[name])
                self.data[name] = old_value
                self._increment_count(old_value)

        self.transactions.pop()
        return None

    def commit(self) -> Optional[str]:
        if not self.transactions:
            return None

        if len(self.transactions) > 1:
            parent_txn = self.transactions[-2]
            for name, value in self.transactions[-1].items():
                if name not in parent_txn:
                    parent_txn[name] = value

        self.transactions.pop()
        return None

    def _increment_count(self, value: str) -> None:
        self._counts[value] = self._counts.get(value, 0) + 1

    def _decrement_count(self, value: str) -> None:
        self._counts[value] -= 1
        if self._counts[value] == 0:
            del self._counts[value]

    def _remove_if_exists(self, name: str) -> None:
        if name in self.data:
            value = self.data[name]
            self._decrement_count(value)
            del self.data[name]

    def _restore_value(self, name: str, value: str) -> None:
        current_value = self.data.get(name)
        if current_value is not None:
            self._decrement_count(current_value)
        self.data[name] = value
        self._increment_count(value)


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


class BeginCommand(Command):
    def execute(self, db: Database, args: List[str]) -> Optional[str]:
        if not self.validate_args(args, 0):
            return "BEGIN takes no arguments"
        db.begin()
        return None


class RollbackCommand(Command):
    def execute(self, db: Database, args: List[str]) -> Optional[str]:
        if not self.validate_args(args, 0):
            return "ROLLBACK takes no arguments"
        return db.rollback()


class CommitCommand(Command):
    def execute(self, db: Database, args: List[str]) -> Optional[str]:
        if not self.validate_args(args, 0):
            return "COMMIT takes no arguments"
        return db.commit()


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
            "BEGIN": BeginCommand,
            "ROLLBACK": RollbackCommand,
            "COMMIT": CommitCommand,
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

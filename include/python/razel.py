# allows annotations with types that are not yet defined
from __future__ import annotations

import abc
import json
import os
from typing import ClassVar, List, Optional, Any


class Razel:
    _instance: ClassVar[Optional[Razel]] = None

    def __init__(self, workspace_dir: str) -> None:
        self._workspace_dir = workspace_dir
        self._commands: List[Command] = []

    @staticmethod
    def init(workspace_dir: str) -> Razel:
        assert Razel._instance is None
        Razel._instance = Razel(workspace_dir)
        return Razel._instance

    @staticmethod
    def instance() -> Razel:
        assert Razel._instance is not None
        return Razel._instance

    def add_data_file(self, path: str) -> File:
        return File(self._rel_path(path), True, None)

    def add_output_file(self, path: str) -> File:
        return File(self._rel_path(path), False, None)

    def add_command(
        self, name: str, executable: str, args: List[str | File], env: Optional[dict[str, str]] = None
    ) -> CustomCommand:
        name = self._sanitize_name(name)
        command = CustomCommand(name, self._rel_path(executable), args, env)
        command = self._add(command)
        assert isinstance(command, CustomCommand)  # For type-checking only
        return command

    def add_task(self):
        pass  # TODO

    def ensure_equal(self, file1: File, file2: File) -> None:
        pass  # TODO

    def ensure_not_equal(self, file1: File, file2: File) -> None:
        pass  # TODO

    def write_razel_file(self) -> None:
        with open(os.path.join(self._workspace_dir, "razel.jsonl"), "w", encoding="utf-8") as file:
            for command in self._commands:
                json.dump(command.json(), file)
                file.write("\n")

    def _add(self, command: Command) -> Command:
        for existing in self._commands:
            if existing.name == command.name:
                assert (
                    existing.json() == command.json()
                ), f"conflicting actions: {command.name}:\n{existing.command_line()}\n{command.command_line()}"
                return existing

        self._commands.append(command)
        return command

    @staticmethod
    def _sanitize_name(name: str) -> str:
        return name.replace(":", ".")  # target names may not contain ':'

    def _rel_path(self, file_name: str) -> str:
        if not os.path.isabs(file_name):
            return file_name

        return os.path.relpath(file_name, self._workspace_dir)


class File:
    def __init__(self, file_name: str, is_data: bool, created_by: Optional[Command]) -> None:
        self._file_name = file_name
        self._is_data = is_data
        self._created_by = created_by

    @property
    def file_name(self) -> str:
        return self._file_name

    @property
    def is_data(self) -> bool:
        return self._is_data

    @property
    def created_by(self) -> Optional[Command]:
        return self._created_by

    @property
    def basename(self) -> str:
        return os.path.basename(self._file_name)

    def ensure_equal(self, other: File) -> None:
        Razel.instance().ensure_equal(self, other)

    def ensure_not_equal(self, other: File) -> None:
        Razel.instance().ensure_not_equal(self, other)


class Command(abc.ABC):
    def __init__(self, name: str, outputs: List[File]) -> None:
        self._name = name
        self._outputs = outputs

    @property
    def name(self) -> str:
        return self._name

    @property
    def outputs(self) -> List[File]:
        return self._outputs

    @property
    def output(self) -> File:
        assert len(self._outputs) == 1
        return self._outputs[0]

    @abc.abstractmethod
    def command_line(self) -> str:
        pass

    @abc.abstractmethod
    def json(self) -> dict[str, Any]:
        pass


class CustomCommand(Command):
    def __init__(
        self, name: str, executable: str, args: List[str | File], env: Optional[dict[str, str]] = None
    ) -> None:
        super().__init__(name, [x for x in args if isinstance(x, File) and not x.is_data and x.created_by is None])

        self._executable = executable
        self._args = args
        self._env = env or {}

        for out in self.outputs:
            out._created_by = self

    @property
    def executable(self) -> str:
        return self._executable

    @property
    def args(self) -> List[str | File]:
        return self._args

    @property
    def env(self) -> dict[str, str]:
        return self._env

    def command_line(self) -> str:
        return "TODO"

    def json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "executable": self.executable,
            "args": [x.file_name if isinstance(x, File) else x for x in self.args],
            "inputs": [x.file_name for x in self.args if isinstance(x, File) and x.created_by != self],
            "outputs": [x.file_name for x in self.outputs],
            "env": self.env,
        }

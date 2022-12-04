# allows annotations with types that are not yet defined
from __future__ import annotations

import abc
import json
import os
from typing import ClassVar, Final, Optional, Any, TypeVar
from collections.abc import Mapping, Sequence


class Razel:
    _instance: ClassVar[Optional[Razel]] = None
    OUT_DIR: Final = "razel-out"

    def __init__(self, workspace_dir: str) -> None:
        self._workspace_dir = workspace_dir
        self._commands: list[Command] = []

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
        self, name: str, executable: str, args: Sequence[str | File], env: Optional[Mapping[str, str]] = None
    ) -> CustomCommand:
        name = self._sanitize_name(name)
        command = CustomCommand(name, self._rel_path(executable), args, env)
        return self._add(command)

    def add_task(self, name: str, task: str, args: Sequence[str | File]) -> Task:
        name = Razel._sanitize_name(name)
        command = Task(name, task, args)
        return self._add(command)

    def ensure_equal(self, file1: File, file2: File) -> None:
        name = f"{file1.basename}##shouldEqual##{file2.basename}"
        self._add(Task(name, "ensure-equal", [file1, file2]))

    def ensure_not_equal(self, file1: File, file2: File) -> None:
        name = f"{file1.basename}##shouldNotEqual##{file2.basename}"
        self._add(Task(name, "ensure-not-equal", [file1, file2]))

    def write_razel_file(self) -> None:
        with open(os.path.join(self._workspace_dir, "razel.jsonl"), "w", encoding="utf-8") as file:
            for command in self._commands:
                json.dump(command.json(), file,  separators=(',', ':'))
                file.write("\n")

    # Generic type used to ensure that _add() takes and returns the same type.
    _Command = TypeVar("_Command", bound="Command")

    def _add(self, command: _Command) -> _Command:
        for existing in self._commands:
            if existing.name == command.name:
                assert (
                    existing.json() == command.json()
                ), f"conflicting actions: {command.name}:\n{existing.command_line()}\n{command.command_line()}"
                assert isinstance(existing, type(command))
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
    def __init__(self, name: str, outputs: Sequence[File]) -> None:
        self._name = name
        self._outputs = outputs
        self._stdout: File | None = None
        self._stderr: File | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def outputs(self) -> Sequence[File]:
        return self._outputs

    @property
    def output(self) -> File:
        assert len(self._outputs) == 1
        return self._outputs[0]

    @property
    def stdout(self) -> File | None:
        return self._stdout

    @property
    def stderr(self) -> File | None:
        return self._stderr

    def ensure_equal(self, other: Command) -> None:
        assert len(self._outputs) == len(other._outputs)
        for i in range(len(self._outputs)):
            Razel.instance().ensure_equal(self._outputs[i], other._outputs[i])

    def ensure_not_equal(self, other: Command) -> None:
        assert len(self._outputs) == len(other._outputs)
        for i in range(len(self._outputs)):
            Razel.instance().ensure_not_equal(self._outputs[i], other._outputs[i])

    @abc.abstractmethod
    def command_line(self) -> str:
        pass

    @abc.abstractmethod
    def json(self) -> Mapping[str, Any]:
        pass


class CustomCommand(Command):
    def __init__(
        self, name: str, executable: str, args: Sequence[str | File], env: Optional[Mapping[str, str]] = None
    ) -> None:
        super().__init__(name, [x for x in args if isinstance(x, File) and not x.is_data and x.created_by is None])

        self._executable = executable
        self._args = args
        self._env = env

        for out in self.outputs:
            out._created_by = self

    @property
    def executable(self) -> str:
        return self._executable

    @property
    def args(self) -> Sequence[str | File]:
        return self._args

    @property
    def env(self) -> Optional[Mapping[str, str]]:
        return self._env

    def write_stdout_to_file(self, path: str = None) -> CustomCommand:
        self._stdout = Razel.instance().add_output_file(path if path else self.name)
        self._stdout._created_by = self
        self.outputs.append(self._stdout)
        return self

    def write_stderr_to_file(self, path: str = None) -> CustomCommand:
        self._stderr = Razel.instance().add_output_file(path if path else self.name)
        self._stderr._created_by = self
        self.outputs.append(self._stderr)
        return self


    def command_line(self) -> str:
        return " ".join(
            [
                f"./{self.executable}",
                *[
                    x
                    if not isinstance(x, File)
                    else x.file_name
                    if x.is_data
                    else os.path.join(Razel.OUT_DIR, x.file_name)
                    for x in self.args
                ],
            ]
        )

    def json(self) -> Mapping[str, Any]:
        j = {
            "name": self.name,
            "executable": self.executable,
            "args": [x.file_name if isinstance(x, File) else x for x in self.args],
            "inputs": [x.file_name for x in self.args if isinstance(x, File) and x.created_by != self],
            "outputs": [x.file_name for x in self.outputs if x != self._stdout and x != self._stderr]
        }
        if self.env:
            j["env"] = self.env
        if self._stdout:
            j["stdout"] = self._stdout.file_name
        if self._stderr:
            j["stderr"] = self._stderr.file_name
        return j


class Task(Command):
    @staticmethod
    def write_file(path: str, lines: Sequence[str]) -> File:
        file = Razel.instance().add_output_file(path)
        Razel.instance().add_task(path, "write-file", [file, *lines])
        return file

    def __init__(self, name: str, task: str, args: Sequence[str | File]) -> None:
        super().__init__(name, [x for x in args if isinstance(x, File) and not x.is_data and x.created_by is None])

        self._task = task
        self._args = args

        for output in self.outputs:
            output._created_by = self

    @property
    def task(self) -> str:
        return self._task

    @property
    def args(self) -> Sequence[str | File]:
        return self._args

    def command_line(self) -> str:
        return " ".join(
            [
                "razel",
                self.task,
                *[
                    x
                    if not isinstance(x, File)
                    else x.file_name
                    if x.is_data
                    else os.path.join(Razel.OUT_DIR, x.file_name)
                    for x in self.args
                ],
            ]
        )

    def json(self) -> Mapping[str, Any]:
        return {
            "name": self.name,
            "task": self.task,
            "args": [x.file_name if isinstance(x, File) else x for x in self.args],
        }

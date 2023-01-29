# allows annotations with types that are not yet defined
from __future__ import annotations

import abc
import json
import os
from typing import ClassVar, Optional, Any, TypeVar
from collections.abc import Mapping, Sequence


class Razel:
    _instance: ClassVar[Optional[Razel]] = None

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
        self, name: str, executable: str | File | Command, args: Sequence[str | File | Command],
        env: Optional[Mapping[str, str]] = None
    ) -> CustomCommand:
        name = self._sanitize_name(name)
        path = self._rel_path(_map_arg_to_output_path(executable))
        command = CustomCommand(name, path, _map_args_to_output_files(args), env)
        return self._add(command)

    def add_task(self, name: str, task: str, args: Sequence[str | File | Command]) -> Task:
        name = Razel._sanitize_name(name)
        command = Task(name, task, _map_args_to_output_files(args))
        return self._add(command)

    def ensure_equal(self, arg1: File | Command, arg2: File | Command) -> None:
        """Add a task to compare two files. In case of two commands, all output files will be compared."""
        if isinstance(arg1, Command) and isinstance(arg2, Command):
            assert len(arg1.outputs) == len(arg2.outputs)
            for i in range(len(arg1.outputs)):
                self.ensure_equal(arg1.outputs[i], arg2.outputs[i])
        else:
            file1 = _map_arg_to_output_file(arg1)
            file2 = _map_arg_to_output_file(arg2)
            name = f"{file1.basename}##shouldEqual##{file2.basename}"
            self._add(Task(name, "ensure-equal", [file1, file2]))

    def ensure_not_equal(self, arg1: File | Command, arg2: File | Command) -> None:
        """Add a task to compare two files. In case of two commands, all output files will be compared."""
        if isinstance(arg1, Command) and isinstance(arg2, Command):
            assert len(arg1.outputs) == len(arg2.outputs)
            for i in range(len(arg1.outputs)):
                self.ensure_equal(arg1.outputs[i], arg2.outputs[i])
        else:
            file1 = _map_arg_to_output_file(arg1)
            file2 = _map_arg_to_output_file(arg2)
            name = f"{file1.basename}##shouldNotEqual##{file2.basename}"
            self._add(Task(name, "ensure-not-equal", [file1, file2]))

    def write_razel_file(self) -> None:
        with open(os.path.join(self._workspace_dir, "razel.jsonl"), "w", encoding="utf-8") as file:
            for command in self._commands:
                json.dump(command.json(), file, separators=(',', ':'))
                file.write("\n")

    # Generic type used to ensure that _add() takes and returns the same type.
    _Command = TypeVar("_Command", bound="Command")

    def _add(self, command: _Command) -> _Command:
        for existing in self._commands:
            if existing.name == command.name:
                assert command.json() == existing.json(), \
                    f"conflicting actions: {command.name}:\nexisting: {existing.json()}\nto add: {command.json()}"
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

    def ensure_equal(self, other: File | Command) -> None:
        Razel.instance().ensure_equal(self, other)

    def ensure_not_equal(self, other: File | Command) -> None:
        Razel.instance().ensure_not_equal(self, other)


class Command(abc.ABC):
    def __init__(self, name: str, inputs: Sequence[File], outputs: Sequence[File]) -> None:
        self._name = name
        self._inputs = inputs
        self._outputs = outputs
        self._stdout: File | None = None
        self._stderr: File | None = None
        for out in self._outputs:
            out._created_by = self

    @property
    def name(self) -> str:
        return self._name

    @property
    def inputs(self) -> Sequence[File]:
        return self._inputs

    @property
    def outputs(self) -> Sequence[File]:
        return self._outputs

    @property
    def output(self) -> File:
        assert len(self._outputs) == 1, \
            f"output() requires exactly one output file, but the command has {len(self._outputs)} outputs: {self.name}"
        return self._outputs[0]

    @property
    def stdout(self) -> File | None:
        return self._stdout

    @property
    def stderr(self) -> File | None:
        return self._stderr

    def ensure_equal(self, other: File | Command) -> None:
        Razel.instance().ensure_equal(self, other)

    def ensure_not_equal(self, other: File | Command) -> None:
        Razel.instance().ensure_not_equal(self, other)

    @abc.abstractmethod
    def json(self) -> Mapping[str, Any]:
        pass


class CustomCommand(Command):
    def __init__(
        self, name: str, executable: str, args: Sequence[str | File], env: Optional[Mapping[str, str]] = None
    ) -> None:
        (inputs, outputs) = _split_args_in_inputs_and_outputs(args)
        super().__init__(name, inputs, outputs)
        self._executable = executable
        self._args = args
        self._env = env

    @property
    def executable(self) -> str:
        return self._executable

    @property
    def args(self) -> Sequence[str | File]:
        return self._args

    @property
    def env(self) -> Optional[Mapping[str, str]]:
        return self._env

    def add_input_file(self, arg: str | File) -> CustomCommand:
        """Add an input file which is not part of the command line."""
        file = arg if isinstance(arg, File) else Razel.instance().add_data_file(arg)
        self._inputs.append(file)
        return self

    def add_input_files(self, args: Sequence[str | File]) -> CustomCommand:
        """Add input files which are not part of the command line."""
        for x in args:
            self.add_input_file(x)
        return self

    def add_output_file(self, arg: str | File) -> CustomCommand:
        """Add an output file which is not part of the command line."""
        file = arg if isinstance(arg, File) else Razel.instance().add_output_file(arg)
        file._createdBy = self
        self._outputs.append(file)
        return self

    def write_stdout_to_file(self, path: str = None) -> CustomCommand:
        self._stdout = Razel.instance().add_output_file(path if path else self._name)
        self._stdout._created_by = self
        self._outputs.append(self._stdout)
        return self

    def write_stderr_to_file(self, path: str = None) -> CustomCommand:
        self._stderr = Razel.instance().add_output_file(path if path else self._name)
        self._stderr._created_by = self
        self._outputs.append(self._stderr)
        return self

    def json(self) -> Mapping[str, Any]:
        j = {
            "name": self.name,
            "executable": self.executable,
            "args": [x.file_name if isinstance(x, File) else x for x in self.args],
            "inputs": [x.file_name for x in self._inputs],
            "outputs": [x.file_name for x in self._outputs if x != self._stdout and x != self._stderr],
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
        (inputs, outputs) = _split_args_in_inputs_and_outputs(args)
        super().__init__(name, inputs, outputs)
        self._task = task
        self._args = args

    @property
    def task(self) -> str:
        return self._task

    @property
    def args(self) -> Sequence[str | File]:
        return self._args

    def json(self) -> Mapping[str, Any]:
        return {
            "name": self.name,
            "task": self.task,
            "args": [x.file_name if isinstance(x, File) else x for x in self.args],
        }


def _map_arg_to_output_path(arg: str | File | Command) -> str:
    if isinstance(arg, Command):
        return arg.output.file_name
    elif isinstance(arg, File):
        return arg.file_name
    return arg


def _map_arg_to_output_file(arg: File | Command) -> File:
    return arg.output if isinstance(arg, Command) else arg


def _map_args_to_output_files(args: Sequence[str | File | Command]) -> Sequence[str | File]:
    return [x.output if isinstance(x, Command) else x for x in args]


def _split_args_in_inputs_and_outputs(args: Sequence[str | File | Command]) -> (Sequence[File], Sequence[File]):
    inputs = [x for x in args if isinstance(x, File) and (x.is_data or x.created_by)]
    outputs = [x for x in args if isinstance(x, File) and not x.is_data and x.created_by is None]
    return inputs, outputs

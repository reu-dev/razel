# allows annotations with types that are not yet defined
from __future__ import annotations

import abc
import json
import os
import platform
import subprocess
import sys
from enum import Enum
from typing import ClassVar, Optional, Any, Tuple, TypeVar
from collections.abc import Mapping, Sequence


class Razel:
    version: ClassVar[str] = "0.3.0"
    _instance: ClassVar[Optional[Razel]] = None

    class Tag(str, Enum):
        QUIET = 'razel:quiet'
        """don't be verbose if command succeeded"""
        VERBOSE = 'razel:verbose'
        """always show verbose output"""
        CONDITION = 'razel:condition'
        """keep running and don't be verbose if command failed"""
        NO_CACHE = 'razel:no-cache',
        """always execute a command without caching"""
        NO_REMOTE_CACHE = 'razel:no-remote-cache',
        """don't use remote cache"""
        NO_SANDBOX = 'razel:no-sandbox',
        """disable sandbox and also cache - for commands with unspecified input/output files"""

    def __init__(self, workspace_dir: str) -> None:
        assert os.path.isabs(workspace_dir)
        self._workspace_dir = workspace_dir
        self.razel_file = os.path.join(self._workspace_dir, "razel.jsonl")
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
            assert len(arg1.outputs) == len(arg2.outputs), "Commands to compare have different number of output files!"
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
            assert len(arg1.outputs) == len(arg2.outputs), "Commands to compare have different number of output files!"
            for i in range(len(arg1.outputs)):
                self.ensure_equal(arg1.outputs[i], arg2.outputs[i])
        else:
            file1 = _map_arg_to_output_file(arg1)
            file2 = _map_arg_to_output_file(arg2)
            name = f"{file1.basename}##shouldNotEqual##{file2.basename}"
            self._add(Task(name, "ensure-not-equal", [file1, file2]))

    def run(self, args: list[str] = ["exec"]):
        """Run the native razel binary to execute the commands.

        Commands are written to `<workspace_dir>/razel.jsonl`. That file is processed with `razel exec`.
        If the native razel binary is not available, it will be downloaded.

        Output files are created in `<cwd>/razel-out`.
        """
        self.write_razel_file()
        razel_binary_path = find_or_download_razel_binary(Razel.version)
        cmd = [razel_binary_path]
        if len(args) > 0 and args[0] == "exec":
            razel_file_rel = os.path.relpath(self.razel_file, os.curdir)
            cmd.extend([args[0]] + ['-f', razel_file_rel] + args[1:])
        else:
            cmd.extend(args)
        print(" ".join(cmd))
        status = subprocess.run(cmd).returncode
        if status != 0:
            sys.exit(status)

    def write_razel_file(self) -> None:
        with open(self.razel_file, "w", encoding="utf-8") as file:
            for command in self._commands:
                json.dump(command.json(), file, separators=(',', ':'))
                file.write("\n")

    # Generic type used to ensure that _add() takes and returns the same type.
    _Command = TypeVar("_Command", bound="Command")

    def _add(self, command: _Command) -> _Command:
        for existing in self._commands:
            if existing.name == command.name:
                existing_json = existing.json_for_comparing_to_existing_command()
                command_son = command.json_for_comparing_to_existing_command()
                assert command_son == existing_json, \
                    f"conflicting command: {command.name}:\nexisting: {existing_json}\nto add:   {command_son}"
                assert isinstance(existing, type(command))
                return existing

        self._commands.append(command)
        return command

    @staticmethod
    def _sanitize_name(name: str) -> str:
        return name.replace(":", ".")  # target names may not contain ':'

    def _rel_path(self, file_name: str) -> str:
        if not os.path.isabs(file_name) or not file_name.startswith(self._workspace_dir):
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
    def __init__(self, name: str, inputs: list[File], outputs: list[File]) -> None:
        self._name = name
        self._inputs = inputs
        self._outputs = outputs
        self._stdout: File | None = None
        self._stderr: File | None = None
        self._deps: list[Command] = []
        self._tags: list[Razel.Tag | str] = []
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

    @property
    def tags(self) -> Sequence[Razel.Tag | str]:
        return self._tags

    def add_dependency(self, dependency: Command) -> Command:
        if dependency not in self._deps:
            self._deps.append(dependency)
        return self

    def add_dependencies(self, dependencies: Sequence[Command]) -> Command:
        for dependency in dependencies:
            self.add_dependency(dependency)
        return self

    def add_tag(self, tag: Razel.Tag | str) -> Command:
        if tag not in self._tags:
            self._tags.append(tag)
        return self

    def add_tags(self, tags: Sequence[Razel.Tag | str]) -> Command:
        for tag in tags:
            self.add_tag(tag)
        return self

    def ensure_equal(self, other: File | Command) -> None:
        Razel.instance().ensure_equal(self, other)

    def ensure_not_equal(self, other: File | Command) -> None:
        Razel.instance().ensure_not_equal(self, other)

    @abc.abstractmethod
    def json(self) -> Mapping[str, Any]:
        pass

    @abc.abstractmethod
    def json_for_comparing_to_existing_command(self) -> Mapping[str, Any]:
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
        if not any(x.file_name == file.file_name for x in self._inputs):
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
        if not any(x.file_name == file.file_name for x in self._outputs):
            file._created_by = self
            self._outputs.append(file)
        return self

    def write_stdout_to_file(self, path: Optional[str] = None) -> CustomCommand:
        new_file = Razel.instance().add_output_file(path if path else self._name + ".stdout.txt")
        if self._stdout:
            assert new_file.file_name == self._stdout.file_name
            return self
        self._stdout = new_file
        self._stdout._created_by = self
        self._outputs.append(self._stdout)
        return self

    def write_stderr_to_file(self, path: Optional[str] = None) -> CustomCommand:
        new_file = Razel.instance().add_output_file(path if path else self._name + ".stderr.txt")
        if self._stderr:
            assert new_file.file_name == self._stderr.file_name
            return self
        self._stderr = new_file
        self._stderr._created_by = self
        self._outputs.append(self._stderr)
        return self

    def json(self) -> Mapping[str, Any]:
        j: Any = {
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
        if self._deps:
            j["deps"] = [x.name for x in self._deps]
        if self._tags:
            j["tags"] = self._tags
        return j

    def json_for_comparing_to_existing_command(self) -> Mapping[str, Any]:
        j: Any = {
            "executable": self.executable,
            "args": [x.file_name if isinstance(x, File) else x for x in self.args],
            # additional input/output files might be added after constructor(), therefore not adding them here
            # additional env variables might be added after constructor(), therefore not adding them here
        }
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
        j = {
            "name": self.name,
            "task": self.task,
            "args": [x.file_name if isinstance(x, File) else x for x in self.args],
        }
        if self._deps:
            j["deps"] = [x.name for x in self._deps]
        if self._tags:
            j["tags"] = self._tags
        return j

    def json_for_comparing_to_existing_command(self) -> Mapping[str, Any]:
        return {
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


def _split_args_in_inputs_and_outputs(args: Sequence[str | File | Command]) -> Tuple[list[File], list[File]]:
    inputs = [x for x in args if isinstance(x, File) and (x.is_data or x.created_by)]
    outputs = [x for x in args if isinstance(x, File) and not x.is_data and x.created_by is None]
    return inputs, outputs


def find_or_download_razel_binary(version: str) -> str:
    ext = ".exe" if platform.system() == "Windows" or platform.system().startswith("CYGWIN") else ""
    # try to use razel binary from PATH
    path = f"razel{ext}"
    if get_razel_version(path) == version:
        return path
    # try to use razel binary from .cache
    if platform.system() == "Darwin":
        cache_dir = f"{os.environ['HOME']}/Library/Caches/de.reu-dev.razel"
    elif platform.system() == "Windows":
        localAppData = os.environ["LOCALAPPDATA"].replace('\\', '/')
        cache_dir = f"{localAppData}/reu-dev/razel"
    else:
        cache_dir = f"{os.environ['HOME']}/.cache/razel"
    path = f"{cache_dir}/razel{ext}"
    if get_razel_version(path) == version:
        return path
    # download razel binary to .cache
    download_razel_binary(version, path)
    return path


def get_razel_version(path: str) -> Optional[str]:
    try:
        p = subprocess.run([path, "--version"], capture_output=True, text=True)
        if p.returncode != 0:
            return None
        return p.stdout.strip().split(" ")[1]
    except:
        return None


def download_razel_binary(version: Optional[str], path: str):
    import gzip
    import pathlib
    import urllib.request
    download_tag = f"download/v{version}" if version else "latest/download"
    if platform.system() == "Darwin":
        build_target = "x86_64-apple-darwin"
    elif platform.system() == "Windows" or platform.system().startswith("CYGWIN"):
        build_target = "x86_64-pc-windows-msvc"
    else:
        build_target = "x86_64-unknown-linux-gnu"
    url = f"https://github.com/reu-dev/razel/releases/{download_tag}/razel-{build_target}.gz"
    print('Download razel binary from', url)
    with urllib.request.urlopen(url) as response:
        with gzip.GzipFile(fileobj=response) as uncompressed:
            file_content = uncompressed.read()
    print(f"Extract razel binary to {path}")
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(file_content)
    if platform.system() != "Windows":
        subprocess.check_call(["chmod", "+x", path])
    actual_version = get_razel_version(path)
    assert actual_version, "Failed to download razel binary. To build it from source, run: cargo install razel"
    print(f"Downloaded razel {actual_version}")

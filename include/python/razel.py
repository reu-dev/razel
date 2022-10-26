import os
import abc
from typing import List
import json


class Command(abc.ABC):
    def __init__(self, name: str, outputs) -> None:
        self._name = name
        self._outputs = outputs

    @property
    def name(self):
        return self._name

    @property
    def outputs(self):
        return self._outputs

    @property
    def output(self):
        assert len(self._outputs) == 1
        return self._outputs[0]

    @abc.abstractmethod
    def command_line(self) -> str:
        pass

    @abc.abstractmethod
    def json(self) -> any:
        pass


class CustomCommand(Command):
    def __init__(self, name: str, executable: str, args: List[str], env: any = None) -> None:
        super().__init__(name, list(filter(lambda x: isinstance(x, File) and not x.is_data and x.created_by is None, args)))

        self._executable = executable
        self._args = args
        self._env = env or {}

        for out in self.outputs:
            out._created_by = self

    @property
    def executable(self):
        return self._executable

    @property
    def args(self):
        return self._args

    @property
    def env(self):
        return self._env

    def command_line(self) -> str:
        pass

    def json(self) -> dict:
        return {
            "name": self.name,
            "executable": self.executable,
            "args": list(map(lambda x: x.file_name if isinstance(x, File) else x, self.args)),
            "inputs": list(
                map(lambda x: x.file_name, filter(lambda x: isinstance(x, File) and x.created_by != self, self.args))
            ),
            "outputs": list(map(lambda x: x.file_name, self.outputs)),
            "env": self.env,
        }


class File:
    def __init__(self, file_name: str, is_data: bool, created_by: Command or None) -> None:
        self._file_name = file_name
        self._is_data = is_data
        self._created_by = created_by

    @property
    def file_name(self):
        return self._file_name

    @property
    def is_data(self):
        return self._is_data

    @property
    def created_by(self):
        return self._created_by

    @property
    def basename(self):
        return os.path.basename(self._file_name)

    def ensure_equal(self, other):
        Razel.instance().ensureEqual(self, other)

    def ensure_not_equal(self, other):
        Razel.instance().ensureNotEqual(self, other)


class Razel:
    _instance = None

    @staticmethod
    def init(workspace_dir: str):
        assert Razel._instance is None

        Razel._instance = Razel(workspace_dir)
        return Razel._instance

    @staticmethod
    def instance():
        return Razel._instance

    def __init__(self, workspace_dir: str) -> None:
        self._workspace_dir = workspace_dir
        self._commands = []

    def add_data_file(self, path: str) -> File:
        return File(self._rel_path(path), True, None)

    def add_output_file(self, path: str) -> File:
        return File(self._rel_path(path), False, None)

    def add_command(self, name: str, executable: str, args: List[str or File], env: any = None) -> CustomCommand:
        name = self._sanitize_name(name)
        command = CustomCommand(name, self._rel_path(executable), args, env)
        return self._add(command)

    def write_razel_file(self):
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
    def _sanitize_name(name: str):
        return name.replace(":", ".")  # target names may not contain ':'

    def _rel_path(self, file_name: str) -> str:
        if not os.path.isabs(file_name):
            return file_name

        return os.path.relpath(file_name, self._workspace_dir)

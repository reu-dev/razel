import {assertEquals} from 'https://deno.land/std@0.135.0/testing/asserts.ts';
import * as path from 'https://deno.land/std@0.135.0/path/mod.ts';

export class Razel {
    private static _instance: Razel;
    static readonly outDir = 'razel-out';
    private commands: Command[] = [];

    private constructor(public readonly workspaceDir: string) {
    }

    static init(workspaceDir: string): Razel {
        assertEquals(Razel._instance, undefined);
        Razel._instance = new Razel(workspaceDir);
        return Razel._instance;
    }

    static instance(): Razel {
        return Razel._instance;
    }

    addDataFile(path: string): File {
        return new File(this.relPath(path), true, null);
    }

    addOutputFile(path: string): File {
        return new File(this.relPath(path), false, null);
    }

    addCommand(name: string, executable: string, args: (string | File)[], env?: any): CustomCommand {
        name = this.sanitizeName(name);
        const command = new CustomCommand(name, this.relPath(executable), args, env);
        return this.add(command) as CustomCommand;
    }

    addTask(name: string, task: string, args: (string | File)[]): Task {
        name = this.sanitizeName(name);
        const command = new Task(name, task, args);
        return this.add(command) as Task;
    }

    ensureEqual(file1: File, file2: File) {
        const name = `${file1.basename}##shouldEqual##${file2.basename}`;
        this.add(new Task(name, 'ensure-equal', [file1, file2]));
    }

    ensureNotEqual(file1: File, file2: File) {
        const name = `${file1.basename}##shouldNotEqual##${file2.basename}`;
        this.add(new Task(name, 'ensure-not-equal', [file1, file2]));
    }

    writeRazelFile() {
        const json = this.commands.map(x => JSON.stringify(x.json()));
        Deno.writeTextFileSync(`${this.workspaceDir}/razel.jsonl`, json.join('\n'));
    }

    private add(command: Command): Command {
        const existing = this.commands.find(x => x.name === command.name);
        if (existing) {
            assertEquals(command.commandLine(), existing.commandLine(), `conflicting actions: ${command.name}:\n${existing.commandLine()}\n${command.commandLine()}`);
            return existing;
        }
        this.commands.push(command);
        return command;
    }

    private sanitizeName(name: string): string {
        return name.replaceAll(':', '.'); // target names may not contain ':'
    }

    private relPath(fileName: string): string {
        if (!path.isAbsolute(fileName)) {
            return fileName;
        }
        return path.relative(this.workspaceDir, fileName);
    }
}

export class File {
    constructor(public readonly fileName: string, public readonly isData: boolean, public createdBy: Command | null) {
    }

    get basename(): string {
        return path.basename(this.fileName);
    }

    ensureEqual(other: File) {
        Razel.instance().ensureEqual(this, other);
    }

    ensureNotEqual(other: File) {
        Razel.instance().ensureNotEqual(this, other);
    }
}

abstract class Command {
    protected constructor(public readonly name: string, public readonly outputs: File[]) {
    }

    get output(): File {
        assertEquals(this.outputs.length, 1);
        return this.outputs[0];
    }

    abstract commandLine(): string;

    abstract json(): any;
}

class CustomCommand extends Command {
    constructor(name: string, public readonly executable: string, public readonly args: (string | File)[],
                public readonly env?: any) {
        super(name, args.filter(x => (x instanceof File) && !(x as File).isData && !(x as File).createdBy) as File[]);
        this.outputs.forEach(x => x.createdBy = this);
    }

    commandLine(): string {
        return [
            `./${this.executable}`,
            ...this.args.map(x => x instanceof File ? (x.isData ? x.fileName : path.join(Razel.outDir, x.fileName)) : x)
        ].join(' ');
    }

    json(): any {
        return {
            name: this.name,
            executable: this.executable,
            args: this.args.map(x => x instanceof File ? x.fileName : x),
            inputs: this.args.filter(x => x instanceof File && x.createdBy !== this).map(x => (x as File).fileName),
            outputs: this.outputs.map(x => x.fileName),
        };
    }
}

class Task extends Command {
    constructor(name: string, public readonly task: string, public readonly args: (string | File)[]) {
        super(name, args.filter(x => (x instanceof File) && !(x as File).isData && !(x as File).createdBy) as File[]);
        this.outputs.forEach(x => x.createdBy = this);
    }

    commandLine(): string {
        return [
            'razel',
            this.task,
            ...this.args.map(x => x instanceof File ? (x.isData ? x.fileName : path.join(Razel.outDir, x.fileName)) : x)
        ].join(' ');
    }

    json(): any {
        return {
            name: this.name,
            task: this.task,
            args: this.args.map(x => x instanceof File ? x.fileName : x),
        };
    }
}

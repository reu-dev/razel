import {assertEquals} from 'https://deno.land/std@0.135.0/testing/asserts.ts';
import * as path from 'https://deno.land/std@0.135.0/path/mod.ts';

export class Razel {
    private static _instance: Razel;
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

    addCommand(name: string, executable: (string | File | Command), args: (string | File | Command)[], env?: any): CustomCommand {
        name = this.sanitizeName(name);
        const path = this.relPath(mapArgToOutputPath(executable));
        const command = new CustomCommand(name, path, mapArgsToOutputFiles(args), env);
        return this.add(command) as CustomCommand;
    }

    addTask(name: string, task: string, args: (string | File | Command)[]): Task {
        name = this.sanitizeName(name);
        const command = new Task(name, task, mapArgsToOutputFiles(args));
        return this.add(command) as Task;
    }

    // Add a task to compare two files. In case of two commands, all output files will be compared.
    ensureEqual(arg1: File | Command, arg2: File | Command): void {
        if (arg1 instanceof Command && arg2 instanceof Command) {
            assertEquals(arg1.outputs.length, arg2.outputs.length);
            for (let i = 0; i != arg1.outputs.length; ++i) {
                this.ensureEqual(arg1.outputs[i], arg2.outputs[i]);
            }
        } else {
            const file1 = mapArgToOutputFile(arg1);
            const file2 = mapArgToOutputFile(arg2);
            const name = `${file1.basename}##shouldEqual##${file2.basename}`;
            this.add(new Task(name, 'ensure-equal', [file1, file2]));
        }
    }

    // Add a task to compare two files. In case of two commands, all output files will be compared.
    ensureNotEqual(arg1: File | Command, arg2: File | Command): void {
        if (arg1 instanceof Command && arg2 instanceof Command) {
            assertEquals(arg1.outputs.length, arg2.outputs.length);
            for (let i = 0; i != arg1.outputs.length; ++i) {
                this.ensureEqual(arg1.outputs[i], arg2.outputs[i]);
            }
        } else {
            const file1 = mapArgToOutputFile(arg1);
            const file2 = mapArgToOutputFile(arg2);
            const name = `${file1.basename}##shouldNotEqual##${file2.basename}`;
            this.add(new Task(name, 'ensure-not-equal', [file1, file2]));
        }
    }

    writeRazelFile() {
        const json = this.commands.map(x => JSON.stringify(x.json()));
        Deno.writeTextFileSync(path.join(this.workspaceDir, 'razel.jsonl'), json.join('\n') + '\n');
    }

    private add(command: Command): Command {
        const existing = this.commands.find(x => x.name === command.name);
        if (existing) {
            assertEquals(command.json(), existing.json(),
                `conflicting actions: ${command.name}:\nexisting: ${existing.json()}\nto add: ${command.json()}`);
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

    ensureEqual(other: File | Command): void {
        Razel.instance().ensureEqual(this, other);
    }

    ensureNotEqual(other: File | Command): void {
        Razel.instance().ensureNotEqual(this, other);
    }
}

export abstract class Command {
    public stdout: File | undefined = undefined;
    public stderr: File | undefined = undefined;

    protected constructor(public readonly name: string, public readonly inputs: File[], public readonly outputs: File[]) {
        this.outputs.forEach(x => x.createdBy = this);
    }

    get output(): File {
        assertEquals(this.outputs.length, 1,
            `output() requires exactly one output file, but the command has ${this.outputs.length} outputs: ${this.name}`);
        return this.outputs[0];
    }

    ensureEqual(other: File | Command): void {
        Razel.instance().ensureEqual(this, other);
    }

    ensureNotEqual(other: File | Command): void {
        Razel.instance().ensureNotEqual(this, other);
    }

    abstract json(): any;
}

export class CustomCommand extends Command {
    constructor(name: string, public readonly executable: string, public readonly args: (string | File)[],
                public readonly env?: any) {
        const [inputs, outputs] = splitArgsInInputsAndOutputs(args);
        super(name, inputs, outputs);
    }

    // Add an input file which is not part of the command line.
    addInputFile(arg: string | File): CustomCommand {
        const file = arg instanceof File ? arg : Razel.instance().addDataFile(arg);
        this.inputs.push(file);
        return this;
    }

    // Add an output file which is not part of the command line.
    addOutputFile(arg: string | File): CustomCommand {
        const file = arg instanceof File ? arg : Razel.instance().addOutputFile(arg);
        file.createdBy = this;
        this.outputs.push(file);
        return this;
    }

    writeStdoutToFile(path?: string): CustomCommand {
        this.stdout = Razel.instance().addOutputFile(path ? path : this.name);
        this.stdout.createdBy = this;
        this.outputs.push(this.stdout);
        return this;
    }

    writeStderrToFile(path?: string): CustomCommand {
        this.stderr = Razel.instance().addOutputFile(path ? path : this.name);
        this.stderr.createdBy = this;
        this.outputs.push(this.stderr);
        return this;
    }

    json(): any {
        return {
            name: this.name,
            executable: this.executable,
            args: this.args.map(x => x instanceof File ? x.fileName : x),
            inputs: this.inputs.map(x => x.fileName),
            outputs: this.outputs.map(x => x.fileName),
            env: this.env,
            stdout: this.stdout?.fileName,
            stderr: this.stderr?.fileName,
        };
    }
}

export class Task extends Command {
    static writeFile(path: string, lines: string[]): File {
        const file = Razel.instance().addOutputFile(path);
        Razel.instance().addTask(path, 'write-file', [file, ...lines]);
        return file;
    }

    constructor(name: string, public readonly task: string, public readonly args: (string | File)[]) {
        const [inputs, outputs] = splitArgsInInputsAndOutputs(args);
        super(name, inputs, outputs);
    }

    json(): any {
        return {
            name: this.name,
            task: this.task,
            args: this.args.map(x => x instanceof File ? x.fileName : x),
        };
    }
}

function mapArgToOutputPath(arg: string | File | Command): string {
    if (arg instanceof Command) {
        return arg.output.fileName;
    } else if (arg instanceof File) {
        return arg.fileName;
    }
    return arg;
}

function mapArgToOutputFile(arg: File | Command): File {
    return arg instanceof Command ? arg.output : arg;
}

function mapArgsToOutputFiles(args: (string | File | Command)[]): (string | File)[] {
    return args.map(x => x instanceof Command ? x.output : x);
}

function splitArgsInInputsAndOutputs(args: (string | File)[]): [File[], File[]] {
    const inputs = args.filter(x => (x instanceof File) && ((x as File).isData || (x as File).createdBy)) as File[];
    const outputs = args.filter(x => (x instanceof File) && !(x as File).isData && !(x as File).createdBy) as File[];
    return [inputs, outputs];
}

import {assert, assertEquals} from 'https://deno.land/std@0.135.0/testing/asserts.ts';
import * as path from 'https://deno.land/std@0.135.0/path/mod.ts';

export class Razel {
    static version = "0.1.8";
    private static _instance: Razel;
    razelFile: string;
    private commands: Command[] = [];

    private constructor(public readonly workspaceDir: string) {
        this.razelFile = path.join(this.workspaceDir, 'razel.jsonl');
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

    /** Run the native razel binary to execute the commands.
     *
     * Commands are written to `<workspaceDir>/razel.jsonl`. That file is processed with `razel exec`.
     * If the native razel binary is not available, it will be downloaded.
     *
     * Output files are created in `<cwd>/razel-out`.
     */
    async run(args: string[] = ["exec"]) {
        await this.writeRazelFile();
        const razelBinaryPath = await findOrDownloadRazelBinary(Razel.version);
        const cmd = [razelBinaryPath];
        if (args.length > 0 && args[0] === 'exec') {
            const razelFileRel = path.relative(Deno.cwd(), this.razelFile);
            cmd.push(args[0], '-f', razelFileRel, ...args.slice(1));
        } else {
            cmd.push(...args);
        }
        console.log(cmd.join(" "));
        const status = await Deno.run({cmd}).status();
        if (!status.success) {
            Deno.exit(status.code);
        }
    }

    async writeRazelFile() {
        const json = this.commands.map(x => JSON.stringify(x.json()));
        await Deno.writeTextFile(this.razelFile, json.join('\n') + '\n');
    }

    private add(command: Command): Command {
        const existing = this.commands.find(x => x.name === command.name);
        if (existing) {
            const existingJson = existing.jsonForComparingToExistingCommand();
            const commandJson = command.jsonForComparingToExistingCommand();
            assertEquals(commandJson, existingJson,
                `conflicting command: ${command.name}:\n\
                existing: ${JSON.stringify(existingJson)}\n\
                to add:   ${JSON.stringify(commandJson)}`);
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

export namespace Razel {
    export enum Tag {
        Quiet = 'razel:quiet',
        Verbose = 'razel:verbose',
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
    public readonly tags: (Razel.Tag | string)[] = [];

    protected constructor(public readonly name: string, public readonly inputs: File[], public readonly outputs: File[]) {
        this.outputs.forEach(x => x.createdBy = this);
    }

    get output(): File {
        assertEquals(this.outputs.length, 1,
            `output() requires exactly one output file, but the command has ${this.outputs.length} outputs: ${this.name}`);
        return this.outputs[0];
    }

    addTag(tag: Razel.Tag | string): Command {
        this.tags.push(tag);
        return this;
    }

    addTags(tags: (Razel.Tag | string)[]): Command {
        this.tags.push(...tags);
        return this;
    }

    ensureEqual(other: File | Command): void {
        Razel.instance().ensureEqual(this, other);
    }

    ensureNotEqual(other: File | Command): void {
        Razel.instance().ensureNotEqual(this, other);
    }

    abstract json(): any;

    abstract jsonForComparingToExistingCommand(): any;
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

    // Add input files which are not part of the command line.
    addInputFiles(args: (string | File)[]): CustomCommand {
        args.forEach(x => this.addInputFile(x));
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
        const newFile = Razel.instance().addOutputFile(path ? path : this.name + ".stdout.txt");
        if (this.stdout) {
            assertEquals(newFile.fileName, this.stdout.fileName);
            return this;
        }
        this.stdout = newFile;
        this.stdout.createdBy = this;
        this.outputs.push(this.stdout);
        return this;
    }

    writeStderrToFile(path?: string): CustomCommand {
        const newFile = Razel.instance().addOutputFile(path ? path : this.name + ".stderr.txt");
        if (this.stderr) {
            assertEquals(newFile.fileName, this.stderr.fileName);
            return this;
        }
        this.stderr = newFile;
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
            outputs: this.outputs.filter(x => x !== this.stdout && x !== this.stderr).map(x => x.fileName),
            env: this.env,
            stdout: this.stdout?.fileName,
            stderr: this.stderr?.fileName,
            tags: this.tags.length != 0 ? this.tags : undefined,
        };
    }

    jsonForComparingToExistingCommand(): any {
        return {
            executable: this.executable,
            args: this.args.map(x => x instanceof File ? x.fileName : x),
            inputs: this.inputs.map(x => x.fileName),
            outputs: this.outputs.filter(x => x !== this.stdout && x !== this.stderr).map(x => x.fileName),
            env: this.env,
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
            tags: this.tags.length != 0 ? this.tags : undefined,
        };
    }

    jsonForComparingToExistingCommand(): any {
        return {
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

export async function findOrDownloadRazelBinary(version: string): Promise<string> {
    const ext = Deno.build.os === "windows" ? ".exe" : "";
    // try to use razel binary from PATH
    let razelBinaryPath = `razel${ext}`;
    if (await getRazelVersion(razelBinaryPath) === version) {
        return razelBinaryPath;
    }
    // try to use razel binary from .cache
    let cacheDir;
    if (Deno.build.os === "darwin") {
        cacheDir = `${Deno.env.get("HOME")}/Library/Caches/de.reu-dev.razel`;
    } else if (Deno.build.os === "windows") {
        const localAppData = Deno.env.get("LOCALAPPDATA");
        assert(localAppData);
        cacheDir = `${localAppData.replaceAll("\\", "/")}/reu-dev/razel`;
    } else {
        cacheDir = `${Deno.env.get("HOME")}/.cache/razel`;
    }
    razelBinaryPath = `${cacheDir}/razel${ext}`;
    if (await getRazelVersion(razelBinaryPath) === version) {
        return razelBinaryPath;
    }
    // download razel binary to .cache
    await downloadRazelBinary(version, razelBinaryPath);
    return razelBinaryPath;
}

async function getRazelVersion(razelBinaryPath: string): Promise<string | null> {
    try {
        const p = Deno.run({cmd: [razelBinaryPath, "--version"], stdout: "piped"});
        const [status, rawOutput] = await Promise.all([p.status(), p.output()]);
        if (!status.success) {
            return null;
        }
        const stdout = new TextDecoder().decode(rawOutput);
        return stdout.trim().split(" ")[1];
    } catch {
        return null;
    }
}

async function downloadRazelBinary(version: string | null, razelBinaryPath: string) {
    const downloadTag = version ? `download/v${version}` : "latest/download";
    let buildTarget;
    if (Deno.build.os === "darwin") {
        buildTarget = "x86_64-apple-darwin";
    } else if (Deno.build.os === "windows") {
        buildTarget = "x86_64-pc-windows-msvc";
    } else {
        buildTarget = "x86_64-unknown-linux-gnu";
    }
    const url = `https://github.com/reu-dev/razel/releases/${downloadTag}/razel-${buildTarget}.gz`;
    console.log('Download razel binary from', url);
    const response = await fetch(url);
    if (!response.body) {
        throw response.statusText;
    }
    console.log(`Extract razel binary to ${razelBinaryPath}`);
    await Deno.mkdir(path.dirname(razelBinaryPath), { recursive: true });
    const dest = await Deno.open(razelBinaryPath, {create: true, write: true});
    await response.body
        .pipeThrough(new DecompressionStream("gzip"))
        .pipeTo(dest.writable);
    if (Deno.build.os !== "windows") {
        const mode = (await Deno.stat(razelBinaryPath)).mode || 0;
        await Deno.chmod(razelBinaryPath, mode | 0o700);
    }
    const actualVersion = await getRazelVersion(razelBinaryPath);
    assert(actualVersion, "Failed to download razel binary. To build it from source, run: cargo install razel");
    console.log(`Downloaded razel ${actualVersion}`);
}

import { assert, assertEquals } from "https://deno.land/std@0.135.0/testing/asserts.ts";
import * as path from "https://deno.land/std@0.135.0/path/mod.ts";

export class Razel {
    static version = "0.5.6";
    private static _instance: Razel;
    razelFile: string;
    private commands: Command[] = [];

    private constructor(public readonly workspaceDir: string) {
        assert(path.isAbsolute(workspaceDir));
        this.razelFile = path.join(this.workspaceDir, "razel.jsonl");
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

    addCommand(
        name: string,
        executable: string | File | Command,
        args: (string | File | Command)[],
        env?: any,
    ): CustomCommand {
        name = this.sanitizeName(name);
        const executablePath = mapArgToOutputPath(executable);
        const command = new CustomCommand(name, executablePath, mapArgsToOutputFiles(args), env || {});
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
            assertEquals(
                arg1.outputs.length,
                arg2.outputs.length,
                "Commands to compare have different number of output files!",
            );
            for (let i = 0; i != arg1.outputs.length; ++i) {
                this.ensureEqual(arg1.outputs[i], arg2.outputs[i]);
            }
        } else {
            const file1 = mapArgToOutputFile(arg1);
            const file2 = mapArgToOutputFile(arg2);
            const name = `${file1.fileName}##shouldEqual##${file2.fileName}`;
            this.add(new Task(name, "ensure-equal", [file1, file2]));
        }
    }

    // Add a task to compare two files. In case of two commands, all output files will be compared.
    ensureNotEqual(arg1: File | Command, arg2: File | Command): void {
        if (arg1 instanceof Command && arg2 instanceof Command) {
            assertEquals(
                arg1.outputs.length,
                arg2.outputs.length,
                "Commands to compare have different number of output files!",
            );
            for (let i = 0; i != arg1.outputs.length; ++i) {
                this.ensureEqual(arg1.outputs[i], arg2.outputs[i]);
            }
        } else {
            const file1 = mapArgToOutputFile(arg1);
            const file2 = mapArgToOutputFile(arg2);
            const name = `${file1.fileName}##shouldNotEqual##${file2.fileName}`;
            this.add(new Task(name, "ensure-not-equal", [file1, file2]));
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
        if (args.length > 0 && args[0] === "exec") {
            const razelFileRel = path.relative(Deno.cwd(), this.razelFile);
            args = [args[0], "-f", razelFileRel, ...args.slice(1)];
        }
        const status = await runRazelBinary(args);
        if (!status.success) {
            Deno.exit(status.code);
        }
    }

    async tryRun(args: string[] = ["exec"]): Promise<boolean> {
        await this.writeRazelFile();
        if (args.length > 0 && args[0] === "exec") {
            const razelFileRel = path.relative(Deno.cwd(), this.razelFile);
            args = [args[0], "-f", razelFileRel, ...args.slice(1)];
        }
        const status = await runRazelBinary(args);
        return status.success;
    }

    async writeRazelFile() {
        const json = this.commands.map((x) => JSON.stringify(x.json()));
        await Deno.writeTextFile(this.razelFile, json.join("\n") + "\n");
    }

    readLogFile(): LogFileItem[] {
        const path = "razel-out/razel-metadata/log.json";
        const text = Deno.readTextFileSync(path);
        return JSON.parse(text);
    }

    clear() {
        this.commands = [];
    }

    private add(command: Command): Command {
        const existing = this.commands.find((x) => x.name === command.name);
        if (existing) {
            const existingJson = existing.jsonForComparingToExistingCommand();
            const commandJson = command.jsonForComparingToExistingCommand();
            assertEquals(
                commandJson,
                existingJson,
                `conflicting command: ${command.name}:\n\
                existing: ${JSON.stringify(existingJson)}\n\
                to add:   ${JSON.stringify(commandJson)}`,
            );
            return existing;
        }
        this.commands.push(command);
        return command;
    }

    private sanitizeName(name: string): string {
        return name.replaceAll(":", "."); // target names may not contain ':'
    }

    private relPath(fileName: string): string {
        if (!path.isAbsolute(fileName) || !fileName.startsWith(this.workspaceDir)) {
            return fileName;
        }
        return path.relative(this.workspaceDir, fileName);
    }
}

export namespace Razel {
    export enum Tag {
        // don't be verbose if command succeeded
        Quiet = "razel:quiet",
        // always show verbose output
        Verbose = "razel:verbose",
        // keep running and don't be verbose if command failed
        Condition = "razel:condition",
        // always execute a command without caching
        NoCache = "razel:no-cache",
        // don't use remote cache
        NoRemoteCache = "razel:no-remote-cache",
        // disable sandbox and also cache - for commands with unspecified input/output files
        NoSandbox = "razel:no-sandbox",
    }
}

export class File {
    constructor(
        public readonly fileName: string,
        public readonly isData: boolean,
        public createdBy: Command | null,
    ) {
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
    public readonly deps: Command[] = [];
    public readonly tags: (Razel.Tag | string)[] = [];

    protected constructor(
        public readonly name: string,
        public readonly inputs: File[],
        public readonly outputs: File[],
    ) {
        this.outputs.forEach((x) => x.createdBy = this);
    }

    get output(): File {
        assertEquals(
            this.outputs.length,
            1,
            `output() requires exactly one output file, but the command has ${this.outputs.length} outputs: ${this.name}`,
        );
        return this.outputs[0];
    }

    addDependency(dependency: Command): this {
        if (!this.deps.includes(dependency)) {
            this.deps.push(dependency);
        }
        return this;
    }

    addDependencies(dependencies: Command[]): this {
        dependencies.forEach((x) => this.addDependency(x));
        return this;
    }

    addTag(tag: Razel.Tag | string): this {
        if (!this.tags.includes(tag)) {
            this.tags.push(tag);
        }
        return this;
    }

    addTags(tags: (Razel.Tag | string)[]): this {
        tags.forEach((x) => this.addTag(x));
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
    constructor(
        name: string,
        public readonly executable: string,
        public readonly args: (string | File)[],
        public readonly env: any,
    ) {
        const [inputs, outputs] = splitArgsInInputsAndOutputs(args);
        super(name, inputs, outputs);
    }

    addEnv(key: string, value: string): CustomCommand {
        this.env[key] = value;
        return this;
    }

    // Add an input file which is not part of the command line.
    addInputFile(arg: string | File): CustomCommand {
        const file = arg instanceof File ? arg : Razel.instance().addDataFile(arg);
        if (!this.inputs.some((x) => x.fileName === file.fileName)) {
            this.inputs.push(file);
        }
        return this;
    }

    // Add input files which are not part of the command line.
    addInputFiles(args: (string | File)[]): CustomCommand {
        args.forEach((x) => this.addInputFile(x));
        return this;
    }

    // Add an output file which is not part of the command line.
    addOutputFile(arg: string | File): CustomCommand {
        const file = arg instanceof File ? arg : Razel.instance().addOutputFile(arg);
        if (!this.outputs.some((x) => x.fileName === file.fileName)) {
            file.createdBy = this;
            this.outputs.push(file);
        }
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
            args: this.args.map((x) => x instanceof File ? x.fileName : x),
            inputs: this.inputs.map((x) => x.fileName),
            outputs: this.outputs.filter((x) => x !== this.stdout && x !== this.stderr).map((x) =>
                x.fileName
            ),
            env: Object.keys(this.env).length !== 0 ? this.env : undefined,
            stdout: this.stdout?.fileName,
            stderr: this.stderr?.fileName,
            deps: this.deps.length != 0 ? this.deps.map((x) => x.name) : undefined,
            tags: this.tags.length != 0 ? this.tags : undefined,
        };
    }

    jsonForComparingToExistingCommand(): any {
        return {
            executable: this.executable,
            args: this.args.map((x) => x instanceof File ? x.fileName : x),
            // additional input/output files might be added after constructor(), therefore not adding them here
            // additional env variables might be added after constructor(), therefore not adding them here
        };
    }
}

export class Task extends Command {
    static writeFile(path: string, lines: string[]): File {
        const file = Razel.instance().addOutputFile(path);
        Razel.instance().addTask(path, "write-file", [file, ...lines]);
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
            args: this.args.map((x) => x instanceof File ? x.fileName : x),
            deps: this.deps.length != 0 ? this.deps.map((x) => x.name) : undefined,
            tags: this.tags.length != 0 ? this.tags : undefined,
        };
    }

    jsonForComparingToExistingCommand(): any {
        return {
            task: this.task,
            args: this.args.map((x) => x instanceof File ? x.fileName : x),
        };
    }
}

export interface LogFileItem {
    name: string;
    tags?: string[];
    status: string;
    error?: string;
    cache?: string;
    // original execution duration of the command/task - ignoring cache
    exec?: number;
    // actual duration of processing the command/task - including caching and overheads
    total?: number;
    // total size of all output files and stdout/stderr [bytes]
    output_size?: number;
    measurements?: { [key: string]: string | number };
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
    return args.map((x) => x instanceof Command ? x.output : x);
}

function splitArgsInInputsAndOutputs(args: (string | File)[]): [File[], File[]] {
    const inputs = args.filter((x) =>
        (x instanceof File) && ((x as File).isData || (x as File).createdBy)
    ) as File[];
    const outputs = args.filter((x) =>
        (x instanceof File) && !(x as File).isData && !(x as File).createdBy
    ) as File[];
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
        const command = new Deno.Command(razelBinaryPath, {
            args: ["--version"],
            stdout: "piped",
            stderr: "piped",
        });
        const { code, stdout } = await command.output();
        if (code !== 0) {
            return null;
        }
        const stdoutDecoded = new TextDecoder().decode(stdout);
        return stdoutDecoded.trim().split(" ")[1];
    } catch {
        return null;
    }
}

async function downloadRazelBinary(version: string | null, razelBinaryPath: string) {
    const downloadTag = version ? `download/v${version}` : "latest/download";
    let buildTarget;
    if (Deno.build.os === "darwin" && Deno.build.arch === "aarch64") {
        buildTarget = "aarch64-apple-darwin";
    } else if (Deno.build.os === "darwin" && Deno.build.arch === "x86_64") {
        buildTarget = "x86_64-apple-darwin";
    } else if (Deno.build.os === "windows") {
        buildTarget = "x86_64-pc-windows-msvc";
    } else if (Deno.build.arch === "aarch64") {
        buildTarget = "aarch64-unknown-linux-gnu";
    } else {
        buildTarget = "x86_64-unknown-linux-gnu";
    }
    const url = `https://github.com/reu-dev/razel/releases/${downloadTag}/razel-${buildTarget}.gz`;
    console.log("Download razel binary from", url);
    const response = await fetch(url);
    if (!response.body) {
        throw response.statusText;
    }
    console.log(`Extract razel binary to ${razelBinaryPath}`);
    await Deno.mkdir(path.dirname(razelBinaryPath), { recursive: true });
    const dest = await Deno.open(razelBinaryPath, { create: true, write: true });
    await response.body
        .pipeThrough(new DecompressionStream("gzip"))
        .pipeTo(dest.writable);
    if (Deno.build.os !== "windows") {
        const mode = (await Deno.stat(razelBinaryPath)).mode || 0;
        await Deno.chmod(razelBinaryPath, mode | 0o700);
    }
    const actualVersion = await getRazelVersion(razelBinaryPath);
    assert(
        actualVersion,
        "Failed to download razel binary. To build it from source, run: cargo install razel",
    );
    console.log(`Downloaded razel ${actualVersion}`);
}

// Run the native razel binary. If not available, it will be downloaded. Returns the exit code.
export async function runRazelBinary(args: string[]): Promise<Deno.CommandStatus> {
    const razelBinaryPath = await findOrDownloadRazelBinary(Razel.version);
    const cmd = [razelBinaryPath, ...args];
    console.log(cmd.join(" "));
    const process = new Deno.Command(cmd[0], {
        args: cmd.slice(1),
        stdout: "inherit",
        stderr: "inherit",
    }).spawn();
    return await process.status;
}

if (import.meta.main) {
    if (Deno.args.length === 0) {
        await findOrDownloadRazelBinary(Razel.version);
        Deno.exit(0);
    }
    const status = await runRazelBinary(Deno.args);
    Deno.exit(status.code);
}

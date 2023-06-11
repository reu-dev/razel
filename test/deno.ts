import * as path from 'https://deno.land/std@0.135.0/path/mod.ts';
import {Razel} from 'https://deno.land/x/razel@v0.1.9/razel.ts';

const workspaceDir = path.resolve(path.dirname(path.fromFileUrl(import.meta.url)));
const razel = Razel.init(workspaceDir);

// data/a.csv and data/f.csv are two input files
const a = razel.addDataFile('data/a.csv');
const f = razel.addDataFile('data/f.csv');
// add task to verify that they differ
a.ensureNotEqual(f);
// add tasks to create additional files and compare the final output to a data file
const b = razel.addTask('b.csv', 'write-file', [razel.addOutputFile('b.csv'), 'a,b,xyz', '3,4,56', '7,8,9']);
const c = razel.addTask('c.csv', 'csv-concat', [a, b, razel.addOutputFile('c.csv')]);
razel.addTask('filtered.csv', 'csv-filter', ['-i', c, '-o', razel.addOutputFile('filtered.csv'), '-c', 'a', 'xyz'])
    .ensureEqual(f);
// add command to copy a file using the OS executable
const d = razel.addCommand('d.csv', 'cp', [a, razel.addOutputFile('d.csv')])
    .addTag('copy');
d.ensureEqual(a);

if (false) {  // requires clang
    // compile an executable from a c file
    const say_hi = razel.addCommand('say_hi', 'clang',
        ['-o', razel.addOutputFile('say_hi'), razel.addDataFile('data/say_hi.c')]);
    // run it, redirect stdout to a file and compare it with the output of another command
    razel.addCommand('say_hi_using_c', say_hi, ['Razel'])
        .writeStdoutToFile()
        .addTag(Razel.Tag.Verbose)
        .ensureEqual(razel.addCommand('say_hi_using_echo', 'echo', ['Hi Razel!']).writeStdoutToFile());
}

// execute the commands using the native razel binary (will be downloaded)
await razel.run(['exec', ...Deno.args.slice(1)]);

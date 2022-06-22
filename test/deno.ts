import * as path from 'https://deno.land/std@0.135.0/path/mod.ts';
import {Razel} from "../include/deno/razel.ts";

const workspaceDir = path.dirname(new URL(import.meta.url).pathname);
const razel = Razel.init(workspaceDir);

// data/a.csv and data/f.csv are two input files
const a = razel.addDataFile(path.join('data', 'a.csv'));
const f = razel.addDataFile(path.join('data', 'f.csv'));
// add task to verify that they differ
a.ensureNotEqual(f);
// add tasks to create additional files and compare the final output to a data file
const b = razel.addTask('b.csv', 'write-file', [razel.addOutputFile('b.csv'), 'a,b,xyz', '3,4,56', '7,8,9']).output;
const c = razel.addTask('c.csv', 'csv-concat', [a, b, razel.addOutputFile('c.csv')]).output;
razel.addTask('filtered.csv', 'csv-filter', ['-i', c, '-o', razel.addOutputFile('filtered.csv'), '-c', 'a', 'xyz'])
    .output
    .ensureEqual(f);
// add command: use cmake to copy a file
razel.addCommand('d.csv', 'cmake', ['-E', 'copy', a, razel.addOutputFile('d.csv')])
    .output
    .ensureEqual(a);

razel.writeRazelFile();

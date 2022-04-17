import * as path from 'https://deno.land/std@0.135.0/path/mod.ts';
import {Razel} from "../include/deno/razel.ts";

const workspaceDir = path.dirname(new URL(import.meta.url).pathname);
const razel = Razel.init(workspaceDir);

const a = razel.addDataFile(path.join('data', 'a.csv'));
const b = razel.addTask('b.csv', 'write', [razel.addOutputFile('b.csv'), 'a,b,xyz', '3,4,56 7,8,9']).output;
const c = razel.addTask('c.csv', 'csv-concat', [a, b, razel.addOutputFile('c.csv')]).output;
razel.addTask('filtered.csv', 'csv-filter', ['-i', c, '-o', razel.addOutputFile('filtered.csv'), '-c', 'a', 'xyz'])
    .output
    .ensureEqual(razel.addDataFile(path.join('data', 'f.csv')));

razel.writeRazelFile();

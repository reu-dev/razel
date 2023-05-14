#!/usr/bin/env python3

import os.path as path
import sys
from razel import Razel

workspace_dir = path.dirname(path.abspath(__file__))
razel = Razel.init(workspace_dir)

# data/a.csv and data/f.csv are two input files
a = razel.add_data_file('data/a.csv')
f = razel.add_data_file('data/f.csv')
# add task to verify that they differ
a.ensure_not_equal(f)
# add tasks to create additional files and compare the final output to a data file
b = razel.add_task('b.csv', 'write-file', [razel.add_output_file('b.csv'), 'a,b,xyz', '3,4,56', '7,8,9'])
c = razel.add_task('c.csv', 'csv-concat', [a, b, razel.add_output_file('c.csv')])
razel.add_task('filtered.csv', 'csv-filter', ['-i', c, '-o', razel.add_output_file('filtered.csv'), '-c', 'a', 'xyz']) \
    .ensure_equal(f)
# add command to copy a file using the OS executable
d = razel.add_command('d.csv', 'cp', [a, razel.add_output_file('d.csv')]) \
    .add_tag('copy')
d.ensure_equal(a)
# add command to copy a file using a WASM module with WASI
razel.add_command('e.csv', 'bin/wasm32-wasi/cp.wasm', [d, razel.add_output_file('e.csv')]) \
    .add_tag('copy') \
    .ensure_equal(a)

if False:  # requires clang
    # compile an executable from a c file
    say_hi = razel.add_command('say_hi', 'clang',
                               ['-o', razel.add_output_file('say_hi'), razel.add_data_file('data/say_hi.c')])
    # run it, redirect stdout to a file and compare it with the output of another command
    razel.add_command('say_hi_using_c', say_hi, ['Razel']) \
        .write_stdout_to_file() \
        .add_tag(Razel.Tag.VERBOSE) \
        .ensure_equal(razel.add_command('say_hi_using_echo', 'echo', ['Hi Razel!']).write_stdout_to_file())

# execute the commands using the native razel binary (will be downloaded)
razel.run(['exec'] + sys.argv[1:])

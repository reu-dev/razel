#!/usr/bin/env python3

import os.path as path
import sys

sys.path.insert(1, path.join(path.dirname(path.dirname(path.abspath(__file__))), "include", "python"))
from razel import Razel

workspace_dir = path.dirname(path.abspath(__file__))
razel = Razel.init(workspace_dir)

# data/a.csv and data/f.csv are two input files
a = razel.add_data_file('data/a.csv')
f = razel.add_data_file('data/f.csv')
# add task to verify that they differ
a.ensure_not_equal(f)
# add tasks to create additional files and compare the final output to a data file
b = razel.add_task('b.csv', 'write-file', [razel.add_output_file('b.csv'), 'a,b,xyz', '3,4,56', '7,8,9']).output
c = razel.add_task('c.csv', 'csv-concat', [a, b, razel.add_output_file('c.csv')]).output
razel.add_task('filtered.csv', 'csv-filter', ['-i', c, '-o', razel.add_output_file('filtered.csv'), '-c', 'a', 'xyz']) \
    .output \
    .ensure_equal(f)
# add commands to copy a file
d = razel.add_command('d.csv', 'cp', [a, razel.add_output_file('d.csv')]).output
razel.add_command('e.csv', 'cp', [d, razel.add_output_file('e.csv')]) \
    .output \
    .ensure_equal(a)

razel.write_razel_file()

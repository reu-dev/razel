razel task write tmp/a.csv a,b,xyz 1,2,345
razel task write tmp/b.csv a,b,xyz 3,4,56 7,8,9
razel task csv-concat tmp/a.csv tmp/b.csv tmp/c.csv
razel task csv-filter-cols -i tmp/a.csv -o tmp/a.filtered.csv -c a xyz

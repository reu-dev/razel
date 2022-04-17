# data/a.csv is data file
razel task write b.csv a,b,xyz 3,4,56 7,8,9
razel task csv-concat data/a.csv b.csv c.csv
razel task csv-filter -i c.csv -o filtered.csv -c a xyz
razel task ensure-equal filtered.csv data/f.csv

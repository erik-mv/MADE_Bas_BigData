#! /usr/bin/python
import sys

current_years_tag = None
tag_count = 0

for line in sys.stdin:
    years_tag, counts = line.split("\t", 1)
    counts = int(counts)
    if years_tag == current_years_tag:
        tag_count += counts
    else:
        if current_years_tag:
            current_years, current_tag = current_years_tag.split(",", 1)
            print(current_years, tag_count, current_tag, sep="\t")
        current_years_tag = years_tag
        tag_count = counts

if current_years_tag:
    current_years, current_tag = current_years_tag.split(",", 1)
    print(current_years, tag_count, current_tag, sep="\t")

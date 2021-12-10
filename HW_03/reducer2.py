#! /usr/bin/python
import sys

current_years_tag = None
tag_count = 0

flag_first = 0
flag_second = 0

for line in sys.stdin:
    current_years, tag_count, current_tag = line.split("\t", 2)
    if flag_first < 10 and current_years == '2010':
        print (current_years, current_tag.replace('\n', ''), tag_count, sep="\t")
        flag_first += 1

    elif flag_second < 10 and current_years == '2016':
        print (current_years, current_tag.replace('\n', ''), tag_count, sep="\t")
        flag_second += 1

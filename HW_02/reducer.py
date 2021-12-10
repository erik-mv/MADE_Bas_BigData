#! /usr/bin/python
import sys

for line in sys.stdin:
    key = line.replace('\n', '')
    print(key)

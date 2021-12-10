#! /usr/bin/python
import sys
import numpy as np

flag = 1
result = ""
for line in sys.stdin:
    result += line.replace('\n', '')
    if flag > np.random.random_integers(0, 4):
        print(result)
        flag = 1
        result = ""
    else:
        flag += 1
        result += ","

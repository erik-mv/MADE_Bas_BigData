#! /usr/bin/python
import sys
from lxml import etree

for line in sys.stdin:
    tags = []
    try:
        tree = etree.XML(line)
        date = tree.get('CreationDate')
        tags = tree.get('Tags')
        if tags is not None:
            date = date[0:4:]
            if date in ['2010', '2016']:
                tags = tags[1:-1:].split('><')
                for tag in tags:
                    print (date + ',' + tag + '\t' + str(1))
    except:
        pass

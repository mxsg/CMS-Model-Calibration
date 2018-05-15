#!/usr/bin/env python
#-*- coding: utf-8 -*-

import ast
import json

def main():
    # Import file
    wmarchive_record = ''
    with open('wmarchive_full_20180511_merged', 'r') as f:
        first_line = f.readline()
        wmarchive_record = ast.literal_eval(first_line)

    pretty_record = json.dumps(wmarchive_record, indent=4)
    print(pretty_record)


    with open('example_wmarchive.json', 'w') as f:
        f.write(pretty_record)
        f.write('\n')

if __name__ == '__main__':
    main()

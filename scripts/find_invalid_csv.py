"""
Search for irregular data in a CSV file. This script finds lines in the file that do not have an
equal number of columns as the header (or first line) of the file.
"""

import sys


def main():
    with open(sys.argv[1], 'r') as f:

        header = f.readline()
        header_names = header.split(',')

        element_count = len(header_names)

        print("Searching for Irregular data ...")
        print("Header has {} elements".format(element_count))
        print("Header: {}".format(header_names))

        irregular_lines = 0

        for i, line in enumerate(f):
            line_elements = line.split(',')
            if len(line_elements) != element_count:
                irregular_lines += 1
                print("--- Mismatched Line Found: Line {}".format(i))
                print("number of elements: {}".format(line_elements))
                for i, j in zip(header_names, line_elements):
                    print("{}: {}".format(i, j))

        print("Finished, found {} irregular lines.".format(irregular_lines))


if __name__ == '__main__':
    main()

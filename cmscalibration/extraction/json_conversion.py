import ast
import json

import sys


def main():
    if len(sys.argv) != 3:
        print("Please provide source and target file paths.")

    with open(sys.argv[1], 'r') as infile:
        with open(sys.argv[2], 'w') as outfile:

            for i, input_line in enumerate(infile):

                # Provide progress
                if i % 1000 == 0:
                    print("Converting line {}".format(i))

                entry = ast.literal_eval(input_line.strip())
                outfile.write(json.dumps(entry) + '\n')


if __name__ == '__main__':
    main()

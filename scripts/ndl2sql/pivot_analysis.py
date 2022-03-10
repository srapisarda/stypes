#! /usr/local/bin/python3.6

import argparse
import csv


def __print_header():
    header = ','.join(list(
        map(lambda par: f'duration-{par},tasks-{par},dmi-{par},tmi-{par}', [5, 10, 15, 20])))
    print('data-set,evaluation,' + header)


def __pivot_csv(csv_file: str):
    with open(csv_file, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        __print_header()
        next(reader, None)
        evaluation = ''
        row_pivot = []
        for row in reader:
            if len(row) < 0:
                print(','.join(row_pivot))
                break
            elif row[2] != evaluation:
                if evaluation != "":
                    print(','.join(row_pivot))
                evaluation = row[2]
                row_pivot = []
                row_pivot.extend(row[1:])
            else:
                row_pivot.extend(row[3:])


def __main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--csv-file', dest='csv_file', required=True, help='--csv-file <csv_file>', type=str)
    args = parser.parse_args()
    __pivot_csv(args.csv_file)


if __name__ == '__main__':
    __main()

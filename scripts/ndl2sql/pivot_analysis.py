#! /usr/local/bin/python3.6

import argparse
import csv


def __get_reordered_row(row) -> list:
    new_row = [row[0], row[1]]
    for i in range(2, 6):
        for j in range(6):
            new_row.append(row[i + (j * 4)])
    return new_row


def __get_header_b(columns: list, parallelism: list):
    return ','.join(
        list(map(lambda col: ','.join(list(map(lambda par: f'{col}-{par}', parallelism))), columns)))


def __get_header_a(columns: list, parallelism: list):
    return ','.join(
        list(map(lambda par: ','.join(list(map(lambda col: f'{col}-{par}', columns))), parallelism)))

parallelism = [1, 3, 5, 10, 15, 20]
columns = ['duration', 'tasks', 'dp', 'tp', 'stddev']

def __print_header(match_columns):

    if match_columns:
        header = __get_header_b(columns, parallelism)
    else:
        header = __get_header_a(columns, parallelism)
    print('data-set,evaluation,' + header)


def __pivot_csv(csv_file: str, match_columns: bool):
    parallelism_dic = {1: 0, 3: 1, 5: 2, 10: 3, 15: 4, 20: 5}

    with open(csv_file, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        __print_header(match_columns)
        next(reader, None)
        evaluation = ''
        row_pivot = []
        for row in reader:
            if len(row) < 0:
                __print_row(row_pivot, match_columns)
                break
            elif row[2] != evaluation:
                if evaluation != "":
                    __print_row(row_pivot, match_columns)
                evaluation = row[2]
                row_pivot = [row[1], row[2]]
                row_pivot.extend(['' for _ in range(len(parallelism) * len(columns))])  # Initialize with dashes for each parallelism

            columns_parallelism = parallelism_dic[int(row[0])]
            for idx, value in enumerate(row[3:]):
                pos  =  idx * len(parallelism) + columns_parallelism + 2
                row_pivot[pos] = value


        __print_row(row_pivot, match_columns)


def __print_row(row_pivot, match_columns):
#     if match_columns:
#         row_pivot = __get_reordered_row(row_pivot)
    print(','.join(row_pivot))


def __main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--csv-file', dest='csv_file', required=True, help='--csv-file <csv_file>', type=str)
    parser.add_argument('-m', '--match-columns', dest='match_columns', required=False, default=False,
                        help='--match-columns <match_columns>', type=bool)
    args = parser.parse_args()
    __pivot_csv(args.csv_file, args.match_columns)


if __name__ == '__main__':
    __main()

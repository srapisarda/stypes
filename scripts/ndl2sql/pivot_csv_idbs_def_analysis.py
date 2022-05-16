import argparse
import csv


def __main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--csv-file', dest='csv_file', required=True, help='--csv-file <csv_file>', type=str)

    args = parser.parse_args()
    __pivot_csv_by_idb(args.csv_file)


def __print_header(csv_file):
    idbs = set()
    with open(csv_file, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')

        next(reader, None)
        for row in reader:
            idbs.add(row[1] + '_h')
            idbs.add(row[1] + '_b')
    ls = list(idbs)
    ls.sort()
    print('rewriting,' + ','.join(ls))


def __pivot_csv_by_idb(csv_file):
    with open(csv_file, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        __print_header(csv_file)
        next(reader, None)
        evaluation = ''
        row_pivot = []
        idb = ''
        pivot = []
        # for row in reader:
        #     if row[1] == idb:
        #         pivot.



if __name__ == '__main__':
    __main()

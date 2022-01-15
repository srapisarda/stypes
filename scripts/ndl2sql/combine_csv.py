import os
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--file-pattern', dest='file_pattern', required=False, help='--file-pattern <file_pattern>', type=str, default='.csv')
    parser.add_argument('-o', '--otput-file', dest='output_file', required=True, help='--output-file <output_file>', type=str)

    args = parser.parse_args()


if __name__ == '__main__':
    main()
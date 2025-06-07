import argparse
import csv


def extract_duration_stddev(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Read the header and extract relevant column indices
        header = next(reader)
        duration_indices = [i for i, col in enumerate(header) if col.startswith('duration')]
        stddev_indices = [i for i, col in enumerate(header) if col.startswith('stddev')]

        # Write the new header
        new_header = ['data-set', 'evaluation'] + [header[i] for i in duration_indices]
        writer.writerow(new_header)
        print(','.join(new_header))
        # Process and write rows
        for row in reader:
            new_row = [row[0], row[1]] + [
                f"{str( round( float(row[duration_idx]) / 1000, 2)).split('.')[0]}\\thn{{{str( round( float(row[duration_idx]) / 1000, 2)).split('.')[1]}}}\\std{{{str(round(float(row[stddev_idx]) / 1000, 2))}}}"
                if row[duration_idx] and row[stddev_idx] else "-"
                for duration_idx, stddev_idx in zip(duration_indices, stddev_indices)
            ]
            print('\t&'.join(new_row))
            writer.writerow(new_row)




def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-file', dest='csv_input_file', required=True, help='--input-file <csv_input_file>', type=str)
    parser.add_argument('-o', '--output_file', dest='csv_output_file', required=True,  help='--output-file <csv_outfile_file>', type=str)
    args = parser.parse_args()
    extract_duration_stddev(args.csv_input_file, args.csv_output_file)


if __name__ == '__main__':
    main()

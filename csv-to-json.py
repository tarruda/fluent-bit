import argparse
import csv
import json
import pathlib
import sys


def read_schema(schema_path):
    schema_str = schema_path.read_text().replace('REQUIRED', '""')
    data = json.loads(schema_str)
    return list(f['name'] for f in data['avro_schema']['fields'])


def process_file(fin, fout, schema):
    if schema:
        reader = csv.DictReader(fin, fieldnames=schema)
    else:
        reader = csv.reader(fin)
    for row in reader:
        json.dump(row, fout);
        fout.write('\n')


def parse_args():
    parser = argparse.ArgumentParser(
            description='Convert CSV to JSON')
    parser.add_argument('--schema-path',
            help='Path to avro schema for the CSV file')
    parser.add_argument('--enable-field-names',
            action='store_true', default=False,
            help='Read field names from the first line')
    return parser.parse_args()


def main():
    args = parse_args()
    schema = None
    if args.schema_path:
        schema = read_schema(pathlib.Path(args.schema_path))
    process_file(sys.stdin, sys.stdout, schema)


if __name__ == '__main__':
    main()

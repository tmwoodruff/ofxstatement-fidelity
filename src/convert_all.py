import csv
import shutil
import sys
import tempfile
from typing import Any
from io import TextIOWrapper
from ofxstatement.tool import run

def split_accounts(filename: str, outdir: str) -> dict[str, str]:
    result_paths: dict[str, str] = {}
    result_files: dict[str, TextIOWrapper] = {}
    result_writers: dict[str, Any] = {}

    header = None
    columns = {}

    with open(filename, "r") as fin:
        reader = csv.reader(fin)
        for row in reader:
            if not row:
                continue

            # skip blank lines
            if not row[0] or len(row) == 1:
                continue

            # header
            if row[0] == "Run Date":
                header = row
                for idx, col in enumerate(row):
                    columns[col] = idx
                continue

            # skip lines which are comments
            if row[0][:1] == '"':
                continue

            # skip any line that does not begin with a digit
            if not row[0][:1].isdigit():
                continue

            account = row[columns["Account Number"]]
            if not account:
                raise Exception(f"Missing account in row: {row}")

            writer = result_writers.get(account)
            if not writer:
                if not header:
                    raise Exception("No header found")
                respath = f"{outdir}/{account}.csv"
                resfile = open(respath, "w")
                writer = csv.writer(resfile)
                writer.writerow(header)
                result_files[account] = resfile
                result_writers[account] = writer
                result_paths[account] = respath

            writer.writerow(row)

    for writer in result_files.values():
        writer.close()

    return result_paths


def convert_files(account_paths: dict[str, str]):
    for account, csv in account_paths.items():
        rc = run(["convert", "-t", "fidelity", csv, f"{account}.ofx"])
        if rc != 0:
            print(f"Error {rc} converting {csv}")


def clean_dir(dir: str):
    shutil.rmtree(dir)

if __name__ == "__main__":
    infile = sys.argv[1]
    dir = tempfile.mkdtemp(prefix="ofx-")
    account_paths = split_accounts(infile, dir)
    convert_files(account_paths)

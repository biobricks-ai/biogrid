import numpy as np
import pandas as pd
from pathlib import Path
import pyarrow.lib as pa
import zipfile


def create_unzip_dir():
    unzip_dir = Path("unzip")
    unzip_dir.mkdir(exist_ok=True)


def unzip_file(f):
    file_path = Path(f)
    print(file_path)
    with open(file_path, "rb") as zip:
        try:   
            zip_root = zipfile.ZipFile(zip)
            zip_root.extractall(Path("unzip"))
        except zipfile.BadZipFile:
            print(f"There was a problem unzipping: {f}")


def read_file(f):
    filename = f.name or "Unnamed file"
    print(f"reading {filename} into CSV")
    dtypes = {"Entrez Gene Interactor A": np.int64,
              "Entrez Gene Interactor B": np.int64}
    return pd.read_csv(f, delimiter="\t", dtype=dtypes)


def create_out_dir():
    brick_dir = Path("brick")
    brick_dir.mkdir(exist_ok=True)
   

def create_parquet_file(f: Path):
    df = None
    try:
        df = read_file(f)
    except Exception:
        print(f"error reading file: {f.name}")
    if df is not None:
        outfile_name = f.relative_to(Path("unzip")).with_suffix(".parquet")
        outfile_path = Path("brick") / outfile_name
        try:
            df.to_parquet(outfile_path)
        except pa.ArrowTypeError:
            print("Error writing Parquet file")


def run_script():
    create_unzip_dir()
    create_out_dir()
    raw_path = Path("download")
    unzip_path = Path("unzip")
    for file in raw_path.rglob("*.zip"):
        print(file.name)
        unzip_file(file)
    for file in unzip_path.rglob("*.txt"):
        create_parquet_file(file)


if __name__ == '__main__':
    run_script()

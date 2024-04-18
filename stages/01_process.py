import logging
import pandas as pd
from pathlib import Path
import pyarrow.lib as pa
import re
import zipfile

logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def create_unzip_dir():
    logger.info("creating unzip/ directory")
    unzip_dir = Path("unzip")
    unzip_dir.mkdir(exist_ok=True)
    return unzip_dir


ZIP_DIR_PATTERN = re.compile(".*((?=tab3)|(?=ptm)|(?=chemtab)|(?=-4.4.232))")


def unzip_file(f):
    file_path = Path(f)
    match = re.match(ZIP_DIR_PATTERN, file_path.stem)
    if match is not None:
        directory_base = match[0]
        base_path = Path("unzip")
        if directory_base[-1] == ".":
            base_path /= Path(directory_base[:-1])
        else:
            base_path /= Path(directory_base)
        if not base_path.exists():
            base_path.mkdir()
            logger.info("unzipping file at %s", base_path)
    with open(file_path, "rb") as zip:
        try:
            zip_root = zipfile.ZipFile(zip)
            zip_root.extractall(base_path)
        except zipfile.BadZipFile:
            logger.debug("Problem unzipping file", exc_info=1)


DTYPES = {k: pd.StringDtype(storage="pyarrow") for k in
          ["SWISS-PROT Accessions Interactor A",
           "TREMBL Accessions Interactor A",
           "REFSEQ Accessions Interactor A",
           "SWISS-PROT Accessions Interactor B",
           "TREMBL Accessions Interactor B",
           "REFSEQ Accessions Interactor B",
           "Ontology Term IDs",
           "Ontology Term Names",
           "Ontology Term Categories",
           "Ontology Term Qualifier IDs",
           "Ontology Term Qualifier Names",
           "Ontology Term Types",
           "Tags", "Qualifications",
           "Post Translational Modification"]}


def read_file(f):
    logger.info("reading CSV from %s", f.name)
    df = pd.read_csv(f,
                     na_values=["-"],
                     delimiter="\t",
                     dtype=DTYPES,
                     low_memory=False,
                     on_bad_lines='skip')
    if df is not None:
        df.rename(columns={"#BioGRID Interaction ID": "BioGRID Interaction ID"},
                  inplace=True)
        return df
    else:
        logger.warning("Something went wrong reading this DataFrame")
        return None


def create_out_dir():
    logger.info("Creating brick/ directory")
    brick_dir = Path("brick")
    brick_dir.mkdir(exist_ok=True)


def create_parquet_file(f: Path):
    df = None
    try:
        df = read_file(f)
    except Exception:
        logger.debug(f"error reading file: {f.name}")
    if df is not None:
        outfile_name = f.relative_to(Path("unzip")).with_suffix(".parquet")
        outfile_rel_path = outfile_name.parents[0]
        outfile_path = Path("brick") / outfile_rel_path
        if not outfile_path.exists():
            outfile_path.mkdir()
        try:
            logger.info("Writing CSV data to Parquet")
            df.to_parquet(Path("brick") / outfile_name)
        except pa.ArrowTypeError as arrow_err:
            logger.error("Error writing Parquet file: %s", arrow_err,
                         exc_info=1)
    else:
        logger.info("File name with null DF: %s", f)


def run_script():
    logger.info("Running process script.")
    create_unzip_dir()
    create_out_dir()
    raw_path = Path("download")
    unzip_path = Path("unzip")
    for file in raw_path.rglob("*.zip"):
        unzip_file(file)
    for file in unzip_path.rglob("*.txt"):
        create_parquet_file(file)


if __name__ == '__main__':
    run_script()

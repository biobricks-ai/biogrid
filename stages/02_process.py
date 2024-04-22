import logging
import pandas as pd
from pathlib import Path
import pyarrow.lib as pa


logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


DTYPES = {
    k: pd.StringDtype(storage="pyarrow")
    for k in [
        "SWISS-PROT Accessions Interactor A",
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
        "Tags",
        "Qualifications",
        "Post Translational Modification",
        "Entrez Gene Interactor A" "Entrez Gene Interactor B",
        "Related BioGRID Gene ID",
        "Related Entrez Gene ID",
        "Related Organism ID",
        "Synonyms Interactor A",
        "Synonyms Interactor B",
        "Systematic Name Interactor A",
        "Systematic Name Interactor B",
    ]
}


def read_file(f):
    logger.info("reading CSV from %s", f.name)
    df = pd.read_csv(
        f,
        na_values=["-"],
        delimiter="\t",
        dtype=DTYPES,
        low_memory=False,
        on_bad_lines="skip",
    )
    if df is not None:
        df.rename(
            columns={
                "#BioGRID Interaction ID": "BioGRID Interaction ID",
                "#BIOGRID ID": "BIOGRID ID",
                "#BioGRID Chemical Interaction ID": "BioGRID Chemical Interaction ID",
                "#PTM ID": "PTM ID",
            },
            inplace=True,
        )
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
            logger.error("Error writing Parquet file: %s", arrow_err, exc_info=1)
    else:
        logger.info("File name with null DF: %s", f)


def run_script():
    logger.info("Running process script.")
    create_out_dir()
    unzip_path = Path("unzip")
    for file in unzip_path.rglob("*.txt"):
        create_parquet_file(file)


if __name__ == "__main__":
    run_script()

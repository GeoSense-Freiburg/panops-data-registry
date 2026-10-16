"""Convert a GBIF SIMPLE_CSV download into a Parquet dataset shaped like SIMPLE_PARQUET.

GBIF's SIMPLE_PARQUET export writes Catalogue of Life IDs into the taxon key
columns whatever checklist the request names, while SIMPLE_CSV honors the
request's checklistKey. To get GBIF backbone keys in Parquet, download SIMPLE_CSV
and convert it with this script.

Column names are lowercased to match SIMPLE_PARQUET and numeric columns get the
same types. Multi-value and timestamp columns (identifiedby, recordedby,
typestatus, mediatype, issue, dateidentified, lastinterpreted) stay as the plain
strings GBIF writes in the CSV.
"""

import json
import zipfile
from pathlib import Path

import click
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from src.utils.log_utils import setup_logger

log = setup_logger(__name__, "INFO")

NUMERIC_TYPES = {
    "individualcount": pa.int32(),
    "decimallatitude": pa.float64(),
    "decimallongitude": pa.float64(),
    "coordinateuncertaintyinmeters": pa.float64(),
    "coordinateprecision": pa.float64(),
    "elevation": pa.float64(),
    "elevationaccuracy": pa.float64(),
    "depth": pa.float64(),
    "depthaccuracy": pa.float64(),
    "day": pa.int32(),
    "month": pa.int32(),
    "year": pa.int32(),
}


def _to_parquet_shape(batch: pa.RecordBatch) -> pa.Table:
    """Lowercase the column names and cast the numeric columns."""
    table = pa.Table.from_batches([batch])
    table = table.rename_columns([name.lower() for name in table.column_names])
    for name, dtype in NUMERIC_TYPES.items():
        i = table.schema.get_field_index(name)
        table = table.set_column(i, name, table.column(name).cast(dtype))
    return table


def convert(zip_path: Path, out_dir: Path, rows_per_file: int) -> dict[str, int]:
    """Stream the CSV inside ``zip_path`` into numbered Parquet files in ``out_dir``."""
    out_dir.mkdir(parents=True, exist_ok=False)
    skipped = 0

    def _skip(row: pacsv.InvalidRow) -> str:
        nonlocal skipped
        skipped += 1
        return "skip"

    with zipfile.ZipFile(zip_path) as zf:
        (csv_name,) = [n for n in zf.namelist() if n.endswith(".csv")]
        with zf.open(csv_name) as f:
            header = f.readline().decode("utf-8").rstrip("\n").split("\t")
        with zf.open(csv_name) as f:
            reader = pacsv.open_csv(
                f,
                read_options=pacsv.ReadOptions(block_size=64 << 20),
                parse_options=pacsv.ParseOptions(
                    delimiter="\t", quote_char=False, invalid_row_handler=_skip
                ),
                convert_options=pacsv.ConvertOptions(
                    column_types={name: pa.string() for name in header},
                    strings_can_be_null=True,
                ),
            )
            n_rows = n_files = rows_in_file = 0
            writer = None
            for batch in reader:
                table = _to_parquet_shape(batch)
                if writer is None or rows_in_file >= rows_per_file:
                    if writer is not None:
                        writer.close()
                    writer = pq.ParquetWriter(
                        out_dir / f"{n_files:06d}", table.schema, compression="zstd"
                    )
                    n_files += 1
                    rows_in_file = 0
                writer.write_table(table)
                rows_in_file += table.num_rows
                n_rows += table.num_rows
                if n_files % 20 == 0 and rows_in_file == table.num_rows:
                    log.info("%d rows in %d files", n_rows, n_files)
            if writer is not None:
                writer.close()
    return {"rows": n_rows, "files": n_files, "skipped_rows": skipped}


@click.command()
@click.argument("zip_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("out_dir", type=click.Path(path_type=Path))
@click.option("--rows-per-file", type=int, default=2_000_000, show_default=True)
def main(zip_path: Path, out_dir: Path, rows_per_file: int):
    """Convert GBIF SIMPLE_CSV ZIP_PATH into a Parquet dataset at OUT_DIR."""
    stats = convert(zip_path, out_dir, rows_per_file)
    log.info("Done: %s", stats)
    (out_dir.parent / f"{out_dir.name}.conversion.json").write_text(json.dumps(stats))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

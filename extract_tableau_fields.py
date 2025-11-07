"""extract_tableau_fields.py - extract used Tableau fields per workbook and per data source"""

from __future__ import annotations
import argparse
import logging
import zipfile
import re
from pathlib import Path
from xml.etree import ElementTree as ET
import csv
import sys
import tkinter as tk
from tkinter import filedialog
from typing import Iterable, List, Set, Dict

logger = logging.getLogger(__name__)

FIELD_ATTRS = ('column', 'column-instance', 'references', 'ref')
NAME_TAGS_TO_TRUST = {'column', 'column-instance', 'datasource', 'calculation'}

def get_twb_xml(path: Path, encoding: str = 'utf-8') -> str:
    if not path.exists():
        raise FileNotFoundError(f"{path} does not exist")
    suffix = path.suffix.lower()
    if suffix == '.twbx':
        with zipfile.ZipFile(path, 'r') as z:
            for name in z.namelist():
                if name.lower().endswith('.twb'):
                    logger.debug("Found .twb in zip: %s", name)
                    data = z.read(name)
                    return data.decode(encoding)
            raise FileNotFoundError(".twb not found inside .twbx archive")
    elif suffix == '.twb':
        return path.read_text(encoding=encoding)
    else:
        raise ValueError("Unsupported file type. Provide a .twb or .twbx file")

def extract_used_fields(xml_text: str):
    """
    Returns:
        used_fields: list of all fields actually used in Tableau
        ds_to_used_fields: dict mapping datasource name -> used fields from that datasource
    """
    used_fields: Set[str] = set()
    ds_to_all_columns: Dict[str, Set[str]] = {}
    ds_to_used_fields: Dict[str, Set[str]] = {}

    root = ET.fromstring(xml_text)

    # --- Collect all columns per datasource recursively ---
    for ds in root.findall('.//datasource'):
        ds_name = ds.get('name') or 'UNKNOWN_DS'
        ds_columns = set()
        for col in ds.findall('.//column'):
            col_name = col.get('name')
            if col_name:
                ds_columns.add(col_name.strip().strip('[]'))
        ds_to_all_columns[ds_name] = ds_columns
        ds_to_used_fields[ds_name] = set()

    # --- Collect all fields used anywhere in workbook ---
    for elem in root.iter():
        tag = elem.tag.lower().split('}')[-1]

        # Attributes likely to reference fields
        for attr in FIELD_ATTRS:
            val = elem.get(attr)
            if val:
                val = val.strip().strip('[]')
                used_fields.add(val)

        # 'name' attribute from trusted tags
        if tag in NAME_TAGS_TO_TRUST:
            val = elem.get('name')
            if val:
                val = val.strip().strip('[]')
                used_fields.add(val)

        # Formula-like [Field Name]
        if elem.text:
            for m in re.finditer(r'\[([^\]]+)\]', elem.text):
                candidate = m.group(1).strip()
                used_fields.add(candidate)

    # --- Map used fields to datasource columns ---
    for ds_name, columns in ds_to_all_columns.items():
        ds_to_used_fields[ds_name] = {f for f in used_fields if f in columns}

    # Filter out numeric-only or empty strings
    used_fields = {f for f in used_fields if f and not f.isdigit()}
    for ds in ds_to_used_fields:
        ds_to_used_fields[ds] = {f for f in ds_to_used_fields[ds] if f and not f.isdigit()}

    return sorted(used_fields), {k: sorted(v) for k, v in ds_to_used_fields.items()}


def write_csv(fields: Iterable[str], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['field_name'])
        for s in fields:
            writer.writerow([s])

def write_datasource_csv(ds_to_fields: dict[str, List[str]], out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['datasource', 'field_name'])
        for ds, fields in ds_to_fields.items():
            for f in fields:
                writer.writerow([ds, f])

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Extract used Tableau field names from .twb/.twbx")
    p.add_argument('workbook', type=Path, nargs='?', help='path to workbook.twb or workbook.twbx')
    p.add_argument('-o', '--output', type=Path, default=Path('used_fields.csv'),
                   help='output csv path for all used fields (default: used_fields.csv)')
    p.add_argument('-d', '--datasource-output', type=Path, default=Path('datasource_fields.csv'),
                   help='output csv path for fields per datasource (default: datasource_fields.csv)')
    p.add_argument('-v', '--verbose', action='store_true', help='enable debug logging')
    return p

def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='%(levelname)s: %(message)s')

    # --- Tkinter file dialog if no path provided ---
    if not args.workbook:
        logger.info("No workbook path provided, opening file dialog...")
        root = tk.Tk()
        root.withdraw()
        filetypes = [("Tableau Workbook", "*.twb *.twbx"), ("All files", "*.*")]
        selected_file = filedialog.askopenfilename(title="Select Tableau workbook", filetypes=filetypes)
        if not selected_file:
            logger.error("No file selected, exiting.")
            return 1
        workbook_path = Path(selected_file)
    else:
        workbook_path = args.workbook

    logger.info(f"Using workbook: {workbook_path}")

    try:
        xml = get_twb_xml(workbook_path)
    except Exception as e:
        logger.error("Failed to read workbook: %s", e)
        return 2

    try:
        used_fields, ds_to_used_fields = extract_used_fields(xml)
    except ET.ParseError as e:
        logger.error("XML parse failed: %s", e)
        return 3
    except Exception as e:
        logger.error("Unexpected error parsing xml: %s", e)
        return 4

    try:
        write_csv(used_fields, args.output)
        write_datasource_csv(ds_to_used_fields, args.datasource_output)
    except Exception as e:
        logger.error("Failed to write output CSVs: %s", e)
        return 5

    logger.info("Wrote %d used fields to %s", len(used_fields), args.output.resolve())
    logger.info("Wrote datasource-mapped fields to %s", args.datasource_output.resolve())
    return 0

if __name__ == '__main__':
    raise SystemExit(main())

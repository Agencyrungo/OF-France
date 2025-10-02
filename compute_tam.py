import csv
import os
import statistics
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

TARGET_MIN = 3
TARGET_MAX = 10

REGION_NAMES = {
    11: "Île-de-France",
    24: "Centre-Val de Loire",
    27: "Bourgogne-Franche-Comté",
    28: "Normandie",
    32: "Hauts-de-France",
    44: "Grand Est",
    52: "Pays de la Loire",
    53: "Bretagne",
    75: "Nouvelle-Aquitaine",
    76: "Occitanie",
    84: "Auvergne-Rhône-Alpes",
    93: "Provence-Alpes-Côte d'Azur",
    94: "Corse",
    1: "Guadeloupe",
    2: "Martinique",
    3: "Guyane",
    4: "La Réunion",
    6: "Mayotte",
    975: "Saint-Pierre-et-Miquelon",
    977: "Saint-Barthélemy",
    978: "Saint-Martin",
    986: "Wallis-et-Futuna",
    987: "Polynésie française",
    988: "Nouvelle-Calédonie",
    989: "Îles de Clipperton",
}

REGION_ORDER = [
    11,
    84,
    76,
    93,
    75,
    44,
    52,
    32,
    53,
    28,
    27,
    24,
    94,
]


@dataclass
class Record:
    denomination: str
    effectif: Optional[int]
    nb_stagiaires: Optional[float]
    actions_cert: Optional[float]
    code_region: Optional[int]


def load_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    shared_strings: List[str] = []
    path = "xl/sharedStrings.xml"
    if path not in zf.namelist():
        return shared_strings
    with zf.open(path) as f:
        for event, elem in ET.iterparse(f, events=("end",)):
            if elem.tag == NS + "si":
                text = "".join(t.text or "" for t in elem.findall('.//' + NS + 't'))
                shared_strings.append(text)
                elem.clear()
    return shared_strings


def column_ref_to_index(ref: str) -> int:
    letters = "".join(ch for ch in ref if ch.isalpha())
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def get_cell_value(cell: ET.Element, shared_strings: List[str]):
    t = cell.attrib.get("t")
    if t == "s":
        v = cell.find(NS + "v")
        if v is None or v.text is None:
            return None
        return shared_strings[int(v.text)]
    if t == "inlineStr":
        is_elem = cell.find(NS + "is")
        if is_elem is None:
            return None
        return "".join(t_el.text or "" for t_el in is_elem.findall('.//' + NS + 't'))
    v = cell.find(NS + "v")
    if v is None:
        return None
    return v.text


def parse_int(value) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if "." in text:
            return int(round(float(text)))
        return int(text)
    except ValueError:
        return None


def parse_float(value) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_records() -> List[Record]:
    records: List[Record] = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            header_map: Dict[int, str] = {}
            denom_idx = effectif_idx = stagiaires_idx = actions_idx = region_idx = None
            for event, elem in ET.iterparse(f, events=("end",)):
                if elem.tag != NS + "row":
                    continue
                row_index = int(elem.attrib.get("r"))
                values: Dict[int, str] = {}
                if row_index == 1:
                    for cell in elem.findall(NS + "c"):
                        ref = cell.attrib.get("r")
                        if not ref:
                            continue
                        col_idx = column_ref_to_index(ref)
                        val = get_cell_value(cell, shared_strings)
                        if val is not None:
                            header_map[col_idx] = val
                    denom_idx = next((idx for idx, name in header_map.items() if name == "denominationSociale"), None)
                    effectif_idx = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.effectifFormateurs"), None)
                    stagiaires_idx = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.nbStagiaires"), None)
                    actions_idx = next((idx for idx, name in header_map.items() if name == "certifications.actionsDeFormation"), None)
                    region_idx = next((idx for idx, name in header_map.items() if name == "adressePhysiqueOrganismeFormation.codeRegion"), None)
                    elem.clear()
                    continue

                target_indices = {
                    idx
                    for idx in [denom_idx, effectif_idx, stagiaires_idx, actions_idx, region_idx]
                    if idx is not None
                }
                found = set()
                for cell in elem.findall(NS + "c"):
                    ref = cell.attrib.get("r")
                    if not ref:
                        continue
                    col_idx = column_ref_to_index(ref)
                    if col_idx not in target_indices:
                        continue
                    val = get_cell_value(cell, shared_strings)
                    if val is not None:
                        values[col_idx] = val
                        found.add(col_idx)
                    if found == target_indices:
                        break

                record = Record(
                    denomination=str(values.get(denom_idx, "")),
                    effectif=parse_int(values.get(effectif_idx)),
                    nb_stagiaires=parse_float(values.get(stagiaires_idx)),
                    actions_cert=parse_float(values.get(actions_idx)),
                    code_region=parse_int(values.get(region_idx)),
                )
                records.append(record)
                elem.clear()
    return records


def filter_tam(records: Iterable[Record]) -> List[Record]:
    result: List[Record] = []
    for rec in records:
        if rec.effectif is None:
            continue
        if not (TARGET_MIN <= rec.effectif <= TARGET_MAX):
            continue
        if rec.actions_cert is None:
            continue
        if rec.nb_stagiaires is None or rec.nb_stagiaires <= 0:
            continue
        result.append(rec)
    return result


def safe_mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def safe_median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return statistics.median(values)


def format_number(value: Optional[float], decimals: int = 0) -> str:
    if value is None:
        return "-"
    if decimals == 0:
        return f"{int(round(value)):,}".replace(",", " ")
    return f"{value:,.{decimals}f}".replace(",", " ")


def write_table(markdown_lines: List[str], table_name: str, headers: List[str], rows: List[List[str]]):
    markdown_lines.append(table_name)
    markdown_lines.append("| " + " | ".join(headers) + " |")
    markdown_lines.append("|" + "|".join([" --- " for _ in headers]) + "|")
    for row in rows:
        markdown_lines.append("| " + " | ".join(row) + " |")
    markdown_lines.append("")


def main():
    ensure_output_dir()
    records = load_records()
    total_base = len(records)

    filtered_effectif = [r for r in records if r.effectif is not None and TARGET_MIN <= r.effectif <= TARGET_MAX]
    filtered_cert = [r for r in filtered_effectif if r.actions_cert is not None]
    filtered_active = [r for r in filtered_cert if r.nb_stagiaires is not None and r.nb_stagiaires > 0]

    tam_records = filtered_active

    # Table 1
    effectif_distribution: List[List[str]] = []
    total_count = len(tam_records)
    total_stagiaires = sum(r.nb_stagiaires for r in tam_records)
    effectif_totals = {}
    for eff in range(TARGET_MIN, TARGET_MAX + 1):
        subset = [r for r in tam_records if r.effectif == eff]
        count = len(subset)
        pct = (count / total_count * 100) if total_count else 0
        stagiaires = [r.nb_stagiaires for r in subset]
        mean_stagiaires = safe_mean(stagiaires)
        median_stagiaires = safe_median(stagiaires)
        total_stagiaires_eff = sum(stagiaires)
        effectif_totals[eff] = count
        effectif_distribution.append([
            str(eff),
            format_number(count),
            f"{pct:,.1f}%".replace(",", " "),
            format_number(mean_stagiaires, 1),
            format_number(median_stagiaires, 1),
            format_number(total_stagiaires_eff),
        ])
    effectif_distribution.append([
        "TOTAL",
        format_number(total_count),
        "100%",
        format_number(safe_mean([r.nb_stagiaires for r in tam_records]), 1),
        format_number(safe_median([r.nb_stagiaires for r in tam_records]), 1),
        format_number(total_stagiaires),
    ])

    # Table 2 and 3
    region_counts: Dict[int, List[Record]] = defaultdict(list)
    for rec in tam_records:
        region_counts[rec.code_region or 0].append(rec)

    region_rows: List[List[str]] = []
    for code, recs in sorted(region_counts.items(), key=lambda item: len(item[1]), reverse=True):
        count = len(recs)
        pct = (count / total_count * 100) if total_count else 0
        mean_stagiaires = safe_mean([r.nb_stagiaires for r in recs])
        mean_effectif = safe_mean([r.effectif for r in recs if r.effectif is not None])
        name = REGION_NAMES.get(code, "Autres DOM-TOM")
        region_rows.append([
            str(code),
            name,
            format_number(count),
            f"{pct:,.1f}%".replace(",", " "),
            format_number(mean_stagiaires, 1),
            format_number(mean_effectif, 1),
        ])

    region_rows.append([
        "TOTAL",
        "France",
        format_number(total_count),
        "100%",
        format_number(safe_mean([r.nb_stagiaires for r in tam_records]), 1),
        format_number(safe_mean([r.effectif for r in tam_records]), 1),
    ])

    # Table 3 matrix
    region_effectif_counts: Dict[int, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for rec in tam_records:
        code = rec.code_region or 0
        region_effectif_counts[code][rec.effectif] += 1

    ordered_regions = REGION_ORDER + [code for code in region_counts if code not in REGION_ORDER]

    table3_rows: List[List[str]] = []
    for code in ordered_regions:
        if code not in region_counts:
            continue
        name = REGION_NAMES.get(code, "Autres DOM-TOM")
        row = [str(code), name]
        row_total = 0
        for eff in range(TARGET_MIN, TARGET_MAX + 1):
            count = region_effectif_counts[code].get(eff, 0)
            row.append(format_number(count))
            row_total += count
        row.append(format_number(row_total))
        table3_rows.append(row)

    # Total row for matrix
    total_row = ["TOTAL", "France"]
    for eff in range(TARGET_MIN, TARGET_MAX + 1):
        total_row.append(format_number(effectif_totals.get(eff, 0)))
    total_row.append(format_number(total_count))
    table3_rows.append(total_row)

    # Table 4 top regions by intensity
    intensity_rows: List[List[str]] = []
    intensity_data = []
    for code, recs in region_counts.items():
        count = len(recs)
        mean_stagiaires = safe_mean([r.nb_stagiaires for r in recs])
        mean_effectif = safe_mean([r.effectif for r in recs if r.effectif is not None])
        if mean_stagiaires is None:
            continue
        ratio = mean_stagiaires / mean_effectif if mean_effectif else None
        intensity_data.append((code, count, mean_stagiaires, ratio))

    intensity_data.sort(key=lambda x: (x[3] or 0), reverse=True)

    for rank, (code, count, mean_stagiaires, ratio) in enumerate(intensity_data[:10], start=1):
        intensity_rows.append([
            str(rank),
            str(code),
            REGION_NAMES.get(code, "Autres DOM-TOM"),
            format_number(count),
            format_number(mean_stagiaires, 1),
            format_number(ratio, 2),
        ])

    markdown_lines: List[str] = []
    write_table(
        markdown_lines,
        "### Tableau 1 : Distribution des effectifs (3-10 formateurs)",
        [
            "effectifFormateurs",
            "nombre_OF",
            "% du TAM",
            "stagiaires_moyen",
            "stagiaires_median",
            "stagiaires_total",
        ],
        effectif_distribution,
    )

    write_table(
        markdown_lines,
        "### Tableau 2 : Répartition par région",
        [
            "codeRegion",
            "nom_region",
            "nombre_OF_TAM",
            "% national",
            "stagiaires_moyen",
            "effectif_moyen",
        ],
        region_rows,
    )

    headers_table3 = ["codeRegion", "nom_region"] + [str(eff) for eff in range(TARGET_MIN, TARGET_MAX + 1)] + ["Total"]
    write_table(
        markdown_lines,
        "### Tableau 3 : Matrice région × effectif",
        headers_table3,
        table3_rows,
    )

    write_table(
        markdown_lines,
        "### Tableau 4 : Top 10 régions par intensité",
        [
            "Rang",
            "codeRegion",
            "nom_region",
            "nombre_OF",
            "stagiaires_moyen",
            "ratio_production",
        ],
        intensity_rows,
    )

    markdown_path = os.path.join(OUTPUT_DIR, "tam_summary.md")
    with open(markdown_path, "w", encoding="utf-8") as f:
        f.write("\n".join(markdown_lines) + "\n")

    csv_path = os.path.join(OUTPUT_DIR, "distribution_effectif.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "effectif",
            "nombre_OF",
            "pct_tam",
            "stagiaires_moyen",
            "stagiaires_median",
            "stagiaires_total",
        ])
        for row in effectif_distribution[:-1]:
            eff = row[0]
            if eff == "TOTAL":
                continue
            writer.writerow(row)

    # Synthesis text
    tam_total = total_count
    synth_lines = []
    synth_lines.append("## Synthèse exécutive")
    synth_lines.append("")
    synth_lines.append(f"TAM TOTAL QUALIFIÉ : {format_number(tam_total)} OF")
    synth_lines.append("")
    synth_lines.append("Base France : {0} OF".format(format_number(total_base)))
    synth_lines.append(f"↓ 3-10 formateurs : {format_number(len(filtered_effectif))} OF")
    synth_lines.append(f"↓ + Certifiés Qualiopi : {format_number(len(filtered_cert))} OF")
    synth_lines.append(f"↓ + Actifs : {format_number(len(filtered_active))} OF")
    synth_lines.append("")
    reference = 12303
    if reference:
        diff_pct = (tam_total - reference) / reference * 100
        synth_lines.append("Comparaison Document 1 :")
        synth_lines.append(f"- Hypothèse Document 1 : {format_number(reference)} OF")
        synth_lines.append(f"- TAM calculé : {format_number(tam_total)} OF")
        synth_lines.append(f"- Écart : {diff_pct:+.1f}%")
        synth_lines.append("")

    # Top insights
    top_regions = sorted(region_counts.items(), key=lambda item: len(item[1]), reverse=True)
    if top_regions:
        top_region_code, top_region_recs = top_regions[0]
        synth_lines.append(
            f"1. {REGION_NAMES.get(top_region_code, 'Autres DOM-TOM')} concentre {format_number(len(top_region_recs))} OF, soit {len(top_region_recs) / tam_total * 100:.1f}% du TAM."
        )
    top_effectif = max(effectif_totals.items(), key=lambda item: item[1]) if effectif_totals else None
    if top_effectif:
        eff, count = top_effectif
        synth_lines.append(
            f"2. Les structures de {eff} formateurs représentent {format_number(count)} OF ({count / tam_total * 100:.1f}% du TAM)."
        )
    if intensity_rows:
        first_intensity = intensity_rows[0]
        synth_lines.append(
            f"3. Intensité maximale en {first_intensity[2]} avec {first_intensity[5]} stagiaires par formateur."  # ratio already formatted
        )

    synth_path = os.path.join(OUTPUT_DIR, "synthese.md")
    with open(synth_path, "w", encoding="utf-8") as f:
        f.write("\n".join(synth_lines) + "\n")


if __name__ == "__main__":
    main()

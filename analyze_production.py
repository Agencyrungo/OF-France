import math
import os
import statistics
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

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

BINS: List[Tuple[str, float, Optional[float]]] = [
    ("0-2", 0.0, 2.0),
    ("2-5", 2.0, 5.0),
    ("5-10", 5.0, 10.0),
    ("10-15", 10.0, 15.0),
    ("15-20", 15.0, 20.0),
    ("20+", 20.0, None),
]

SEGMENTS = [
    ("A", "1-100 stag/an", 1, 100),
    ("B", "101-300 stag/an", 101, 300),
    ("C", "301+ stag/an", 301, None),
]


@dataclass
class Record:
    denomination: str
    effectif: Optional[int]
    nb_stagiaires: Optional[float]
    actions_cert: Optional[float]
    code_region: Optional[int]

    @property
    def stagiaires_mois(self) -> Optional[float]:
        if self.nb_stagiaires is None:
            return None
        return self.nb_stagiaires / 12.0

    @property
    def livrables(self) -> Optional[float]:
        if self.stagiaires_mois is None or self.effectif is None:
            return None
        return (self.stagiaires_mois / 20.0) + (self.effectif * 2.0)


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


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
    cell_type = cell.attrib.get("t")
    if cell_type == "s":
        v = cell.find(NS + "v")
        if v is None or v.text is None:
            return None
        return shared_strings[int(v.text)]
    if cell_type == "inlineStr":
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


def load_records() -> List[Record]:
    records: List[Record] = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            header_map: Dict[int, str] = {}
            target_indices: Dict[str, Optional[int]] = {
                "denomination": None,
                "effectif": None,
                "stagiaires": None,
                "actions": None,
                "region": None,
            }
            for event, elem in ET.iterparse(f, events=("end",)):
                if elem.tag != NS + "row":
                    continue
                row_index = int(elem.attrib.get("r"))
                if row_index == 1:
                    for cell in elem.findall(NS + "c"):
                        ref = cell.attrib.get("r")
                        if not ref:
                            continue
                        col_idx = column_ref_to_index(ref)
                        val = get_cell_value(cell, shared_strings)
                        if val is not None:
                            header_map[col_idx] = val
                    for key, header in [
                        ("denomination", "denominationSociale"),
                        ("effectif", "informationsDeclarees.effectifFormateurs"),
                        ("stagiaires", "informationsDeclarees.nbStagiaires"),
                        ("actions", "certifications.actionsDeFormation"),
                        ("region", "adressePhysiqueOrganismeFormation.codeRegion"),
                    ]:
                        target_indices[key] = next(
                            (idx for idx, name in header_map.items() if name == header),
                            None,
                        )
                    elem.clear()
                    continue

                indices = {idx for idx in target_indices.values() if idx is not None}
                values: Dict[int, str] = {}
                found = set()
                for cell in elem.findall(NS + "c"):
                    ref = cell.attrib.get("r")
                    if not ref:
                        continue
                    col_idx = column_ref_to_index(ref)
                    if col_idx not in indices:
                        continue
                    val = get_cell_value(cell, shared_strings)
                    if val is not None:
                        values[col_idx] = val
                        found.add(col_idx)
                    if found == indices:
                        break

                records.append(
                    Record(
                        denomination=str(values.get(target_indices["denomination"], "")),
                        effectif=parse_int(values.get(target_indices["effectif"])),
                        nb_stagiaires=parse_float(values.get(target_indices["stagiaires"])),
                        actions_cert=parse_float(values.get(target_indices["actions"])),
                        code_region=parse_int(values.get(target_indices["region"])),
                    )
                )
                elem.clear()
    return records


def filter_tam(records: Iterable[Record]) -> List[Record]:
    result: List[Record] = []
    for rec in records:
        if rec.effectif is None or not (TARGET_MIN <= rec.effectif <= TARGET_MAX):
            continue
        if rec.actions_cert is None or rec.actions_cert <= 0:
            continue
        if rec.nb_stagiaires is None or rec.nb_stagiaires <= 0:
            continue
        if rec.livrables is None:
            continue
        result.append(rec)
    return result


def safe_mean(values: Iterable[float]) -> Optional[float]:
    values = [v for v in values if v is not None]
    if not values:
        return None
    return sum(values) / len(values)


def safe_median(values: Iterable[float]) -> Optional[float]:
    values = [v for v in values if v is not None]
    if not values:
        return None
    return statistics.median(values)


def format_number(value: Optional[float], decimals: int = 0) -> str:
    if value is None:
        return "-"
    if decimals == 0:
        return f"{int(round(value)):,}".replace(",", " ")
    return f"{value:,.{decimals}f}".replace(",", " ")


def format_percent(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:,.{decimals}f}%".replace(",", " ")


def write_markdown_table(markdown_lines: List[str], title: str, headers: List[str], rows: List[List[str]]):
    markdown_lines.append(title)
    markdown_lines.append("| " + " | ".join(headers) + " |")
    markdown_lines.append("|" + "|".join([" --- " for _ in headers]) + "|")
    for row in rows:
        markdown_lines.append("| " + " | ".join(row) + " |")
    markdown_lines.append("")


def assign_segment(nb_stagiaires: float) -> Optional[Tuple[str, str]]:
    for seg_id, label, lower, upper in SEGMENTS:
        if nb_stagiaires < lower:
            continue
        if upper is None or nb_stagiaires <= upper:
            return seg_id, label
    return None


def compute_tables(records: List[Record]):
    total = len(records)
    if total == 0:
        raise ValueError("No records after filtering")

    # Table 1 - Distribution production
    rows_table1: List[List[str]] = []
    cumulative = 0
    for label, lower, upper in BINS:
        if upper is None:
            subset = [r for r in records if r.livrables >= lower]
        else:
            subset = [r for r in records if lower <= r.livrables < upper]
        count = len(subset)
        cumulative += count
        pct = (count / total) * 100
        stag_moy = safe_mean(r.stagiaires_mois for r in subset)
        eff_moy = safe_mean(r.effectif for r in subset)
        rows_table1.append(
            [
                label,
                format_number(count),
                format_percent(pct),
                format_number(stag_moy, 1),
                format_number(eff_moy, 1),
                format_percent((cumulative / total) * 100),
            ]
        )
    rows_table1.append(
        [
            "TOTAL",
            format_number(total),
            "100%",
            format_number(safe_mean(r.stagiaires_mois for r in records), 1),
            format_number(safe_mean(r.effectif for r in records), 1),
            "-",
        ]
    )

    # Table 2 - Validation hypothèse
    count_ge5 = sum(1 for r in records if r.livrables >= 5)
    pct_ge5 = (count_ge5 / total) * 100
    diff_pp = pct_ge5 - 60
    if 55 <= pct_ge5 <= 65:
        verdict = "✅"
        verdict_label = "VALIDÉ"
    elif 45 <= pct_ge5 < 55 or 65 < pct_ge5 <= 75:
        verdict = "⚠️"
        verdict_label = "AJUSTER"
    else:
        verdict = "❌"
        verdict_label = "INVALIDÉ"
    hyp_count = 7381
    rows_table2 = [
        [
            "% OF ≥5 livr/mois",
            "60%",
            format_percent(pct_ge5),
            format_percent(diff_pp),
            f"{verdict} {verdict_label}",
        ],
        [
            "Nb OF concernés",
            format_number(hyp_count),
            format_number(count_ge5),
            format_number(count_ge5 - hyp_count),
            "-",
        ],
    ]

    # Table 3 - Production par effectif
    rows_table3: List[List[str]] = []
    for eff in range(TARGET_MIN, TARGET_MAX + 1):
        subset = [r for r in records if r.effectif == eff]
        if not subset:
            continue
        livr_moy = safe_mean(r.livrables for r in subset)
        livr_med = safe_median(r.livrables for r in subset)
        pct_ge5_eff = (sum(1 for r in subset if r.livrables >= 5) / len(subset)) * 100
        pct_ge10_eff = (sum(1 for r in subset if r.livrables >= 10) / len(subset)) * 100
        rows_table3.append(
            [
                str(eff),
                format_number(len(subset)),
                format_number(livr_moy, 1),
                format_number(livr_med, 1),
                format_percent(pct_ge5_eff),
                format_percent(pct_ge10_eff),
            ]
        )
    rows_table3.append(
        [
            "TOTAL",
            format_number(total),
            format_number(safe_mean(r.livrables for r in records), 1),
            format_number(safe_median(r.livrables for r in records), 1),
            format_percent(pct_ge5),
            format_percent((sum(1 for r in records if r.livrables >= 10) / total) * 100),
        ]
    )

    # Table 4 - Segmentation activité
    rows_table4: List[List[str]] = []
    for seg_id, label, lower, upper in SEGMENTS:
        subset = []
        for r in records:
            if r.nb_stagiaires is None:
                continue
            if r.nb_stagiaires < lower:
                continue
            if upper is not None and r.nb_stagiaires > upper:
                continue
            subset.append(r)
        if not subset:
            continue
        pct_tam = (len(subset) / total) * 100
        stag_moy = safe_mean(r.nb_stagiaires for r in subset)
        livr_moy = safe_mean(r.livrables for r in subset)
        pct_ge5_seg = (sum(1 for r in subset if r.livrables >= 5) / len(subset)) * 100
        rows_table4.append(
            [
                f"{seg_id} ({label})",
                format_number(len(subset)),
                format_percent(pct_tam),
                format_number(stag_moy, 1),
                format_number(livr_moy, 1),
                format_percent(pct_ge5_seg),
            ]
        )
    rows_table4.append(
        [
            "TOTAL",
            format_number(total),
            "100%",
            format_number(safe_mean(r.nb_stagiaires for r in records), 1),
            format_number(safe_mean(r.livrables for r in records), 1),
            format_percent(pct_ge5),
        ]
    )

    # Table 5 - Production régionale
    rows_table5: List[List[str]] = []
    region_groups: Dict[int, List[Record]] = defaultdict(list)
    for r in records:
        region_groups[r.code_region or 0].append(r)
    ranked_regions = sorted(
        region_groups.items(),
        key=lambda item: safe_mean(rec.livrables for rec in item[1]) or 0,
        reverse=True,
    )
    for rank, (code, recs) in enumerate(ranked_regions, start=1):
        livr_moy = safe_mean(r.livrables for r in recs)
        pct_ge5_region = (sum(1 for r in recs if r.livrables >= 5) / len(recs)) * 100
        pct_ge10_region = (sum(1 for r in recs if r.livrables >= 10) / len(recs)) * 100
        rows_table5.append(
            [
                str(rank),
                REGION_NAMES.get(code, "Autres DOM-TOM"),
                format_number(len(recs)),
                format_number(livr_moy, 1),
                format_percent(pct_ge5_region),
                format_percent(pct_ge10_region),
            ]
        )
    rows_table5.append(
        [
            "TOTAL",
            "France",
            format_number(total),
            format_number(safe_mean(r.livrables for r in records), 1),
            format_percent(pct_ge5),
            format_percent((sum(1 for r in records if r.livrables >= 10) / total) * 100),
        ]
    )

    # Table 6 - Power users
    power_users = [r for r in records if r.livrables >= 15]
    power_pct = (len(power_users) / total) * 100 if power_users else 0
    avg_eff_total = safe_mean(r.effectif for r in records)
    avg_stag_total = safe_mean(r.nb_stagiaires for r in records)
    avg_livr_total = safe_mean(r.livrables for r in records)
    if power_users:
        avg_eff_power = safe_mean(r.effectif for r in power_users)
        avg_stag_power = safe_mean(r.nb_stagiaires for r in power_users)
        top_region_code, top_region_count = Counter(r.code_region for r in power_users).most_common(1)[0]
        top_region_share = top_region_count / len(power_users) * 100
    else:
        avg_eff_power = avg_stag_power = None
        top_region_code = None
        top_region_share = 0
    rows_table6 = [
        [
            "Nb OF",
            format_number(len(power_users)),
            format_percent(power_pct),
            "-",
        ],
        [
            "Effectif moy",
            format_number(avg_eff_power, 1),
            "-",
            format_number((avg_eff_power or 0) - (avg_eff_total or 0), 1),
        ],
        [
            "Stagiaires/an moy",
            format_number(avg_stag_power, 1),
            "-",
            format_number((avg_stag_power or 0) - (avg_stag_total or 0), 1),
        ],
        [
            "Livrables/mois moy",
            format_number(safe_mean(r.livrables for r in power_users), 1),
            "-",
            format_number((safe_mean(r.livrables for r in power_users) or 0) - (avg_livr_total or 0), 1),
        ],
        [
            "Région dominante",
            REGION_NAMES.get(top_region_code, "N/A") if power_users else "N/A",
            format_percent(top_region_share),
            "-",
        ],
        [
            "Spé dominante",
            "N/A",
            "-",
            "-",
        ],
    ]

    # Table 7 - Sous-productifs
    under_users = [r for r in records if r.livrables < 3]
    under_pct = (len(under_users) / total) * 100 if under_users else 0
    if under_users:
        avg_eff_under = safe_mean(r.effectif for r in under_users)
        avg_stag_under = safe_mean(r.nb_stagiaires for r in under_users)
        top_region_code_under, top_region_count_under = Counter(r.code_region for r in under_users).most_common(1)[0]
        top_region_share_under = top_region_count_under / len(under_users) * 100
    else:
        avg_eff_under = avg_stag_under = None
        top_region_code_under = None
        top_region_share_under = 0
    rows_table7 = [
        [
            "Nb OF",
            format_number(len(under_users)),
            format_percent(under_pct),
            "-",
        ],
        [
            "Effectif moy",
            format_number(avg_eff_under, 1),
            "-",
            format_number((avg_eff_under or 0) - (avg_eff_total or 0), 1),
        ],
        [
            "Stagiaires/an moy",
            format_number(avg_stag_under, 1),
            "-",
            format_number((avg_stag_under or 0) - (avg_stag_total or 0), 1),
        ],
        [
            "Livrables/mois moy",
            format_number(safe_mean(r.livrables for r in under_users), 1),
            "-",
            format_number((safe_mean(r.livrables for r in under_users) or 0) - (avg_livr_total or 0), 1),
        ],
        [
            "Région dominante",
            REGION_NAMES.get(top_region_code_under, "N/A") if under_users else "N/A",
            format_percent(top_region_share_under),
            "-",
        ],
        [
            "Spé dominante",
            "N/A",
            "-",
            "-",
        ],
    ]

    # Export CSVs
    csv_power_path = os.path.join(OUTPUT_DIR, "prompt09_power_users.csv")
    csv_under_path = os.path.join(OUTPUT_DIR, "prompt09_sous_productifs.csv")
    with open(csv_power_path, "w", encoding="utf-8") as f:
        f.write("denomination;effectif;nb_stagiaires;livrables;code_region\n")
        for r in power_users:
            f.write(
                f"{r.denomination};{r.effectif or ''};{r.nb_stagiaires or ''};{r.livrables or ''};{r.code_region or ''}\n"
            )
    with open(csv_under_path, "w", encoding="utf-8") as f:
        f.write("denomination;effectif;nb_stagiaires;livrables;code_region\n")
        for r in under_users:
            f.write(
                f"{r.denomination};{r.effectif or ''};{r.nb_stagiaires or ''};{r.livrables or ''};{r.code_region or ''}\n"
            )

    # Write markdown tables
    markdown_lines: List[str] = []
    write_markdown_table(
        markdown_lines,
        "### Tableau 1 : Distribution production",
        ["Tranche", "OF", "% TAM", "Stag/mois moy", "Effectif moy", "Cumul %"],
        rows_table1,
    )
    write_markdown_table(
        markdown_lines,
        "### Tableau 2 : Test hypothèse",
        ["Métrique", "Hypothèse Doc 1", "Calculé", "Écart", "Verdict"],
        rows_table2,
    )
    write_markdown_table(
        markdown_lines,
        "### Tableau 3 : Production moyenne par effectif",
        ["Effectif", "OF", "Livr moy", "Livr médian", "% ≥5", "% ≥10"],
        rows_table3,
    )
    write_markdown_table(
        markdown_lines,
        "### Tableau 4 : Production par segment d'activité",
        ["Segment", "OF", "% TAM", "Stag/an moy", "Livr moy", "% ≥5"],
        rows_table4,
    )
    write_markdown_table(
        markdown_lines,
        "### Tableau 5 : Production régionale",
        ["Rang", "Région", "OF", "Livr moy", "% ≥5", "% ≥10"],
        rows_table5,
    )
    write_markdown_table(
        markdown_lines,
        "### Tableau 6 : Power users (≥15 livr/mois)",
        ["Caractéristique", "Valeur", "% TAM", "vs Moyenne"],
        rows_table6,
    )
    write_markdown_table(
        markdown_lines,
        "### Tableau 7 : Sous-productifs (<3 livr/mois)",
        ["Caractéristique", "Valeur", "% TAM", "vs Moyenne"],
        rows_table7,
    )

    markdown_path = os.path.join(OUTPUT_DIR, "prompt09_tables.md")
    with open(markdown_path, "w", encoding="utf-8") as f:
        f.write("\n".join(markdown_lines))

    summary = {
        "total": total,
        "pct_ge5": pct_ge5,
        "count_ge5": count_ge5,
        "diff_pp": diff_pp,
        "verdict": verdict,
        "verdict_label": verdict_label,
        "avg_livr": safe_mean(r.livrables for r in records),
        "avg_livr_power": safe_mean(r.livrables for r in power_users),
        "avg_livr_under": safe_mean(r.livrables for r in under_users),
        "power_count": len(power_users),
        "under_count": len(under_users),
        "power_pct": power_pct,
        "under_pct": under_pct,
        "csv_power": csv_power_path,
        "csv_under": csv_under_path,
        "markdown": markdown_path,
    }
    return summary


def main():
    ensure_output_dir()
    all_records = load_records()
    tam_records = filter_tam(all_records)
    summary = compute_tables(tam_records)
    print(f"TAM records: {summary['total']}")
    print(f"% ≥5 livr/mois: {summary['pct_ge5']:.2f}%")
    print(f"Verdit: {summary['verdict']} {summary['verdict_label']}")
    print(f"Tables written to {summary['markdown']}")
    print(f"Power users CSV: {summary['csv_power']}")
    print(f"Sous-productifs CSV: {summary['csv_under']}")


if __name__ == "__main__":
    main()

import csv
import math
import os
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
OUTPUT_MARKDOWN = os.path.join(OUTPUT_DIR, "prompt13_maturite_qualiopi.md")
OUTPUT_CSV_REGIONS = os.path.join(OUTPUT_DIR, "prompt13_regions_maturite.csv")

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

COL_REGION = 8
COL_CERT_ACTIONS = 9
COL_DATE_DERNIERE_DECL = 18
COL_NB_STAGIAIRES = 27
COL_EFFECTIF_FORMATEURS = 29
COL_CODE_POSTAL = 6

REGION_NAMES: Dict[int, str] = {
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


class Record(Tuple):
    region_code: Optional[int]
    is_certified: bool
    nb_stagiaires: Optional[float]
    effectif_formateurs: Optional[float]
    code_postal: Optional[str]
    annee_derniere_declaration: Optional[int]


def ensure_output_dir() -> None:
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


def get_cell_value(cell: ET.Element, shared_strings: List[str]) -> Optional[str]:
    cell_type = cell.attrib.get("t")
    if cell_type == "s":
        value = cell.find(NS + "v")
        if value is None or value.text is None:
            return None
        return shared_strings[int(value.text)]
    if cell_type == "inlineStr":
        inline = cell.find(NS + "is")
        if inline is None:
            return None
        return "".join(t_el.text or "" for t_el in inline.findall('.//' + NS + 't'))
    value = cell.find(NS + "v")
    if value is None:
        return None
    return value.text


def parse_int(value: Optional[str]) -> Optional[int]:
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


def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace("\u00a0", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_bool(value: Optional[str]) -> bool:
    if value is None:
        return False
    text = str(value).strip().lower()
    if not text:
        return False
    return text in {"1", "true", "vrai", "oui", "o", "y", "yes"}


def parse_excel_year(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    # Try Excel serial number
    try:
        serial = float(text)
        if math.isnan(serial) or serial <= 0:
            raise ValueError
        base = date(1899, 12, 30)
        day_count = int(round(serial))
        dt = base + timedelta(days=day_count)
        return dt.year
    except ValueError:
        pass
    # Try ISO formats
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).year
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text).year
    except ValueError:
        return None


def extract_departement(code_postal: Optional[str]) -> Optional[str]:
    if not code_postal:
        return None
    cp = str(code_postal).strip()
    if not cp:
        return None
    digits = "".join(ch for ch in cp if ch.isdigit())
    if len(digits) >= 3 and digits.startswith(("97", "98")):
        return digits[:3]
    if len(digits) >= 2:
        return digits[:2]
    return None


def load_records() -> List[Dict[str, Optional[object]]]:
    records: List[Dict[str, Optional[object]]] = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            for event, elem in ET.iterparse(f, events=("end",)):
                if elem.tag != NS + "row":
                    continue
                row_index_text = elem.attrib.get("r")
                if row_index_text == "1":
                    elem.clear()
                    continue
                values: Dict[int, str] = {}
                for cell in elem.findall(NS + "c"):
                    ref = cell.attrib.get("r")
                    if not ref:
                        continue
                    col_idx = column_ref_to_index(ref)
                    val = get_cell_value(cell, shared_strings)
                    if val is not None:
                        values[col_idx] = val
                region_code = parse_int(values.get(COL_REGION))
                is_certified = parse_bool(values.get(COL_CERT_ACTIONS))
                nb_stagiaires = parse_float(values.get(COL_NB_STAGIAIRES))
                effectif_formateurs = parse_float(values.get(COL_EFFECTIF_FORMATEURS))
                code_postal = values.get(COL_CODE_POSTAL)
                annee_decl = parse_excel_year(values.get(COL_DATE_DERNIERE_DECL))
                records.append(
                    {
                        "region_code": region_code,
                        "is_certified": is_certified,
                        "nb_stagiaires": nb_stagiaires,
                        "effectif_formateurs": effectif_formateurs,
                        "code_postal": code_postal,
                        "annee_decl": annee_decl,
                    }
                )
                elem.clear()
    return records


def region_label(code: Optional[int]) -> str:
    if code is None:
        return "Non renseigné"
    return REGION_NAMES.get(code, f"Autre ({code})")


def format_int(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def format_float(value: float, decimals: int = 1) -> str:
    return f"{value:,.{decimals}f}".replace(",", " ")


def format_pct(value: float, decimals: int = 1) -> str:
    return f"{value * 100:.{decimals}f}%"


def write_markdown_table(headers: List[str], rows: List[List[str]]) -> List[str]:
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    lines.extend("| " + " | ".join(str(cell) for cell in row) + " |" for row in rows)
    return lines


def compute_region_stats(records: List[Dict[str, Optional[object]]]) -> Dict[str, Dict[str, float]]:
    stats: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for rec in records:
        region = region_label(rec["region_code"])
        stats[region]["total"] += 1
        if rec["is_certified"]:
            stats[region]["certified"] += 1
        if rec["nb_stagiaires"] is not None:
            stats[region]["sum_stagiaires"] += float(rec["nb_stagiaires"])
        if rec["is_certified"] and rec["nb_stagiaires"] is not None:
            stats[region]["sum_stagiaires_cert"] += float(rec["nb_stagiaires"])
            stats[region]["count_cert"] += 1
        if (not rec["is_certified"]) and rec["nb_stagiaires"] is not None:
            stats[region]["sum_stagiaires_non"] += float(rec["nb_stagiaires"])
            stats[region]["count_non"] += 1
    return stats


def safe_div(num: float, denom: float) -> float:
    return num / denom if denom else 0.0


def main() -> None:
    ensure_output_dir()
    records = load_records()

    total_of = len(records)
    total_cert = sum(1 for r in records if r["is_certified"])
    national_rate = safe_div(total_cert, total_of)

    # Region stats for all sizes
    region_stats_all: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for rec in records:
        region = region_label(rec["region_code"])
        region_data = region_stats_all[region]
        region_data["total"] += 1
        if rec["is_certified"]:
            region_data["certified"] += 1

    table1_rows: List[List[str]] = []
    ranked_regions = sorted(
        region_stats_all.items(),
        key=lambda item: safe_div(item[1]["certified"], item[1]["total"]),
        reverse=True,
    )
    for rank, (region, data) in enumerate(ranked_regions, start=1):
        total_region = int(data["total"])
        certified_region = int(data["certified"])
        rate_region = safe_div(certified_region, total_region)
        table1_rows.append(
            [
                region,
                format_int(total_region),
                format_int(certified_region),
                format_pct(rate_region),
                str(rank),
            ]
        )

    table1_rows.append(
        [
            "FRANCE",
            format_int(total_of),
            format_int(total_cert),
            format_pct(national_rate),
            "-",
        ]
    )

    # Filter 3-10 formateurs
    target_records = [
        r
        for r in records
        if r["effectif_formateurs"] is not None
        and 3 <= r["effectif_formateurs"] <= 10
    ]

    total_target = len(target_records)
    total_target_cert = sum(1 for r in target_records if r["is_certified"])
    rate_target = safe_div(total_target_cert, total_target)

    region_stats_target: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for rec in target_records:
        region = region_label(rec["region_code"])
        data = region_stats_target[region]
        data["total"] += 1
        if rec["is_certified"]:
            data["certified"] += 1
        if rec["nb_stagiaires"] is not None:
            data["sum_stagiaires"] += float(rec["nb_stagiaires"])
        if rec["nb_stagiaires"] and rec["nb_stagiaires"] > 0:
            data["active_count"] += 1
        if rec["is_certified"] and rec["nb_stagiaires"] is not None:
            data.setdefault("sum_stag_cert", 0.0)
            data["sum_stag_cert"] += float(rec["nb_stagiaires"])
            data.setdefault("count_cert", 0.0)
            data["count_cert"] += 1
        if (not rec["is_certified"]) and rec["nb_stagiaires"] is not None:
            data.setdefault("sum_stag_non", 0.0)
            data["sum_stag_non"] += float(rec["nb_stagiaires"])
            data.setdefault("count_non", 0.0)
            data["count_non"] += 1

    table2_rows: List[List[str]] = []
    ranked_target = sorted(
        region_stats_target.items(),
        key=lambda item: safe_div(item[1]["certified"], item[1]["total"]),
        reverse=True,
    )
    for region, data in ranked_target:
        total_region = int(data["total"])
        certified_region = int(data["certified"])
        rate_region = safe_div(certified_region, total_region)
        diff_pp = (rate_region - rate_target) * 100
        table2_rows.append(
            [
                region,
                format_int(total_region),
                format_int(certified_region),
                format_pct(rate_region),
                f"{diff_pp:+.1f} pp",
            ]
        )

    table2_rows.append(
        [
            "FRANCE",
            format_int(total_target),
            format_int(total_target_cert),
            format_pct(rate_target),
            "-",
        ]
    )

    # Certification vs activity (3-10)
    certified_group = [r for r in target_records if r["is_certified"]]
    non_certified_group = [r for r in target_records if not r["is_certified"]]

    def avg_stag(group: Iterable[Dict[str, Optional[object]]]) -> float:
        total = 0.0
        count = 0
        for rec in group:
            if rec["nb_stagiaires"] is not None:
                total += float(rec["nb_stagiaires"])
                count += 1
        return total / count if count else 0.0

    def share_active(group: Iterable[Dict[str, Optional[object]]]) -> float:
        total = 0
        active = 0
        for rec in group:
            total += 1
            if rec["nb_stagiaires"] and rec["nb_stagiaires"] > 0:
                active += 1
        return active / total if total else 0.0

    def avg_effectif(group: Iterable[Dict[str, Optional[object]]]) -> float:
        total = 0.0
        count = 0
        for rec in group:
            if rec["effectif_formateurs"] is not None:
                total += float(rec["effectif_formateurs"])
                count += 1
        return total / count if count else 0.0

    avg_stag_cert = avg_stag(certified_group)
    avg_stag_non = avg_stag(non_certified_group)
    stag_diff_pct = safe_div(avg_stag_cert - avg_stag_non, avg_stag_non)

    active_cert = share_active(certified_group)
    active_non = share_active(non_certified_group)
    active_diff_pp = (active_cert - active_non) * 100

    avg_eff_cert = avg_effectif(certified_group)
    avg_eff_non = avg_effectif(non_certified_group)
    eff_diff_pct = safe_div(avg_eff_cert - avg_eff_non, avg_eff_non)

    table3_rows = [
        [
            "Nombre OF",
            format_int(len(certified_group)),
            format_int(len(non_certified_group)),
            "-",
        ],
        [
            "Stagiaires moyens",
            format_float(avg_stag_cert, 1),
            format_float(avg_stag_non, 1),
            f"{stag_diff_pct * 100:+.1f}%",
        ],
        [
            "% d'OF actifs (>0 stag.)",
            format_pct(active_cert, 1),
            format_pct(active_non, 1),
            f"{active_diff_pp:+.1f} pp",
        ],
        [
            "Effectif formateurs moyen",
            format_float(avg_eff_cert, 2),
            format_float(avg_eff_non, 2),
            f"{eff_diff_pct * 100:+.1f}%",
        ],
    ]

    # Dynamics by year (3-10 subset, using last declaration as proxy)
    year_counts: Dict[int, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for rec in target_records:
        if not rec["is_certified"]:
            continue
        year = rec["annee_decl"]
        if year is None:
            continue
        year_counts[year]["new_certified"] += 1

    table4_rows: List[List[str]] = []
    cumulative = 0
    for year in sorted(y for y in year_counts if 2023 <= y <= 2025):
        new_cert = int(year_counts[year]["new_certified"])
        cumulative += new_cert
        prev_total = cumulative - new_cert
        growth = safe_div(cumulative - prev_total, prev_total) if prev_total else 0.0
        table4_rows.append(
            [
                str(year),
                format_int(new_cert),
                format_int(cumulative),
                f"{growth * 100:+.1f}%" if prev_total else "-",
            ]
        )

    if not table4_rows:
        table4_rows.append(["2023-2025", "0", "0", "-"])

    # Regions with potential (3-10 subset)
    national_avg_stag = avg_stag(target_records)

    potential_rows: List[Tuple[float, List[str]]] = []
    for region, data in region_stats_target.items():
        total_region = int(data["total"])
        if total_region == 0:
            continue
        certified_region = int(data["certified"])
        rate_region = safe_div(certified_region, total_region)
        avg_stag_region = safe_div(data.get("sum_stagiaires", 0.0), data.get("count_cert", 0.0) + data.get("count_non", 0.0))
        index = (1 - rate_region) * safe_div(avg_stag_region, national_avg_stag) if national_avg_stag else 0.0
        if rate_region < 0.5:
            opportunity = "HAUTE" if avg_stag_region > national_avg_stag and index >= 1.2 else "MOYENNE"
            row = [
                region,
                format_pct(rate_region),
                format_float(avg_stag_region, 1),
                format_float(index, 2),
                opportunity,
            ]
            potential_rows.append((index, row))

    potential_rows.sort(key=lambda x: x[0], reverse=True)
    table5_rows = [row for _, row in potential_rows]
    if not table5_rows:
        table5_rows.append(["Aucune région", "-", "-", "-", "-"])

    # Department maturity (3-10 subset)
    dept_stats: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for rec in target_records:
        dept = extract_departement(rec["code_postal"])
        if not dept:
            continue
        data = dept_stats[dept]
        data["total"] += 1
        if rec["is_certified"]:
            data["certified"] += 1

    dept_rows: List[List[str]] = []
    for dept, data in sorted(dept_stats.items(), key=lambda x: x[1]["total"], reverse=True)[:30]:
        total_dept = int(data["total"])
        certified_dept = int(data["certified"])
        rate = safe_div(certified_dept, total_dept)
        if rate > 0.8:
            maturity = "Très élevée"
        elif rate > 0.6:
            maturity = "Élevée"
        elif rate >= 0.4:
            maturity = "Moyenne"
        else:
            maturity = "Faible"
        dept_rows.append(
            [
                dept,
                format_int(total_dept),
                format_pct(rate),
                maturity,
            ]
        )

    # Prepare CSV for regional maturity (3-10 subset)
    with open(OUTPUT_CSV_REGIONS, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "region",
            "total_of",
            "certified_of",
            "certification_rate",
            "avg_stagiaires",
        ])
        for region, data in sorted(region_stats_target.items()):
            total_region = int(data["total"])
            certified_region = int(data["certified"])
            avg_stag_region = safe_div(
                data.get("sum_stagiaires", 0.0),
                data.get("count_cert", 0.0) + data.get("count_non", 0.0),
            )
            writer.writerow(
                [
                    region,
                    total_region,
                    certified_region,
                    f"{safe_div(certified_region, total_region):.4f}" if total_region else "0.0000",
                    f"{avg_stag_region:.2f}",
                ]
            )

    # Summary text
    non_certified_target = total_target - total_target_cert
    share_non_certified = safe_div(non_certified_target, total_target)

    top_regions = ranked_target[:3]
    top_summary_lines = [
        f"{idx + 1}. {region} : {format_pct(safe_div(data['certified'], data['total']))} (vs {format_pct(rate_target)} national)"
        for idx, (region, data) in enumerate(top_regions)
    ]

    top_potential = table5_rows[:3]
    potential_summary_lines = [f"{row[0]} : {row[1]} (potentiel {row[4]})" for row in top_potential]

    summary_lines = [
        "MATURITÉ QUALIOPI :",
        "",
        "National (toutes tailles) :",
        f"- Taux certification : {format_pct(national_rate)}",
        f"- {format_int(total_cert)} OF certifiés / {format_int(total_of)}",
        "",
        "Cible 3-10 formateurs :",
        f"- Taux certification : {format_pct(rate_target)}",
        f"- {format_int(total_target_cert)} OF certifiés / {format_int(total_target)}",
        "",
        "Top 3 régions matures :",
        *[f"- {line}" for line in top_summary_lines],
        "",
        "Régions à sensibiliser :",
        *[f"- {line}" for line in potential_summary_lines],
        "",
        "Impact certification :",
        f"- Certifiés : {format_float(avg_stag_cert, 1)} stagiaires en moyenne ({stag_diff_pct * 100:+.1f}% vs non certifiés)",
        f"- Certifiés : {format_pct(active_cert)} actifs vs {format_pct(active_non)} non certifiés",
        "",
        "Opportunité :",
        f"- {format_pct(share_non_certified)} des 3-10 non certifiés = {format_int(non_certified_target)} OF",
        "- Messaging \"Préparation Qualiopi avec Qalia\"",
    ]

    # Assemble markdown document
    lines: List[str] = []
    lines.extend(summary_lines)
    lines.append("")

    lines.append("Tableau 1 : Certification régionale (toutes tailles)")
    lines.extend(write_markdown_table([
        "Région",
        "OF total",
        "OF certifiés",
        "Taux certif",
        "Rang",
    ], table1_rows))
    lines.append("")

    lines.append("Tableau 2 : Certification cible 3-10")
    lines.extend(write_markdown_table([
        "Région",
        "OF 3-10",
        "OF certifiés",
        "Taux certif",
        "vs National",
    ], table2_rows))
    lines.append("")

    lines.append("Tableau 3 : Impact certification sur activité")
    lines.extend(write_markdown_table([
        "Métrique",
        "Certifiés",
        "Non certifiés",
        "Écart",
    ], table3_rows))
    lines.append("")

    lines.append("Tableau 4 : Dynamique certification 2023-2025")
    lines.extend(write_markdown_table([
        "Année",
        "Nouveaux certifiés",
        "Total certifiés",
        "Taux croissance",
    ], table4_rows))
    lines.append("")

    lines.append("Tableau 5 : Régions opportunité sensibilisation")
    lines.extend(write_markdown_table([
        "Région",
        "Taux certif",
        "Stag. moyen",
        "Index potentiel",
        "Opportunité",
    ], table5_rows))
    lines.append("")

    lines.append("Tableau 6 : Départements maturité Qualiopi")
    lines.extend(write_markdown_table([
        "Dept",
        "OF 3-10",
        "Taux certif",
        "Maturité",
    ], dept_rows))
    lines.append("")

    with open(OUTPUT_MARKDOWN, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()

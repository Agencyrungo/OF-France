import csv
import math
import os
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
OUTPUT_MARKDOWN = os.path.join(OUTPUT_DIR, "prompt14_evolution_temporelle.md")
OUTPUT_CSV_REGIONS = os.path.join(OUTPUT_DIR, "prompt14_evolution_regions.csv")

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

COL_NUM_DECL = 0
COL_PREV_DECL = 1
COL_REGION = 8
COL_CERT_ACTIONS = 9
COL_DATE_DERNIERE_DECL = 18
COL_DEBUT_EXERCICE = 19
COL_FIN_EXERCICE = 20
COL_NB_STAGIAIRES = 27
COL_EFFECTIF = 29

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

MAINLAND_REGION_ORDER = [
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
    numero: Optional[str]
    prev_numero: Optional[str]
    region_code: Optional[int]
    is_certified: bool
    year_last_decl: Optional[int]
    start_date: Optional[date]
    end_date: Optional[date]
    nb_stagiaires: Optional[float]
    effectif: Optional[float]


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def column_ref_to_index(ref: str) -> int:
    letters = "".join(ch for ch in ref if ch.isalpha())
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


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


def parse_excel_date(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        serial = float(text)
        if math.isnan(serial) or serial <= 0:
            raise ValueError
        base = date(1899, 12, 30)
        day_count = int(round(serial))
        return base + timedelta(days=day_count)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def parse_excel_year(value: Optional[str]) -> Optional[int]:
    parsed_date = parse_excel_date(value)
    if parsed_date is None:
        return None
    return parsed_date.year


def load_records() -> List[Record]:
    records: List[Record] = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            for event, elem in ET.iterparse(f, events=("end",)):
                if elem.tag != NS + "row":
                    continue
                if elem.attrib.get("r") == "1":
                    elem.clear()
                    continue
                values: Dict[int, str] = {}
                for cell in elem.findall(NS + "c"):
                    ref = cell.attrib.get("r")
                    if not ref:
                        continue
                    col_idx = column_ref_to_index(ref)
                    if col_idx not in {
                        COL_NUM_DECL,
                        COL_PREV_DECL,
                        COL_REGION,
                        COL_CERT_ACTIONS,
                        COL_DATE_DERNIERE_DECL,
                        COL_DEBUT_EXERCICE,
                        COL_FIN_EXERCICE,
                        COL_NB_STAGIAIRES,
                        COL_EFFECTIF,
                    }:
                        continue
                    val = get_cell_value(cell, shared_strings)
                    if val is not None:
                        values[col_idx] = val
                record = Record(
                    numero=str(values.get(COL_NUM_DECL, "")) or None,
                    prev_numero=str(values.get(COL_PREV_DECL, "")) or None,
                    region_code=parse_int(values.get(COL_REGION)),
                    is_certified=parse_bool(values.get(COL_CERT_ACTIONS)),
                    year_last_decl=parse_excel_year(values.get(COL_DATE_DERNIERE_DECL)),
                    start_date=parse_excel_date(values.get(COL_DEBUT_EXERCICE)),
                    end_date=parse_excel_date(values.get(COL_FIN_EXERCICE)),
                    nb_stagiaires=parse_float(values.get(COL_NB_STAGIAIRES)),
                    effectif=parse_float(values.get(COL_EFFECTIF)),
                )
                records.append(record)
                elem.clear()
    return records


def format_int(value: Optional[int]) -> str:
    if value is None:
        return "-"
    return f"{value:,}".replace(",", " ")


def format_float(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:,.{decimals}f}".replace(",", " ")


def format_pct(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.{decimals}f}%"


def safe_div(num: float, denom: float) -> Optional[float]:
    if denom == 0:
        return None
    return num / denom


def detect_new(prev_numero: Optional[str], numero: Optional[str]) -> bool:
    prev_text = (prev_numero or "").strip()
    num_text = (numero or "").strip()
    if not prev_text:
        return True
    return prev_text == num_text


def compute_declarations_table(records: List[Record]) -> List[List[str]]:
    year_counts: Counter[int] = Counter()
    new_counts: Counter[int] = Counter()
    for rec in records:
        if rec.year_last_decl is None:
            continue
        year_counts[rec.year_last_decl] += 1
        if detect_new(rec.prev_numero, rec.numero):
            new_counts[rec.year_last_decl] += 1
    total_count = sum(year_counts.values())
    pre_2023_years = [year for year in year_counts if year <= 2022]
    pre_2023_total = sum(year_counts[year] for year in pre_2023_years)
    pre_2023_new = sum(new_counts[year] for year in pre_2023_years)
    rows: List[List[str]] = []
    previous_count: Optional[int] = None

    if pre_2023_total:
        pct = pre_2023_total / total_count if total_count else 0
        rows.append(
            [
                "2016-2022",
                format_int(pre_2023_total),
                format_pct(pct, 1),
                format_int(pre_2023_new),
                "--",
            ]
        )
        previous_count = pre_2023_total

    for year in sorted(year for year in year_counts if year >= 2023):
        count = year_counts[year]
        pct = count / total_count if total_count else 0
        new_count = new_counts.get(year, 0)
        if previous_count and previous_count > 0:
            growth = (count - previous_count) / previous_count
            growth_text = f"{growth * 100:+.1f}%"
        else:
            growth_text = "--"
        rows.append(
            [
                str(year),
                format_int(count),
                format_pct(pct, 1),
                format_int(new_count),
                growth_text,
            ]
        )
        previous_count = count

    rows.append([
        "TOTAL",
        format_int(total_count),
        "100.0%",
        format_int(sum(new_counts.values())),
        "--",
    ])
    return rows


def filter_tam_base(records: Iterable[Record]) -> List[Record]:
    result: List[Record] = []
    for rec in records:
        if rec.effectif is None:
            continue
        if rec.nb_stagiaires is None:
            continue
        if rec.nb_stagiaires <= 0:
            continue
        if not (3 <= rec.effectif <= 10):
            continue
        result.append(rec)
    return result


def filter_tam_qualifie(records: Iterable[Record]) -> List[Record]:
    return [rec for rec in filter_tam_base(records) if rec.is_certified]


def compute_tam_table(records: List[Record]) -> List[List[str]]:
    tam_records = filter_tam_qualifie(records)
    year_counts: Counter[int] = Counter()
    for rec in tam_records:
        if rec.year_last_decl is None:
            continue
        year_counts[rec.year_last_decl] += 1
    total = sum(year_counts.values())
    rows: List[List[str]] = []
    cumulative = 0.0
    interpretation = {2023: "Anciens", 2024: "Récents", 2025: "Nouveaux"}
    for year in sorted(year_counts):
        if year < 2023:
            continue
        count = year_counts[year]
        share = count / total if total else 0
        cumulative += share
        rows.append(
            [
                str(year),
                format_int(count),
                format_pct(share, 1),
                format_pct(cumulative, 1),
                interpretation.get(year, ""),
            ]
        )
    rows.append([
        "TOTAL",
        format_int(total),
        "100.0%",
        "100.0%",
        "",
    ])
    return rows


def month_name(month: int) -> str:
    names = {
        1: "Janvier",
        2: "Février",
        3: "Mars",
        4: "Avril",
        5: "Mai",
        6: "Juin",
        7: "Juillet",
        8: "Août",
        9: "Septembre",
        10: "Octobre",
        11: "Novembre",
        12: "Décembre",
    }
    return names.get(month, f"M{month}")


def compute_saison_table(records: List[Record]) -> List[List[str]]:
    tam_records = filter_tam_base(records)
    month_counts: Counter[int] = Counter()
    for rec in tam_records:
        if rec.start_date is None:
            continue
        month_counts[rec.start_date.month] += 1
    total = sum(month_counts.values())
    focus_months = [1, 4, 7, 10]
    interpretation = {
        1: "Calendaire",
        4: "Fiscal",
        7: "Rentrée estivale",
        10: "Clôture automnale",
    }
    rows: List[List[str]] = []
    focus_total = 0
    for month in focus_months:
        count = month_counts.get(month, 0)
        focus_total += count
        share = count / total if total else 0
        rows.append(
            [
                month_name(month),
                format_int(count),
                format_pct(share, 1),
                interpretation.get(month, ""),
            ]
        )
    other_count = total - focus_total
    other_share = other_count / total if total else 0
    rows.append([
        "Autres",
        format_int(other_count),
        format_pct(other_share, 1),
        "Divers",
    ])
    rows.append([
        "TOTAL",
        format_int(total),
        "100.0%",
        "",
    ])
    return rows


def compute_duration_months(start: date, end: date) -> Optional[int]:
    if start is None or end is None:
        return None
    delta = end - start
    if delta.days < 0:
        return None
    months = round(delta.days / 30.4375)
    return int(months)


def compute_duration_table(records: List[Record]) -> List[List[str]]:
    tam_records = filter_tam_base(records)
    counts: Counter[str] = Counter()
    total = 0
    for rec in tam_records:
        duration = compute_duration_months(rec.start_date, rec.end_date)
        if duration is None:
            continue
        total += 1
        if duration == 12:
            key = "12"
        elif duration == 18:
            key = "18"
        elif duration == 24:
            key = "24"
        else:
            key = "Autre"
        counts[key] += 1
    rows: List[List[str]] = []
    for label in ["12", "18", "24", "Autre"]:
        count = counts.get(label, 0)
        share = count / total if total else 0
        label_text = "Standard" if label == "12" else ("1.5 an" if label == "18" else ("Bi-annuel" if label == "24" else "Non standard"))
        rows.append(
            [
                "12" if label == "12" else label,
                format_int(count),
                format_pct(share, 1),
                label_text,
            ]
        )
    rows.append([
        "TOTAL",
        format_int(total),
        "100.0%",
        "",
    ])
    return rows


def region_label(code: Optional[int]) -> str:
    if code is None:
        return "Non renseigné"
    return REGION_NAMES.get(code, f"Autre ({code})")


def compute_region_growth(records: List[Record]) -> List[List[str]]:
    tam_records = filter_tam_base(records)
    year_range = [2023, 2024, 2025]
    region_year_counts: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for rec in tam_records:
        if rec.year_last_decl not in year_range:
            continue
        region = region_label(rec.region_code)
        region_year_counts[region][rec.year_last_decl] += 1
    rows: List[List[str]] = []
    ordered_regions = []
    for code in MAINLAND_REGION_ORDER:
        name = region_label(code)
        if name in region_year_counts:
            ordered_regions.append(name)
    remaining = [region for region in region_year_counts if region not in ordered_regions]
    ordered_regions.extend(sorted(remaining))
    for region in ordered_regions:
        counts = region_year_counts[region]
        values = [counts.get(year, 0) for year in year_range]
        base = values[0]
        latest = values[-1]
        if base > 0:
            growth = (latest - base) / base
            growth_text = f"{growth * 100:+.1f}%"
        elif latest > 0:
            growth_text = "+∞"
        else:
            growth_text = "0.0%"
        rows.append(
            [
                region,
                format_int(values[0]),
                format_int(values[1]),
                format_int(values[2]),
                growth_text,
            ]
        )
    rows.append([
        "TOTAL",
        format_int(sum(region_year_counts[region][2023] for region in region_year_counts)),
        format_int(sum(region_year_counts[region][2024] for region in region_year_counts)),
        format_int(sum(region_year_counts[region][2025] for region in region_year_counts)),
        "",
    ])
    return rows, region_year_counts


def write_region_csv(region_year_counts: Dict[str, Dict[int, int]]) -> None:
    year_range = [2023, 2024, 2025]
    with open(OUTPUT_CSV_REGIONS, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["region", "year", "of_tam"])
        for region, counts in sorted(region_year_counts.items()):
            for year in year_range:
                writer.writerow([region, year, counts.get(year, 0)])


def compute_segment_table(records: List[Record]) -> List[List[str]]:
    tam_records = filter_tam_base(records)
    segments = {
        "Nouveau (2025)": [rec for rec in tam_records if rec.year_last_decl == 2025],
        "Récent (2024)": [rec for rec in tam_records if rec.year_last_decl == 2024],
        "Ancien (≤2023)": [rec for rec in tam_records if rec.year_last_decl is not None and rec.year_last_decl <= 2023],
    }
    rows: List[List[str]] = []
    for label, group in segments.items():
        count = len(group)
        avg_stag = safe_div(sum(rec.nb_stagiaires or 0 for rec in group), sum(1 for rec in group if rec.nb_stagiaires is not None))
        avg_effectif = safe_div(sum(rec.effectif or 0 for rec in group), sum(1 for rec in group if rec.effectif is not None))
        cert_rate = safe_div(sum(1 for rec in group if rec.is_certified), count)
        rows.append(
            [
                label,
                format_int(count),
                format_float(avg_stag, 1),
                format_float(avg_effectif, 1),
                format_pct(cert_rate, 1) if cert_rate is not None else "-",
            ]
        )
    total_group = list(segments.values())
    all_records = [rec for group in total_group for rec in group]
    total_count = len(all_records)
    avg_stag_total = safe_div(sum(rec.nb_stagiaires or 0 for rec in all_records), sum(1 for rec in all_records if rec.nb_stagiaires is not None))
    avg_effectif_total = safe_div(sum(rec.effectif or 0 for rec in all_records), sum(1 for rec in all_records if rec.effectif is not None))
    cert_rate_total = safe_div(sum(1 for rec in all_records if rec.is_certified), total_count)
    rows.append(
        [
            "TOTAL",
            format_int(total_count),
            format_float(avg_stag_total, 1),
            format_float(avg_effectif_total, 1),
            format_pct(cert_rate_total, 1) if cert_rate_total is not None else "-",
        ]
    )
    return rows


def write_markdown(headers: List[str], rows: List[List[str]]) -> List[str]:
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    return lines


def build_summary(records: List[Record]) -> List[str]:
    decl_table = compute_declarations_table(records)
    tam_table = compute_tam_table(records)
    tam_rows = [row for row in tam_table if row[0].isdigit()]
    summary_lines = ["## Synthèse"]
    decl_2025 = next((row for row in decl_table if row[0] == "2025"), None)
    if decl_2025:
        summary_lines.append(
            "- Volume de déclarations concentré en 2025 : {} dossiers, soit {} du total.".format(
                decl_2025[1], decl_2025[2]
            )
        )
    if tam_rows:
        if len(tam_rows) >= 2:
            summary_lines.append(
                "- TAM qualifié en forte expansion : {} en {} → {} en {}.".format(
                    tam_rows[0][1], tam_rows[0][0], tam_rows[-1][1], tam_rows[-1][0]
                )
            )
        else:
            summary_lines.append(
                "- TAM qualifié concentré en {} : {} organismes certifiés actifs.".format(
                    tam_rows[0][0], tam_rows[0][1]
                )
            )
    saison_table = compute_saison_table(records)
    summary_lines.append(
        "- Ouvertures d'exercice dominées par {} ({}) avec une saisonnalité calendaire marquée.".format(
            saison_table[0][0], saison_table[0][2]
        )
    )
    duration_table = compute_duration_table(records)
    summary_lines.append(
        "- Durée standard 12 mois pour {} des TAM, confirmant un cycle annuel classique.".format(
            duration_table[0][2]
        )
    )
    summary_lines.append("")
    summary_lines.append("## Recommandations ciblage")
    summary_lines.append(
        "- Nouveaux OF 2025 : renforcer l'accompagnement Qualiopi et la structuration des process."
    )
    summary_lines.append(
        "- Anciens (≤2023) : capitaliser sur leur maturité pour proposer des offres d'optimisation."
    )
    summary_lines.append("")
    return summary_lines


def main() -> None:
    ensure_output_dir()
    records = load_records()

    decl_table = compute_declarations_table(records)
    tam_table = compute_tam_table(records)
    saison_table = compute_saison_table(records)
    duration_table = compute_duration_table(records)
    region_table, region_counts = compute_region_growth(records)
    segment_table = compute_segment_table(records)

    summary_lines = build_summary(records)

    lines: List[str] = ["# Prompt 14 – Évolution temporelle 2023-2025", ""]
    lines.extend(summary_lines)

    lines.append("## Tableau 1 – Déclarations par année")
    lines.extend(write_markdown(["Année", "Déclarations", "% total", "Nouveaux OF", "Croissance"], decl_table))

    lines.append("## Tableau 2 – TAM qualifié par année de déclaration")
    lines.extend(write_markdown(["Année", "OF TAM", "% TAM total", "Cumul", "Interprétation"], tam_table))

    lines.append("## Tableau 3 – Mois de début d'exercice (TAM)")
    lines.extend(write_markdown(["Mois", "OF", "%", "Interprétation"], saison_table))

    lines.append("## Tableau 4 – Durée des exercices (TAM)")
    lines.extend(write_markdown(["Durée (mois)", "OF", "%", "Interprétation"], duration_table))

    lines.append("## Tableau 5 – Dynamique régionale 2023-2025 (TAM)")
    lines.extend(write_markdown(["Région", "OF 2023", "OF 2024", "OF 2025", "Croissance"], region_table))

    lines.append("## Tableau 6 – Profil TAM par ancienneté")
    lines.extend(write_markdown(["Ancienneté", "OF TAM", "Stag. moyen", "Effectif moy", "Taux certif"], segment_table))

    with open(OUTPUT_MARKDOWN, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    write_region_csv(region_counts)


if __name__ == "__main__":
    main()

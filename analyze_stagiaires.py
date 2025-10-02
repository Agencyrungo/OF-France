import csv
import math
import os
import statistics
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

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

METRO_REGION_CODES = [11, 24, 27, 28, 32, 44, 52, 53, 75, 76, 84, 93, 94]


@dataclass
class Record:
    denomination: str
    nb_stagiaires: float
    effectif: Optional[int]
    qualiopi_actions: Optional[int]
    region_code: Optional[int]
    specialite: Optional[str]


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
        v = cell.find(NS + "v")
        if v is None or v.text is None:
            return None
        return shared_strings[int(v.text)]
    if cell_type == "inlineStr":
        is_elem = cell.find(NS + "is")
        if is_elem is None:
            return None
        return "".join(t.text or "" for t in is_elem.findall('.//' + NS + 't'))
    v = cell.find(NS + "v")
    if v is None:
        return None
    return v.text


def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return int(float(text))
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
                "nb_stagiaires": None,
                "effectif": None,
                "actions": None,
                "region": None,
                "specialite": None,
            }
            for event, elem in ET.iterparse(f, events=("end",)):
                if elem.tag != NS + "row":
                    continue
                row_idx = int(elem.attrib.get("r"))
                if row_idx == 1:
                    for cell in elem.findall(NS + "c"):
                        ref = cell.attrib.get("r")
                        if not ref:
                            continue
                        col_idx = column_ref_to_index(ref)
                        val = get_cell_value(cell, shared_strings)
                        if val is not None:
                            header_map[col_idx] = val
                    target_indices["denomination"] = next(
                        (idx for idx, name in header_map.items() if name == "denomination"),
                        None,
                    )
                    target_indices["nb_stagiaires"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "informationsDeclarees.nbStagiaires"
                        ),
                        None,
                    )
                    target_indices["effectif"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "informationsDeclarees.effectifFormateurs"
                        ),
                        None,
                    )
                    target_indices["actions"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "certifications.actionsDeFormation"
                        ),
                        None,
                    )
                    target_indices["region"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "adressePhysiqueOrganismeFormation.codeRegion"
                        ),
                        None,
                    )
                    target_indices["specialite"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name
                            == "informationsDeclarees.specialitesDeFormation.libelleSpecialite1"
                        ),
                        None,
                    )
                    elem.clear()
                    continue

                indices = {idx for idx in target_indices.values() if idx is not None}
                values: Dict[int, str] = {}
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
                denomination = str(values.get(target_indices["denomination"], ""))
                nb_stagiaires = parse_float(values.get(target_indices["nb_stagiaires"])) or 0.0
                effectif = parse_int(values.get(target_indices["effectif"]))
                actions = parse_int(values.get(target_indices["actions"]))
                region_code = parse_int(values.get(target_indices["region"]))
                specialite = values.get(target_indices["specialite"])
                if specialite is not None:
                    specialite = specialite.strip()
                    if not specialite:
                        specialite = None
                records.append(
                    Record(
                        denomination=denomination.strip(),
                        nb_stagiaires=nb_stagiaires,
                        effectif=effectif,
                        qualiopi_actions=actions,
                        region_code=region_code,
                        specialite=specialite,
                    )
                )
                elem.clear()
    return records


def format_int(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{int(round(value)):,}".replace(",", " ")


def format_float(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:,.{decimals}f}".replace(",", " ")


def format_percent(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:.{decimals}f}%"


def percentile(sorted_values: List[float], p: float) -> Optional[float]:
    if not sorted_values:
        return None
    if p <= 0:
        return float(sorted_values[0])
    if p >= 1:
        return float(sorted_values[-1])
    k = (len(sorted_values) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_values[int(k)])
    return float(sorted_values[f] * (c - k) + sorted_values[c] * (k - f))


def safe_mean(values: Iterable[float]) -> Optional[float]:
    values = list(values)
    if not values:
        return None
    return sum(values) / len(values)


def safe_median(values: Iterable[float]) -> Optional[float]:
    values = list(values)
    if not values:
        return None
    return statistics.median(values)


def safe_mode(values: Iterable[float]) -> Optional[float]:
    values = list(values)
    if not values:
        return None
    counts: Dict[float, int] = {}
    for v in values:
        counts[v] = counts.get(v, 0) + 1
    max_count = max(counts.values())
    modes = [v for v, c in counts.items() if c == max_count]
    return float(min(modes))


def classify_tranche(value: float, tranches: List[Tuple[str, Optional[float], Optional[float]]]) -> Optional[str]:
    for name, lower, upper in tranches:
        if lower is not None and value < lower:
            continue
        if upper is not None and value > upper:
            continue
        return name
    return None


def aggregate_tranches(records: List[Record]) -> Tuple[List[List[str]], Dict[str, float]]:
    tranches = [
        ("0 (dormants)", 0, 0),
        ("1-10", 1, 10),
        ("11-50", 11, 50),
        ("51-100", 51, 100),
        ("101-200", 101, 200),
        ("201-500", 201, 500),
        ("501-1000", 501, 1000),
        ("1001-2000", 1001, 2000),
        ("2001+", 2001, None),
    ]
    counts: Dict[str, int] = {name: 0 for name, _, _ in tranches}
    stag_totals: Dict[str, float] = {name: 0.0 for name, _, _ in tranches}
    for rec in records:
        tranche = None
        for name, lower, upper in tranches:
            if lower is not None and rec.nb_stagiaires < lower:
                continue
            if upper is not None and rec.nb_stagiaires > upper:
                continue
            tranche = name
            break
        if tranche is None:
            continue
        counts[tranche] += 1
        stag_totals[tranche] += rec.nb_stagiaires
    total_of = len(records)
    total_stag = sum(r.nb_stagiaires for r in records)
    rows: List[List[str]] = []
    for name, _, _ in tranches:
        nb = counts[name]
        pct = (nb / total_of * 100) if total_of else 0.0
        stag_sum = stag_totals[name]
        pct_stag = (stag_sum / total_stag * 100) if total_stag else 0.0
        rows.append(
            [
                name,
                format_int(nb),
                format_percent(pct, 1),
                format_int(stag_sum),
                format_percent(pct_stag, 1),
            ]
        )
    rows.append(
        [
            "TOTAL",
            format_int(total_of),
            "100%",
            format_int(total_stag),
            "100%",
        ]
    )
    return rows, {name: counts[name] for name in counts}


def filter_tam(records: List[Record]) -> List[Record]:
    result: List[Record] = []
    for rec in records:
        if rec.effectif is None or rec.effectif < 3 or rec.effectif > 10:
            continue
        if rec.qualiopi_actions != 1:
            continue
        if rec.nb_stagiaires <= 0:
            continue
        result.append(rec)
    return result


def build_table2(tam_records: List[Record]) -> List[List[str]]:
    tranches = [
        ("1-50", 1, 50),
        ("51-100", 51, 100),
        ("101-200", 101, 200),
        ("201-500", 201, 500),
        ("501-1000", 501, 1000),
        ("1000+", 1001, None),
    ]
    total = len(tam_records)
    total_stag = sum(r.nb_stagiaires for r in tam_records)
    rows: List[List[str]] = []
    for name, lower, upper in tranches:
        subset = [
            r
            for r in tam_records
            if r.nb_stagiaires >= lower and (upper is None or r.nb_stagiaires <= upper)
        ]
        count = len(subset)
        pct = (count / total * 100) if total else 0.0
        stag_sum = sum(r.nb_stagiaires for r in subset)
        stag_mean = safe_mean([r.nb_stagiaires for r in subset])
        rows.append(
            [
                name,
                format_int(count),
                format_percent(pct, 1),
                format_int(stag_sum),
                format_float(stag_mean, 1),
            ]
        )
    rows.append(
        [
            "TOTAL TAM",
            format_int(total),
            "100%",
            format_int(total_stag),
            format_float(total_stag / total if total else None, 1),
        ]
    )
    return rows


def build_table3(tam_records: List[Record]) -> Tuple[List[List[str]], Dict[int, float]]:
    rows: List[List[str]] = []
    intensity: Dict[int, float] = {}
    for effectif in range(3, 11):
        subset = [r.nb_stagiaires for r in tam_records if r.effectif == effectif]
        count = len(subset)
        mean_val = safe_mean(subset)
        median_val = safe_median(subset)
        ratio = (mean_val / effectif) if mean_val is not None and effectif else None
        rows.append(
            [
                str(effectif),
                format_int(count),
                format_float(mean_val, 1),
                format_float(median_val, 1),
                format_float(ratio, 1),
            ]
        )
        if ratio is not None:
            intensity[effectif] = ratio
    overall_mean = safe_mean([r.nb_stagiaires for r in tam_records])
    overall_median = safe_median([r.nb_stagiaires for r in tam_records])
    overall_ratio = (
        (overall_mean / (sum(r.effectif for r in tam_records if r.effectif) / len(tam_records)))
        if tam_records
        else None
    )
    rows.append(
        [
            "TOTAL",
            format_int(len(tam_records)),
            format_float(overall_mean, 1),
            format_float(overall_median, 1),
            "-",
        ]
    )
    return rows, intensity


def build_table4(tam_records: List[Record]) -> Tuple[List[List[str]], List[Dict[str, str]]]:
    top50 = sorted(tam_records, key=lambda r: r.nb_stagiaires, reverse=True)[:50]
    rows: List[List[str]] = []
    csv_rows: List[Dict[str, str]] = []
    for rank, rec in enumerate(top50, start=1):
        region_name = REGION_NAMES.get(rec.region_code or 0, "Autre / Non renseigné")
        ratio = rec.nb_stagiaires / rec.effectif if rec.effectif else None
        specialite = rec.specialite or "Non renseigné"
        rows.append(
            [
                str(rank),
                rec.denomination or "Non renseigné",
                format_int(rec.effectif),
                format_int(rec.nb_stagiaires),
                format_float(ratio, 1),
                region_name,
                specialite,
            ]
        )
        csv_rows.append(
            {
                "rang": rank,
                "denomination": rec.denomination,
                "effectif": rec.effectif if rec.effectif is not None else "",
                "stagiaires": int(round(rec.nb_stagiaires)),
                "stagiaires_par_formateur": f"{ratio:.1f}" if ratio is not None else "",
                "region": region_name,
                "specialite": specialite,
            }
        )
    return rows, csv_rows


def write_top50_csv(csv_rows: List[Dict[str, str]]) -> str:
    path = os.path.join(OUTPUT_DIR, "top50_tam_stagiaires.csv")
    fieldnames = [
        "rang",
        "denomination",
        "effectif",
        "stagiaires",
        "stagiaires_par_formateur",
        "region",
        "specialite",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)
    return path


def build_table5(tam_records: List[Record]) -> Tuple[List[List[str]], Dict[int, Dict[str, float]]]:
    region_groups: Dict[int, List[Record]] = {}
    for rec in tam_records:
        code = rec.region_code or 0
        region_groups.setdefault(code, []).append(rec)
    total_stag = sum(r.nb_stagiaires for r in tam_records)
    total_count = len(tam_records)
    national_mean = (total_stag / total_count) if total_count else 0.0
    rows: List[List[str]] = []
    region_stats: Dict[int, Dict[str, float]] = {}

    def sort_key(item: Tuple[int, List[Record]]):
        code, recs = item
        name = REGION_NAMES.get(code, "Autre / Non renseigné")
        return name

    for code, recs in sorted(region_groups.items(), key=sort_key):
        count = len(recs)
        stag_sum = sum(r.nb_stagiaires for r in recs)
        mean_val = stag_sum / count if count else 0.0
        pct_fr = (stag_sum / total_stag * 100) if total_stag else 0.0
        uplift = (mean_val / national_mean - 1) if national_mean else 0.0
        region_stats[code] = {
            "count": count,
            "stag_sum": stag_sum,
            "mean": mean_val,
            "pct": pct_fr,
            "uplift": uplift,
        }
        label = REGION_NAMES.get(code, "Autre / Non renseigné")
        rows.append(
            [
                label,
                format_int(count),
                format_int(stag_sum),
                format_float(mean_val, 1),
                format_percent(pct_fr, 1),
                ("+" if uplift >= 0 else "") + f"{uplift * 100:.1f}%",
            ]
        )
    rows.append(
        [
            "TOTAL",
            format_int(total_count),
            format_int(total_stag),
            format_float(national_mean, 1),
            "100%",
            "+0.0%",
        ]
    )
    return rows, region_stats


def build_table6(stats: Dict[str, float]) -> List[List[str]]:
    return [
        ["Moyenne", format_float(stats.get("mean"), 1)],
        ["Médiane", format_float(stats.get("median"), 1)],
        ["Mode", format_float(stats.get("mode"), 1)],
        ["Écart-type", format_float(stats.get("std"), 1)],
        ["Q1", format_float(stats.get("q1"), 1)],
        ["Q3", format_float(stats.get("q3"), 1)],
        ["P90", format_float(stats.get("p90"), 1)],
        ["P95", format_float(stats.get("p95"), 1)],
        ["P99", format_float(stats.get("p99"), 1)],
    ]


def render_table(title: str, headers: List[str], rows: List[List[str]]) -> List[str]:
    lines = [title]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    return lines


def summarize_distribution(tam_records: List[Record]) -> Dict[str, float]:
    values = sorted(r.nb_stagiaires for r in tam_records)
    mean_val = safe_mean(values)
    median_val = safe_median(values)
    mode_val = safe_mode(values)
    std_val = statistics.pstdev(values) if len(values) > 1 else 0.0
    stats = {
        "mean": mean_val or 0.0,
        "median": median_val or 0.0,
        "mode": mode_val or 0.0,
        "std": std_val,
        "q1": percentile(values, 0.25) or 0.0,
        "q3": percentile(values, 0.75) or 0.0,
        "p90": percentile(values, 0.90) or 0.0,
        "p95": percentile(values, 0.95) or 0.0,
        "p99": percentile(values, 0.99) or 0.0,
    }
    return stats


def detect_distribution_shape(mean_val: float, median_val: float, mode_val: float) -> str:
    if mean_val > median_val > mode_val:
        return "Asymétrique droite"
    if abs(mean_val - median_val) < 1e-6 and abs(mean_val - mode_val) < 1e-6:
        return "Normale"
    return "Asymétrique"


def compute_segment_shares(tam_records: List[Record]) -> Dict[str, float]:
    total = len(tam_records)
    if total == 0:
        return {"faible": 0.0, "moyenne": 0.0, "haute": 0.0}
    segments = {
        "faible": sum(1 for r in tam_records if 1 <= r.nb_stagiaires <= 100),
        "moyenne": sum(1 for r in tam_records if 101 <= r.nb_stagiaires <= 500),
        "haute": sum(1 for r in tam_records if r.nb_stagiaires >= 501),
    }
    return {k: v / total * 100 for k, v in segments.items()}


def main() -> None:
    ensure_output_dir()
    records = load_records()
    table1_rows, _ = aggregate_tranches(records)

    tam_records = filter_tam(records)
    table2_rows = build_table2(tam_records)
    table3_rows, intensity = build_table3(tam_records)
    table4_rows, top50_csv_rows = build_table4(tam_records)
    table5_rows, region_stats = build_table5(tam_records)
    stats = summarize_distribution(tam_records)
    table6_rows = build_table6(stats)

    write_top50_csv(top50_csv_rows)

    total_france = sum(r.nb_stagiaires for r in records)
    tam_total = sum(r.nb_stagiaires for r in tam_records)
    tam_count = len(tam_records)
    tam_mean = tam_total / tam_count if tam_count else 0.0
    tam_median = safe_median([r.nb_stagiaires for r in tam_records]) or 0.0

    segment_shares = compute_segment_shares(tam_records)

    sorted_ratios = sorted(intensity.items(), key=lambda x: x[0])
    trend = "Dégressif" if sorted_ratios and all(
        sorted_ratios[i][1] >= sorted_ratios[i + 1][1] - 1e-6
        for i in range(len(sorted_ratios) - 1)
    ) else "Linéaire"
    sweet_spot = None
    if intensity:
        sweet_spot = max(intensity.items(), key=lambda x: x[1])[0]

    top10_cut = max(1, math.ceil(tam_count * 0.10)) if tam_count else 0
    sorted_tam = sorted(tam_records, key=lambda r: r.nb_stagiaires, reverse=True)
    top10_sum = sum(r.nb_stagiaires for r in sorted_tam[:top10_cut])
    top10_share = (top10_sum / tam_total * 100) if tam_total else 0.0

    dist_shape = detect_distribution_shape(stats["mean"], stats["median"], stats["mode"])

    summary_lines = [
        "## Synthèse",
        "",
        f"* Total stagiaires France : {format_int(total_france)}",
        f"* TAM qualifié 3-10 : {format_int(tam_count)} OF actifs certifiés pour {format_int(tam_total)} stagiaires ({format_percent((tam_total / total_france * 100) if total_france else 0.0, 1)})",
        f"* Intensité moyenne : {format_float(tam_mean, 1)} stagiaires / OF, médiane {format_float(tam_median, 1)}",
        f"* Segments d'activité TAM : Faible (1-100) {format_percent(segment_shares['faible'], 1)}, Moyenne (101-500) {format_percent(segment_shares['moyenne'], 1)}, Haute (501+) {format_percent(segment_shares['haute'], 1)}",
        f"* Rendement : {trend} – productivité maximale autour de {sweet_spot} formateurs" if sweet_spot else f"* Rendement : {trend}",
        f"* Concentration : Top 10% OF concentrent {format_percent(top10_share, 1)} des stagiaires TAM",
        f"* Distribution statistique : {dist_shape} (σ = {format_float(stats['std'], 1)}, P90 = {format_float(stats['p90'], 1)})",
        "",
    ]

    markdown_lines: List[str] = []
    markdown_lines.extend(summary_lines)
    markdown_lines.extend(render_table("### Tableau 1 : Distribution complète des stagiaires (tous OF)", [
        "Tranche stagiaires",
        "Nombre OF",
        "% total",
        "Stagiaires total",
        "% stag France",
    ], table1_rows))

    markdown_lines.extend(render_table("### Tableau 2 : Activité stagiaires – TAM qualifié (3-10 formateurs, Qualiopi, actifs)", [
        "Tranche stagiaires",
        "OF TAM",
        "% TAM",
        "Stagiaires total",
        "Stagiaires moyen",
    ], table2_rows))

    markdown_lines.extend(render_table("### Tableau 3 : Stagiaires par effectif formateurs (TAM)", [
        "Effectif",
        "OF TAM",
        "Stagiaires moyen",
        "Stagiaires médian",
        "Stagiaires / formateur",
    ], table3_rows))

    markdown_lines.extend(render_table("### Tableau 4 : Top 50 OF TAM par stagiaires", [
        "Rang",
        "Dénomination",
        "Effectif",
        "Stagiaires",
        "Stag./formateur",
        "Région",
        "Spécialité",
    ], table4_rows))

    markdown_lines.extend(render_table("### Tableau 5 : Activité TAM par région", [
        "Région",
        "OF TAM",
        "Stagiaires total",
        "Stagiaires moyen",
        "% France",
        "Écart vs national",
    ], table5_rows))

    markdown_lines.extend(render_table("### Tableau 6 : Statistiques descriptives TAM", [
        "Indicateur",
        "Valeur",
    ], table6_rows))

    output_path = os.path.join(OUTPUT_DIR, "stagiaires_analysis.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(markdown_lines))


if __name__ == "__main__":
    main()

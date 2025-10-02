import csv
import os
import statistics
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

# Column indices based on header inspection
COL_DENOMINATION = 2
COL_CODE_POSTAL = 6
COL_VILLE = 7
COL_CODE_REGION = 8
COL_ACTIONS_FORMATION = 9
COL_NB_STAGIAIRES = 27
COL_EFFECTIF_FORMATEURS = 29

DEPARTMENT_NAMES: Dict[str, str] = {
    "01": "Ain",
    "02": "Aisne",
    "03": "Allier",
    "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes",
    "06": "Alpes-Maritimes",
    "07": "Ardèche",
    "08": "Ardennes",
    "09": "Ariège",
    "10": "Aube",
    "11": "Aude",
    "12": "Aveyron",
    "13": "Bouches-du-Rhône",
    "14": "Calvados",
    "15": "Cantal",
    "16": "Charente",
    "17": "Charente-Maritime",
    "18": "Cher",
    "19": "Corrèze",
    "2A": "Corse-du-Sud",
    "2B": "Haute-Corse",
    "21": "Côte-d'Or",
    "22": "Côtes-d'Armor",
    "23": "Creuse",
    "24": "Dordogne",
    "25": "Doubs",
    "26": "Drôme",
    "27": "Eure",
    "28": "Eure-et-Loir",
    "29": "Finistère",
    "30": "Gard",
    "31": "Haute-Garonne",
    "32": "Gers",
    "33": "Gironde",
    "34": "Hérault",
    "35": "Ille-et-Vilaine",
    "36": "Indre",
    "37": "Indre-et-Loire",
    "38": "Isère",
    "39": "Jura",
    "40": "Landes",
    "41": "Loir-et-Cher",
    "42": "Loire",
    "43": "Haute-Loire",
    "44": "Loire-Atlantique",
    "45": "Loiret",
    "46": "Lot",
    "47": "Lot-et-Garonne",
    "48": "Lozère",
    "49": "Maine-et-Loire",
    "50": "Manche",
    "51": "Marne",
    "52": "Haute-Marne",
    "53": "Mayenne",
    "54": "Meurthe-et-Moselle",
    "55": "Meuse",
    "56": "Morbihan",
    "57": "Moselle",
    "58": "Nièvre",
    "59": "Nord",
    "60": "Oise",
    "61": "Orne",
    "62": "Pas-de-Calais",
    "63": "Puy-de-Dôme",
    "64": "Pyrénées-Atlantiques",
    "65": "Hautes-Pyrénées",
    "66": "Pyrénées-Orientales",
    "67": "Bas-Rhin",
    "68": "Haut-Rhin",
    "69": "Rhône",
    "70": "Haute-Saône",
    "71": "Saône-et-Loire",
    "72": "Sarthe",
    "73": "Savoie",
    "74": "Haute-Savoie",
    "75": "Paris",
    "76": "Seine-Maritime",
    "77": "Seine-et-Marne",
    "78": "Yvelines",
    "79": "Deux-Sèvres",
    "80": "Somme",
    "81": "Tarn",
    "82": "Tarn-et-Garonne",
    "83": "Var",
    "84": "Vaucluse",
    "85": "Vendée",
    "86": "Vienne",
    "87": "Haute-Vienne",
    "88": "Vosges",
    "89": "Yonne",
    "90": "Territoire de Belfort",
    "91": "Essonne",
    "92": "Hauts-de-Seine",
    "93": "Seine-Saint-Denis",
    "94": "Val-de-Marne",
    "95": "Val-d'Oise",
    "971": "Guadeloupe",
    "972": "Martinique",
    "973": "Guyane",
    "974": "La Réunion",
    "975": "Saint-Pierre-et-Miquelon",
    "976": "Mayotte",
    "977": "Saint-Barthélemy",
    "978": "Saint-Martin",
    "986": "Wallis-et-Futuna",
    "987": "Polynésie française",
    "988": "Nouvelle-Calédonie",
    "989": "Île de Clipperton",
    "990": "Monaco",
}

REGION_NAMES: Dict[str, str] = {
    "01": "Guadeloupe",
    "02": "Martinique",
    "03": "Guyane",
    "04": "La Réunion",
    "06": "Mayotte",
    "11": "Île-de-France",
    "24": "Centre-Val de Loire",
    "27": "Bourgogne-Franche-Comté",
    "28": "Normandie",
    "32": "Hauts-de-France",
    "44": "Grand Est",
    "52": "Pays de la Loire",
    "53": "Bretagne",
    "75": "Nouvelle-Aquitaine",
    "76": "Occitanie",
    "84": "Auvergne-Rhône-Alpes",
    "93": "Provence-Alpes-Côte d'Azur",
    "94": "Corse",
    "905": "Hors territoire (Etranger/Monaco)",
}

CITY_METRO_POP = {
    "PARIS": 10800000,
    "LYON": 2400000,
    "MARSEILLE": 1800000,
    "TOULOUSE": 1300000,
    "LILLE": 1200000,
    "BORDEAUX": 1000000,
    "NICE": 950000,
    "NANTES": 980000,
    "STRASBOURG": 850000,
    "RENNES": 750000,
    "MONTPELLIER": 780000,
    "TOULON": 650000,
    "GRENOBLE": 680000,
    "ROUEN": 660000,
    "DOUAI": 650000,
    "SAINT-ÉTIENNE": 520000,
    "SAINT-ETIENNE": 520000,
    "TOURS": 500000,
    "CLERMONT-FERRAND": 480000,
    "CLERMONT FERRAND": 480000,
    "METZ": 430000,
    "NANCY": 430000,
    "ANGERS": 420000,
    "DIJON": 390000,
    "BREST": 380000,
    "ORLÉANS": 450000,
    "ORLEANS": 450000,
    "LE HAVRE": 330000,
    "AMIENS": 320000,
    "BESANÇON": 280000,
    "BESANCON": 280000,
    "PERPIGNAN": 320000,
    "AVIGNON": 300000,
    "BAYONNE": 300000,
    "PAU": 240000,
    "CAEN": 420000,
    "REIMS": 320000,
    "TOURCOING": 1200000,
    "VILLEURBANNE": 2400000,
}

CLUSTERS = [
    ("Grand Paris", ["75", "92", "93", "94", "91", "78", "95", "77"]),
    ("Lyon Métropole", ["69", "01", "38", "42"]),
    ("Aix-Marseille-Provence", ["13", "83", "84"]),
    ("Toulouse & Occitanie Ouest", ["31", "32", "82", "81"]),
    ("Lille - Flandres", ["59", "62", "80"]),
    ("Bordeaux - Nouvelle Aquitaine", ["33", "24", "47", "40"]),
    ("Nantes - Bretagne Sud", ["44", "49", "56", "85"]),
]


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


def normalize_postal_code(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    text = text.replace(" ", "")
    if text.endswith(".0"):
        text = text[:-2]
    if len(text) >= 3 and text[:3].isdigit() and text[:2] in {"97", "98"}:
        return text[:3]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 3 and digits[:2] in {"97", "98"}:
        if digits.startswith("97") and len(digits) >= 3:
            return digits[:3]
        if digits.startswith("98") and len(digits) >= 3:
            return digits[:3]
    if len(digits) == 0:
        return None
    if len(digits) < 5:
        digits = digits.zfill(5)
    if digits.startswith("20"):
        third = digits[2] if len(digits) > 2 else "0"
        return "2A" if third in {"0", "1"} else "2B"
    return digits[:2]


def extract_department(code_postal: Optional[str]) -> Optional[str]:
    dept = normalize_postal_code(code_postal)
    if dept is None:
        return None
    if dept in DEPARTMENT_NAMES:
        return dept
    return dept


def clean_region_code(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit() and len(text) == 1:
        text = text.zfill(2)
    return text


def load_records() -> List[Dict[str, Optional[str]]]:
    records: List[Dict[str, Optional[str]]] = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            for event, elem in ET.iterparse(f, events=("end",)):
                if elem.tag != NS + "row":
                    continue
                row_index = int(elem.attrib.get("r"))
                if row_index == 1:
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
                record = {
                    "denomination": values.get(COL_DENOMINATION, ""),
                    "code_postal_raw": values.get(COL_CODE_POSTAL),
                    "ville": values.get(COL_VILLE),
                    "code_region": clean_region_code(values.get(COL_CODE_REGION)),
                    "actions_formation": parse_float(values.get(COL_ACTIONS_FORMATION)),
                    "nb_stagiaires": parse_float(values.get(COL_NB_STAGIAIRES)),
                    "effectif_formateurs": parse_float(values.get(COL_EFFECTIF_FORMATEURS)),
                }
                record["departement"] = extract_department(record["code_postal_raw"])
                records.append(record)
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


def compute_department_stats(records: List[Dict[str, Optional[str]]]):
    dept_stats: Dict[str, Dict[str, float]] = defaultdict(lambda: {
        "count": 0,
        "with_city": 0,
        "with_stagiaires": 0,
        "stagiaires_sum": 0.0,
        "stagiaires_count": 0,
        "unique_cities": Counter(),
    })
    for rec in records:
        dept = rec.get("departement")
        if not dept:
            continue
        stats = dept_stats[dept]
        stats["count"] += 1
        ville = (rec.get("ville") or "").strip().upper()
        if ville:
            stats["with_city"] += 1
            stats["unique_cities"][ville] += 1
        if rec.get("nb_stagiaires") is not None:
            stats["with_stagiaires"] += 1
            stats["stagiaires_sum"] += float(rec["nb_stagiaires"])
            stats["stagiaires_count"] += 1
    return dept_stats


def compute_region_stats(records: List[Dict[str, Optional[str]]]):
    region_totals: Dict[str, Dict[str, float]] = defaultdict(lambda: {
        "with_cp": 0,
        "without_cp": 0,
    })
    for rec in records:
        region = rec.get("code_region")
        if not region:
            continue
        if rec.get("departement"):
            region_totals[region]["with_cp"] += 1
        else:
            region_totals[region]["without_cp"] += 1
    return region_totals


def top_cities(records: List[Dict[str, Optional[str]]], limit: int = 20) -> List[Dict[str, object]]:
    city_counts: Dict[Tuple[str, str], Dict[str, object]] = defaultdict(lambda: {
        "count": 0,
        "dept": None,
        "nb_stagiaires": [],
    })
    for rec in records:
        dept = rec.get("departement")
        if not dept:
            continue
        ville_raw = (rec.get("ville") or "").strip()
        if not ville_raw:
            continue
        ville = ville_raw.upper()
        key = (ville, dept)
        city_counts[key]["count"] += 1
        city_counts[key]["dept"] = dept
        if rec.get("nb_stagiaires") is not None:
            city_counts[key]["nb_stagiaires"].append(float(rec["nb_stagiaires"]))
    rows: List[Tuple[str, str, float, float]] = []
    for (ville, dept), data in city_counts.items():
        count = data["count"]
        avg_stagiaires = (
            statistics.mean(data["nb_stagiaires"]) if data["nb_stagiaires"] else 0.0
        )
        rows.append((ville, dept, count, avg_stagiaires))
    rows.sort(key=lambda x: x[2], reverse=True)
    return [
        {
            "ville": ville,
            "dept": dept,
            "count": int(count),
            "avg_stagiaires": avg_stagiaires,
        }
        for ville, dept, count, avg_stagiaires in rows[:limit]
    ]


def summarize_department_table(dept_stats, total_records: int):
    rows = []
    for dept, stats in dept_stats.items():
        count = stats["count"]
        share = count / total_records * 100 if total_records else 0
        avg_stagiaires = (
            stats["stagiaires_sum"] / stats["stagiaires_count"]
            if stats["stagiaires_count"]
            else None
        )
        unique_cities = stats["unique_cities"]
        top_cities = ", ".join(
            f"{city.title()} ({cnt})" for city, cnt in unique_cities.most_common(3)
        )
        city_ratio = stats["with_city"] / count if count else 0
        stag_ratio = stats["with_stagiaires"] / count if count else 0
        completeness = 0.5 * (city_ratio + stag_ratio)
        density_proxy = count / len(unique_cities) if unique_cities else 0
        rows.append(
            {
                "dept": dept,
                "name": DEPARTMENT_NAMES.get(dept, dept),
                "count": count,
                "share_pct": share,
                "avg_stagiaires": avg_stagiaires,
                "top_cities": top_cities,
                "completeness": completeness,
                "density_proxy": density_proxy,
                "with_city_ratio": city_ratio,
                "with_stagiaires_ratio": stag_ratio,
                "stagiaires_sum": stats["stagiaires_sum"],
                "stagiaires_count": stats["stagiaires_count"],
            }
        )
    rows.sort(key=lambda r: (r["count"], r["avg_stagiaires"] or 0), reverse=True)
    return rows


def write_markdown_table(filename: str, headers: List[str], rows: Iterable[Iterable[object]]):
    ensure_output_dir()
    path = os.path.join(OUTPUT_DIR, filename)
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def write_csv(filename: str, headers: List[str], rows: Iterable[Iterable[object]]):
    ensure_output_dir()
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
    return path


def build_dom_table(dept_rows):
    dom_codes = ["971", "972", "973", "974", "976", "987", "988"]
    dom_rows = []
    for code in dom_codes:
        match = next((row for row in dept_rows if row["dept"] == code), None)
        if match:
            dom_rows.append(match)
        else:
            dom_rows.append(
                {
                    "dept": code,
                    "name": DEPARTMENT_NAMES.get(code, code),
                    "count": 0,
                    "share_pct": 0.0,
                    "avg_stagiaires": None,
                    "top_cities": "",
                    "stagiaires_sum": 0.0,
                    "stagiaires_count": 0,
                }
            )
    dom_rows.sort(key=lambda r: r["count"], reverse=True)
    return dom_rows


def build_clusters_table(dept_summary):
    cluster_rows = []
    total_records = sum(r["count"] for r in dept_summary)
    for name, codes in CLUSTERS:
        count = sum(next((r["count"] for r in dept_summary if r["dept"] == code), 0) for code in codes)
        share = count / total_records * 100 if total_records else 0
        top_city = None
        for code in codes:
            top_city = next(
                (
                    r["top_cities"].split(",")[0].strip()
                    for r in dept_summary
                    if r["dept"] == code and r["top_cities"]
                ),
                None,
            )
            if top_city:
                break
        cluster_rows.append((name, ", ".join(codes), count, share, top_city or "-"))
    cluster_rows.sort(key=lambda x: x[2], reverse=True)
    return cluster_rows


def build_scoring_table(dept_summary):
    counts = [r["count"] for r in dept_summary]
    avg_stagiaires_values = [r["avg_stagiaires"] or 0 for r in dept_summary]
    density_values = [r["density_proxy"] for r in dept_summary]
    max_count = max(counts) if counts else 1
    max_avg = max(avg_stagiaires_values) if avg_stagiaires_values else 1
    max_density = max(density_values) if density_values else 1
    scored = []
    for r in dept_summary:
        count = r["count"]
        norm_count = count / max_count if max_count else 0
        avg_value = r["avg_stagiaires"] or 0
        norm_avg = avg_value / max_avg if max_avg else 0
        norm_density = r["density_proxy"] / max_density if max_density else 0
        stag_count = r.get("stagiaires_count", 0)
        avg_weight = min(1.0, stag_count / 50) if stag_count else 0.0
        norm_avg *= avg_weight
        completeness = r["completeness"] * (0.5 + 0.5 * avg_weight)
        size_factor = 0.5 + 0.5 * min(1.0, count / 500) if count else 0
        score = (
            0.35 * norm_count
            + 0.25 * norm_avg
            + 0.20 * norm_density
            + 0.20 * completeness
        ) * size_factor
        scored.append(
            {
                "dept": r["dept"],
                "name": r["name"],
                "score": score,
                "count": count,
            }
        )
    scored.sort(key=lambda x: x["score"], reverse=True)
    for idx, row in enumerate(scored, start=1):
        row["rank"] = idx
        if idx <= 10:
            row["priority"] = "🔴 HAUTE"
        elif idx <= 20:
            row["priority"] = "🟠 MOYENNE"
        elif idx <= 30:
            row["priority"] = "🟢 BASSE"
        else:
            row["priority"] = ""
    return scored


def main():
    ensure_output_dir()
    records = load_records()
    total_records = len(records)
    records_with_cp = [r for r in records if r.get("departement")]
    dept_stats = compute_department_stats(records)
    dept_summary = summarize_department_table(dept_stats, total_records)
    top100 = dept_summary[:100]

    # Table 1
    table1_rows = []
    table1_csv_rows = []
    for idx, row in enumerate(top100, start=1):
        table1_rows.append(
            [
                idx,
                row["dept"],
                row["name"],
                format_int(row["count"]),
                f"{row['share_pct']:.2f}%",
                f"{row['with_city_ratio']*100:.1f}%",
                format_float(row["avg_stagiaires"], 1),
                row["top_cities"] or "-",
            ]
        )
        table1_csv_rows.append(
            [
                idx,
                row["dept"],
                row["name"],
                row["count"],
                round(row["share_pct"], 4),
                round(row["with_city_ratio"] * 100, 1),
                round(row["avg_stagiaires"], 1) if row["avg_stagiaires"] is not None else None,
                row["top_cities"] or "",
            ]
        )
    write_markdown_table(
        "table1_top_departements.md",
        [
            "Rang",
            "Dept",
            "Nom",
            "Nombre OF",
            "% TAM",
            "% fiches avec ville",
            "Stagiaires moyen",
            "Villes principales",
        ],
        table1_rows,
    )
    write_csv(
        "top_departements.csv",
        [
            "rang",
            "departement",
            "nom",
            "nombre_of",
            "part_tam_pct",
            "part_fiches_ville_pct",
            "stagiaires_moyen",
            "villes_principales",
        ],
        table1_csv_rows,
    )

    # Table 2
    dom_rows = build_dom_table(dept_summary)
    table2_rows = []
    dom_total_count = 0
    dom_total_share = 0.0
    dom_stag_sum = 0.0
    dom_stag_count = 0
    for row in dom_rows:
        dom_total_count += row["count"]
        dom_total_share += row["share_pct"]
        dom_stag_sum += row["stagiaires_sum"]
        dom_stag_count += row["stagiaires_count"]
        table2_rows.append(
            [
                row["dept"],
                row["name"],
                format_int(row["count"]),
                f"{row['share_pct']:.2f}%",
                format_float(row["avg_stagiaires"], 1),
                row["top_cities"].split(",")[0] if row["top_cities"] else "-",
            ]
        )
    dom_avg_total = dom_stag_sum / dom_stag_count if dom_stag_count else None
    table2_rows.append(
        [
            "TOTAL",
            "DOM-TOM",
            format_int(dom_total_count),
            f"{dom_total_share:.2f}%",
            format_float(dom_avg_total, 1),
            "-",
        ]
    )
    write_markdown_table(
        "table2_dom.md",
        ["Code", "Département", "Nombre OF", "% TAM", "Stagiaires moyen", "Ville principale"],
        table2_rows,
    )

    # Table 3 - top cities
    top_cities_rows = top_cities(records)
    table3_rows = []
    for idx, item in enumerate(top_cities_rows, start=1):
        ville = item["ville"]
        dept = item["dept"]
        ville_upper = ville.upper()
        metro_pop = CITY_METRO_POP.get(ville_upper)
        if metro_pop is None:
            if ville_upper.startswith("PARIS"):
                metro_pop = 10800000
            elif "LYON" in ville_upper:
                metro_pop = 2400000
            elif "MARSEILLE" in ville_upper or "AIX" in ville_upper:
                metro_pop = 1800000
            elif "TOULOUSE" in ville_upper:
                metro_pop = 1300000
            elif "LILLE" in ville_upper or "ROUBAIX" in ville_upper or "TOURCOING" in ville_upper:
                metro_pop = 1200000
            elif "BORDEAUX" in ville_upper:
                metro_pop = 1000000
            elif "NANTES" in ville_upper:
                metro_pop = 980000
            elif "STRASBOURG" in ville_upper:
                metro_pop = 850000
            elif "RENNES" in ville_upper:
                metro_pop = 750000
            elif "MONTPELLIER" in ville_upper:
                metro_pop = 780000
            elif "NICE" in ville_upper or "CANNES" in ville_upper or "ANTIBES" in ville_upper:
                metro_pop = 950000
            else:
                metro_pop = "-"
        table3_rows.append(
            [
                idx,
                ville.title(),
                dept,
                format_int(item["count"]),
                f"{item['count'] / len(records_with_cp) * 100:.2f}%" if records_with_cp else "0%",
                format_float(item["avg_stagiaires"], 1),
                metro_pop,
            ]
        )
    write_markdown_table(
        "table3_grandes_villes.md",
        ["Rang", "Ville", "Dept", "OF cible", "% TAM ville", "Stagiaires moyen", "Pop. métropole"],
        table3_rows,
    )

    # Table 4 - OF sans CP par région
    region_stats = compute_region_stats(records)
    table4_rows = []
    for code, stats in sorted(region_stats.items(), key=lambda x: (-(x[1]["without_cp"]), x[0])):
        total = stats["with_cp"] + stats["without_cp"]
        completeness = stats["with_cp"] / total * 100 if total else 0
        table4_rows.append(
            [
                code,
                REGION_NAMES.get(code, code),
                format_int(stats["without_cp"]),
                format_int(stats["with_cp"]),
                f"{completeness:.1f}%",
            ]
        )
    write_markdown_table(
        "table4_regions_sans_cp.md",
        ["Code région", "Nom", "OF sans CP", "OF avec CP", "Taux complétude"],
        table4_rows,
    )

    # Table 5 - clusters
    clusters_rows = build_clusters_table(dept_summary)
    table5_rows = []
    table5_csv_rows = []
    for name, codes, count, share, city in clusters_rows:
        table5_rows.append(
            [
                name,
                codes,
                format_int(count),
                f"{share:.2f}%",
                city,
            ]
        )
        table5_csv_rows.append(
            [
                name,
                codes,
                count,
                round(share, 4),
                city,
            ]
        )
    write_markdown_table(
        "table5_clusters.md",
        ["Cluster", "Départements", "Total OF", "% TAM", "Ville principale"],
        table5_rows,
    )
    write_csv(
        "clusters.csv",
        ["cluster", "departements", "total_of", "part_tam_pct", "ville_principale"],
        table5_csv_rows,
    )

    # Table 6 - scoring
    scoring_rows = build_scoring_table(dept_summary)
    table6_rows = []
    for row in scoring_rows[:30]:
        table6_rows.append(
            [
                row["rank"],
                row["dept"],
                row["name"],
                f"{row['score']:.3f}",
                format_int(row["count"]),
                row["priority"],
            ]
        )
    write_markdown_table(
        "table6_scoring.md",
        ["Rang", "Dept", "Nom", "Score", "OF cible", "Priorité"],
        table6_rows,
    )

    # Synthèse
    with_cp = len(records_with_cp)
    without_cp = total_records - with_cp
    top10_tam = sum(row["count"] for row in dept_summary[:10]) / with_cp * 100 if with_cp else 0
    top30_tam = sum(row["count"] for row in dept_summary[:30]) / with_cp * 100 if with_cp else 0
    dom_total = sum(row["count"] for row in dept_summary if row["dept"] in {"971", "972", "973", "974", "976", "987", "988"})
    clusters_identified = len([row for row in clusters_rows if row[2] > 0])

    synthese_lines = [
        "## Synthèse",
        f"Départements analysés : {len(dept_summary)}",
        f"OF avec CP : {format_int(with_cp)} ({with_cp / total_records * 100:.1f}%)",
        f"OF sans CP : {format_int(without_cp)} ({without_cp / total_records * 100:.1f}%)",
        f"Top 10 départements : {top10_tam:.1f}% du TAM (sur base OF avec CP)",
        f"Top 30 départements : {top30_tam:.1f}% du TAM (sur base OF avec CP)",
        f"DOM-TOM : {format_int(dom_total)} OF ({dom_total / with_cp * 100:.1f}% des OF avec CP)",
        f"Clusters identifiés : {clusters_identified}",
    ]
    with open(os.path.join(OUTPUT_DIR, "synthese.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(synthese_lines) + "\n")


if __name__ == "__main__":
    main()

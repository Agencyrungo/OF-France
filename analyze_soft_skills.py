import csv
import os
import statistics
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
OUTPUT_MARKDOWN = os.path.join(OUTPUT_DIR, "soft_skills_analysis.md")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "soft_skills_tam.csv")
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

SOFT_LABEL_MAP = {
    "développement des capacités comportementales et relationnelles": "Comportementales",
    "enseignement, formation": "Enseignement",
    "ressources humaines, gestion du personnel, gestion de l'emploi": "RH gestion",
    "développement des capacités d'orientation, d'insertion ou de réinsertion sociales et professionnelles": "Orientation",
}

SOFT_ORDER = ["Comportementales", "Enseignement", "RH gestion", "Orientation"]

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


@dataclass
class Record:
    nda: str
    denomination: str
    nb_stagiaires: Optional[float]
    effectif: Optional[int]
    actions_cert: Optional[float]
    region_code: Optional[int]
    specialites: Tuple[Optional[str], Optional[str], Optional[str]]

    @property
    def soft_categories(self) -> List[str]:
        categories: List[str] = []
        seen: set[str] = set()
        for label in self.specialites:
            if not label:
                continue
            key = normalize_label(label)
            if key in SOFT_LABEL_MAP:
                category = SOFT_LABEL_MAP[key]
                if category not in seen:
                    categories.append(category)
                    seen.add(category)
        return categories


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_label(label: str) -> str:
    return label.strip().casefold()


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
                "nda": None,
                "denomination": None,
                "stagiaires": None,
                "effectif": None,
                "actions": None,
                "region": None,
                "spec1": None,
                "spec2": None,
                "spec3": None,
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
                    target_indices["nda"] = next((idx for idx, name in header_map.items() if name == "numeroDeclarationActivite"), None)
                    target_indices["denomination"] = next((idx for idx, name in header_map.items() if name == "denomination"), None)
                    target_indices["stagiaires"] = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.nbStagiaires"), None)
                    target_indices["effectif"] = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.effectifFormateurs"), None)
                    target_indices["actions"] = next((idx for idx, name in header_map.items() if name == "certifications.actionsDeFormation"), None)
                    target_indices["region"] = next((idx for idx, name in header_map.items() if name == "adressePhysiqueOrganismeFormation.codeRegion"), None)
                    target_indices["spec1"] = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.specialitesDeFormation.libelleSpecialite1"), None)
                    target_indices["spec2"] = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.specialitesDeFormation.libelleSpecialite2"), None)
                    target_indices["spec3"] = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.specialitesDeFormation.libelleSpecialite3"), None)
                    elem.clear()
                    continue

                indices = {idx for idx in target_indices.values() if idx is not None}
                if not indices:
                    continue
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
                nda = str(values.get(target_indices["nda"], "")).strip()
                denomination = str(values.get(target_indices["denomination"], "")).strip()
                nb_stagiaires = parse_float(values.get(target_indices["stagiaires"]))
                effectif = parse_int(values.get(target_indices["effectif"]))
                actions = parse_float(values.get(target_indices["actions"]))
                region = parse_int(values.get(target_indices["region"]))
                spec1 = values.get(target_indices["spec1"])
                spec2 = values.get(target_indices["spec2"])
                spec3 = values.get(target_indices["spec3"])
                specs = tuple(s.strip() if isinstance(s, str) and s.strip() else None for s in (spec1, spec2, spec3))
                records.append(
                    Record(
                        nda=nda,
                        denomination=denomination,
                        nb_stagiaires=nb_stagiaires,
                        effectif=effectif,
                        actions_cert=actions,
                        region_code=region,
                        specialites=specs,
                    )
                )
                elem.clear()
    return records


def mean(values: Iterable[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def median(values: Iterable[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return statistics.median(nums)


def format_number(value: Optional[float]) -> str:
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
    return f"{value*100:.{decimals}f}%"


def production_estimee(record: Record) -> float:
    stag = record.nb_stagiaires or 0.0
    effectif = record.effectif or 0
    return stag / 12.0 / 20.0 + effectif * 2.0


def build_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> List[str]:
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def percent_share(part: int, total: int) -> Optional[float]:
    if total == 0:
        return None
    return part / total


def analyse() -> None:
    ensure_output_dir()
    records = load_records()
    unique_records: Dict[str, Record] = {}
    for record in records:
        if record.nda and record.nda not in unique_records:
            unique_records[record.nda] = record
    unique_list = list(unique_records.values())

    base_total = len(unique_list)

    soft_records = [r for r in unique_list if r.soft_categories]
    soft_total = len(soft_records)

    # Analysis 1
    table1_rows: List[List[str]] = []
    for category in SOFT_ORDER:
        subset = [r for r in soft_records if category in r.soft_categories]
        count = len(subset)
        stag_mean = mean(r.nb_stagiaires for r in subset)
        effectif_mean = mean(r.effectif for r in subset)
        table1_rows.append([
            category,
            format_number(count),
            format_percent(percent_share(count, base_total)),
            format_float(stag_mean, 1),
            format_float(effectif_mean, 1),
        ])
    multi_soft = [r for r in soft_records if len(r.soft_categories) >= 2]
    multi_count = len(multi_soft)
    table1_rows.append([
        "Multi-soft (2+)",
        format_number(multi_count),
        format_percent(percent_share(multi_count, base_total)),
        format_float(mean(r.nb_stagiaires for r in multi_soft), 1),
        format_float(mean(r.effectif for r in multi_soft), 1),
    ])
    table1_rows.append([
        "TOTAL UNIQUE",
        format_number(soft_total),
        format_percent(percent_share(soft_total, base_total)),
        format_float(mean(r.nb_stagiaires for r in soft_records), 1),
        format_float(mean(r.effectif for r in soft_records), 1),
    ])

    table1 = build_table(
        ["Spécialité", "OF", "% base", "Stag. moyen", "Effectif moyen"], table1_rows
    )

    # TAM filter
    tam_records = [
        r
        for r in unique_list
        if r.effectif is not None
        and 3 <= r.effectif <= 10
        and r.actions_cert is not None
        and (r.nb_stagiaires or 0) > 0
    ]
    tam_total = len(tam_records)
    soft_tam = [r for r in tam_records if r.soft_categories]
    soft_tam_total = len(soft_tam)

    # Analysis 2
    table2_rows: List[List[str]] = []
    for category in SOFT_ORDER:
        subset = [r for r in soft_tam if category in r.soft_categories]
        count = len(subset)
        stag_mean = mean(r.nb_stagiaires for r in subset)
        stag_median = median(r.nb_stagiaires for r in subset)
        table2_rows.append([
            category,
            format_number(count),
            format_percent(percent_share(count, tam_total)),
            format_percent(percent_share(count, soft_tam_total)),
            format_float(stag_mean, 1),
            format_float(stag_median, 1),
        ])
    multi_soft_tam = [r for r in soft_tam if len(r.soft_categories) >= 2]
    table2_rows.append([
        "Multi-soft",
        format_number(len(multi_soft_tam)),
        format_percent(percent_share(len(multi_soft_tam), tam_total)),
        format_percent(percent_share(len(multi_soft_tam), soft_tam_total)),
        format_float(mean(r.nb_stagiaires for r in multi_soft_tam), 1),
        format_float(median(r.nb_stagiaires for r in multi_soft_tam), 1),
    ])
    table2_rows.append([
        "TOTAL SOFT",
        format_number(soft_tam_total),
        format_percent(percent_share(soft_tam_total, tam_total)),
        format_percent(1.0 if soft_tam_total else None),
        format_float(mean(r.nb_stagiaires for r in soft_tam), 1),
        format_float(median(r.nb_stagiaires for r in soft_tam), 1),
    ])

    table2 = build_table(
        ["Spécialité", "OF 3-10 qual.", "% TAM", "% soft total", "Stag. moyen", "Stag. médian"],
        table2_rows,
    )

    # Analysis 3
    def agg_stats(records_subset: List[Record]) -> Dict[str, Optional[float]]:
        if not records_subset:
            return {
                "count": 0,
                "stag_mean": None,
                "stag_median": None,
                "effectif_mean": None,
                "stag_form": None,
                "prod_mean": None,
            }
        stag_values = [r.nb_stagiaires or 0.0 for r in records_subset]
        effectif_values = [r.effectif or 0 for r in records_subset]
        prod_values = [production_estimee(r) for r in records_subset]
        total_effectif = sum(v for v in effectif_values if v is not None)
        stag_form = None
        if total_effectif:
            stag_form = sum(stag_values) / total_effectif
        return {
            "count": len(records_subset),
            "stag_mean": sum(stag_values) / len(stag_values),
            "stag_median": statistics.median(stag_values),
            "effectif_mean": sum(effectif_values) / len(effectif_values),
            "stag_form": stag_form,
            "prod_mean": sum(prod_values) / len(prod_values),
        }

    soft_stats = agg_stats(soft_tam)
    other_tam = [r for r in tam_records if r not in soft_tam]
    other_stats = agg_stats(other_tam)

    def diff_pct(soft_value: Optional[float], other_value: Optional[float]) -> Optional[float]:
        if soft_value is None or other_value is None or other_value == 0:
            return None
        return (soft_value - other_value) / other_value

    def significance(value: Optional[float], threshold: float = 0.1) -> str:
        if value is None:
            return "-"
        return "OUI" if abs(value) >= threshold else "NON"

    table3_rows = [
        [
            "Nombre OF",
            format_number(soft_stats["count"]),
            format_number(other_stats["count"]),
            "--",
            "--",
        ],
        [
            "% TAM",
            format_percent(percent_share(int(soft_stats["count"]), tam_total)),
            format_percent(percent_share(int(other_stats["count"]), tam_total)),
            "--",
            "--",
        ],
    ]

    metrics = [
        ("Stagiaires moyens", soft_stats["stag_mean"], other_stats["stag_mean"]),
        ("Stagiaires médians", soft_stats["stag_median"], other_stats["stag_median"]),
        ("Effectif moyen", soft_stats["effectif_mean"], other_stats["effectif_mean"]),
        ("Stagiaires / formateur", soft_stats["stag_form"], other_stats["stag_form"]),
        ("Production estimée", soft_stats["prod_mean"], other_stats["prod_mean"]),
    ]

    for label, soft_value, other_value in metrics:
        delta = diff_pct(soft_value, other_value)
        table3_rows.append(
            [
                label,
                format_float(soft_value, 1),
                format_float(other_value, 1),
                format_percent(delta) if delta is not None else "-",
                significance(delta),
            ]
        )

    table3 = build_table(
        ["Métrique", "Soft skills", "Autres", "Écart %", "Significatif ?"],
        table3_rows,
    )

    # Analysis 4
    region_soft_counts: Dict[int, int] = {}
    region_tam_counts: Dict[int, int] = {}
    for rec in tam_records:
        if rec.region_code is None:
            continue
        region_tam_counts[rec.region_code] = region_tam_counts.get(rec.region_code, 0) + 1
    for rec in soft_tam:
        if rec.region_code is None:
            continue
        region_soft_counts[rec.region_code] = region_soft_counts.get(rec.region_code, 0) + 1

    table4_rows: List[List[str]] = []
    for region_code, soft_count in sorted(region_soft_counts.items(), key=lambda x: x[1], reverse=True):
        tam_count = region_tam_counts.get(region_code, 0)
        soft_share = percent_share(soft_count, soft_tam_total)
        tam_share = percent_share(tam_count, tam_total)
        index = None
        if soft_share is not None and tam_share:
            index = soft_share / tam_share
        table4_rows.append(
            [
                REGION_NAMES.get(region_code, str(region_code)),
                format_number(soft_count),
                format_percent(soft_share),
                format_percent(tam_share),
                format_float(index, 2),
            ]
        )

    table4 = build_table(
        ["Région", "OF soft", "% soft national", "% TAM région", "Index concentration"],
        table4_rows,
    )

    # Analysis 5
    effectif_buckets: Dict[int, List[Record]] = {}
    for rec in soft_tam:
        if rec.effectif is None:
            continue
        effectif_buckets.setdefault(rec.effectif, []).append(rec)

    table5_rows: List[List[str]] = []
    for effectif in sorted(effectif_buckets):
        subset = effectif_buckets[effectif]
        count = len(subset)
        stag_mean = mean(r.nb_stagiaires for r in subset)
        prod_mean = mean(production_estimee(r) for r in subset)
        table5_rows.append(
            [
                str(effectif),
                format_number(count),
                format_percent(percent_share(count, soft_tam_total)),
                format_float(stag_mean, 1),
                format_float(prod_mean, 1),
            ]
        )

    table5 = build_table(
        ["Effectif", "OF soft", "% soft", "Stag. moyen", "Prod. estimée"],
        table5_rows,
    )

    # Analysis 6
    category_totals: Dict[str, int] = {}
    for rec in soft_tam:
        for category in rec.soft_categories:
            category_totals[category] = category_totals.get(category, 0) + 1

    combo_counts: Dict[Tuple[str, str], int] = {}
    for rec in soft_tam:
        soft_cats = rec.soft_categories
        if not soft_cats:
            continue
        non_soft_labels = {
            label for label in rec.specialites if label and normalize_label(label) not in SOFT_LABEL_MAP
        }
        for category in soft_cats:
            for label in non_soft_labels:
                key = (category, label)
                combo_counts[key] = combo_counts.get(key, 0) + 1

    top_combos = sorted(combo_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    table6_rows: List[List[str]] = []
    for (category, label), count in top_combos:
        total_for_cat = category_totals.get(category, 0)
        share = percent_share(count, total_for_cat)
        interpretation = f"Couplage soft + {label.lower()}"
        table6_rows.append(
            [
                category,
                label,
                format_number(count),
                format_percent(share),
                interpretation,
            ]
        )

    table6 = build_table(
        ["Soft skills", "Spé complémentaire", "OF", "% soft", "Interprétation"],
        table6_rows,
    )

    # Analysis 7
    position_counts: Dict[str, List[Record]] = {
        "Spé 1 (principale)": [],
        "Spé 2 (secondaire)": [],
        "Spé 3 (tertiaire)": [],
        "Multi-positions": [],
    }
    for rec in soft_tam:
        flags = {
            1: rec.specialites[0] is not None and normalize_label(rec.specialites[0]) in SOFT_LABEL_MAP,
            2: rec.specialites[1] is not None and normalize_label(rec.specialites[1]) in SOFT_LABEL_MAP,
            3: rec.specialites[2] is not None and normalize_label(rec.specialites[2]) in SOFT_LABEL_MAP,
        }
        positions = [idx for idx, has in flags.items() if has]
        if len(positions) == 1:
            pos = positions[0]
            if pos == 1:
                position_counts["Spé 1 (principale)"].append(rec)
            elif pos == 2:
                position_counts["Spé 2 (secondaire)"].append(rec)
            else:
                position_counts["Spé 3 (tertiaire)"].append(rec)
        elif len(positions) >= 2:
            position_counts["Multi-positions"].append(rec)

    table7_rows: List[List[str]] = []
    for label in ["Spé 1 (principale)", "Spé 2 (secondaire)", "Spé 3 (tertiaire)", "Multi-positions"]:
        subset = position_counts[label]
        count = len(subset)
        share = percent_share(count, soft_tam_total)
        table7_rows.append(
            [
                label,
                format_number(count),
                format_percent(share),
                format_float(mean(r.nb_stagiaires for r in subset), 1),
                {
                    "Spé 1 (principale)": "Cœur métier",
                    "Spé 2 (secondaire)": "Diversification",
                    "Spé 3 (tertiaire)": "Complément",
                    "Multi-positions": "Expertise large",
                }[label],
            ]
        )

    table7 = build_table(
        ["Position", "OF", "% soft total", "Stag. moyen", "Interprétation"],
        table7_rows,
    )

    # Summary metrics for synthesis
    top_regions = sorted(region_soft_counts.items(), key=lambda x: x[1], reverse=True)
    top3_total_share = 0.0
    top3_labels: List[str] = []
    for code, count in top_regions[:3]:
        share = percent_share(count, soft_tam_total) or 0.0
        top3_total_share += share
        top3_labels.append(f"{REGION_NAMES.get(code, str(code))} ({format_percent(share)})")
    top3_percent = format_percent(top3_total_share)
    max_index_region = None
    max_index_value = None
    for code, count in top_regions:
        soft_share = percent_share(count, soft_tam_total)
        tam_share = percent_share(region_tam_counts.get(code, 0), tam_total)
        if soft_share is None or tam_share in (None, 0):
            continue
        index_value = soft_share / tam_share
        if max_index_value is None or index_value > max_index_value:
            max_index_value = index_value
            max_index_region = REGION_NAMES.get(code, str(code))

    soft_vs_other = {
        "stag_delta": diff_pct(soft_stats["stag_mean"], other_stats["stag_mean"]),
        "prod_delta": diff_pct(soft_stats["prod_mean"], other_stats["prod_mean"]),
    }

    # Markdown assembly
    lines: List[str] = ["# Analyse Soft Skills"]
    lines.append("")
    lines.append("## Tableau 1 : Soft skills – base complète")
    lines.extend(table1)
    lines.append("")
    lines.append("## Tableau 2 : Soft skills – TAM qualifié")
    lines.extend(table2)
    lines.append("")
    lines.append("## Tableau 3 : Soft skills vs autres (TAM)")
    lines.extend(table3)
    lines.append("")
    lines.append("## Tableau 4 : Répartition géographique (TAM)")
    lines.extend(table4)
    lines.append("")
    lines.append("## Tableau 5 : Distribution effectifs soft skills (TAM)")
    lines.extend(table5)
    lines.append("")
    lines.append("## Tableau 6 : Top combinaisons soft + autres spécialités (TAM)")
    lines.extend(table6)
    lines.append("")
    lines.append("## Tableau 7 : Position des spécialités soft skills (TAM)")
    lines.extend(table7)
    lines.append("")

    lines.append("## Synthèse")
    lines.append(
        f"Soft skills dans le TAM : {format_number(soft_tam_total)} OF ({format_percent(percent_share(soft_tam_total, tam_total))})."
    )
    lines.append(
        "Répartition : "
        + ", ".join(
            f"{category} : {format_number(len([r for r in soft_tam if category in r.soft_categories]))} OF ({format_percent(percent_share(len([r for r in soft_tam if category in r.soft_categories]), soft_tam_total))})"
            for category in SOFT_ORDER
        )
    )
    stag_perf = soft_vs_other["stag_delta"]
    prod_perf = soft_vs_other["prod_delta"]
    stag_text = (
        f"Stagiaires : {format_percent(stag_perf)}"
        if stag_perf is not None
        else "Stagiaires : n/a"
    )
    prod_text = (
        f"Production : {format_percent(prod_perf)}"
        if prod_perf is not None
        else "Production : n/a"
    )
    lines.append(
        f"Performance vs autres : {stag_text} [{significance(stag_perf)}], {prod_text} [{significance(prod_perf)}]."
    )
    if top3_labels:
        lines.append(
            f"Concentration : Top 3 régions = {top3_percent} du soft skills ({', '.join(top3_labels)})."
        )
    if max_index_region and max_index_value is not None:
        lines.append(
            f"Index max : {format_float(max_index_value, 2)} ({max_index_region})."
        )

    decision = "OUI - Créer messaging spécifique" if (stag_perf or 0) > 0.05 else "NON - Traiter comme segment général"
    justification = []
    if stag_perf is not None and stag_perf > 0:
        justification.append(
            f"+{format_percent(stag_perf)} de stagiaires vs autres TAM"
        )
    if prod_perf is not None and prod_perf > 0:
        justification.append(
            f"+{format_percent(prod_perf)} de production estimée"
        )
    if max_index_value and max_index_value > 1.1:
        justification.append(
            f"forte concentration régionale ({format_float(max_index_value, 2)} en {max_index_region})"
        )
    if not justification:
        justification.append("performance similaire au reste du TAM")

    lines.append("")
    lines.append("### Décision segment dédié")
    lines.append(f"Décision : {decision}")
    lines.append("Justification : " + ", ".join(justification) + ".")

    with open(OUTPUT_MARKDOWN, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # CSV export
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "numeroDeclarationActivite",
                "denomination",
                "region",
                "categories_soft",
                "nb_stagiaires",
                "effectif_formateurs",
                "production_estimee",
            ]
        )
        for rec in soft_tam:
            categories = ", ".join(rec.soft_categories)
            if rec.region_code is None:
                region_name = ""
            else:
                region_name = REGION_NAMES.get(rec.region_code, str(rec.region_code))
            writer.writerow(
                [
                    rec.nda,
                    rec.denomination,
                    region_name,
                    categories,
                    f"{rec.nb_stagiaires or 0:.0f}",
                    rec.effectif or 0,
                    f"{production_estimee(rec):.2f}",
                ]
            )


if __name__ == "__main__":
    analyse()

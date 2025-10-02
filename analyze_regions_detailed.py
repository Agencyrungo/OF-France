import os
import statistics
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from compute_tam import (
    NS,
    column_ref_to_index,
    get_cell_value,
    load_shared_strings,
)

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"

COL_REGION = 8
COL_ACTIONS = 9
COL_SPECIALITE1 = 22
COL_SPECIALITE2 = 24
COL_SPECIALITE3 = 26
COL_NB_STAGIAIRES = 27
COL_EFFECTIF = 29
COL_CODE_POSTAL = 6
COL_VILLE = 7

TARGET_MIN = 3
TARGET_MAX = 10

REGION_NAMES = {
    "11": "Île-de-France",
    "84": "Auvergne-Rhône-Alpes",
    "76": "Occitanie",
    "93": "Provence-Alpes-Côte d'Azur",
    "75": "Nouvelle-Aquitaine",
    "44": "Grand Est",
    "52": "Pays de la Loire",
    "32": "Hauts-de-France",
    "53": "Bretagne",
    "28": "Normandie",
    "27": "Bourgogne-Franche-Comté",
    "24": "Centre-Val de Loire",
    "94": "Corse",
}

REGION_ORDER = [
    "11",
    "84",
    "76",
    "93",
    "75",
    "44",
    "52",
    "32",
    "53",
    "28",
    "27",
    "24",
    "94",
    "DOM-TOM",
]

SOFT_KEYWORDS = [
    "SOFT",
    "DEVELOPPEMENT DES CAPACITES",
    "DEVELOPPEMENT PERSONNEL",
    "COMPETENCES TRANSVERSALES",
    "COMMUNICATION",
    "MANAGEMENT",
    "SAVOIRS DE BASE",
]

MACRO_ZONES = {
    "Grand Bassin Parisien": {
        "regions": ["11", "24", "28"],
        "comment": "Hyper-concentration",
    },
    "Grand Sud-Est": {
        "regions": ["84", "93", "94"],
        "comment": "Dynamisme",
    },
    "Grand Sud-Ouest": {
        "regions": ["76", "75"],
        "comment": "Émergence",
    },
    "Grand Est": {
        "regions": ["44", "27"],
        "comment": "Transfrontalier",
    },
    "Grand Ouest": {
        "regions": ["53", "52"],
        "comment": "Équilibre",
    },
    "Grand Nord": {
        "regions": ["32"],
        "comment": "Industrie",
    },
    "Outre-mer": {
        "regions": ["DOM-TOM"],
        "comment": "Insularité",
    },
}


@dataclass
class RegionMetrics:
    code: str
    name: str
    base_total: int = 0
    cp_filled: int = 0
    of_3_10: int = 0
    certified: int = 0
    tam_total: int = 0
    tam_stag_sum: float = 0.0
    tam_actions_sum: float = 0.0
    tam_effectif_sum: int = 0
    tam_stag_list: List[float] = field(default_factory=list)
    tam_actions_list: List[float] = field(default_factory=list)
    tam_effectif_list: List[int] = field(default_factory=list)
    tam_distribution: Counter = field(default_factory=Counter)
    departments: Counter = field(default_factory=Counter)
    cities: Counter = field(default_factory=Counter)
    specialities: Counter = field(default_factory=Counter)
    soft_skills: int = 0

    def record_cp(self, has_cp: bool):
        self.base_total += 1
        if has_cp:
            self.cp_filled += 1


SOFT_CODES_PREFIXES = {"15", "14"}


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
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def normalize_region(code: Optional[int]) -> str:
    if code is None:
        return "DOM-TOM"
    code_str = str(code)
    if code_str in REGION_NAMES:
        return code_str
    return "DOM-TOM"


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


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


def format_percent(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.{decimals}f}%".replace(",", " ")


def classify_maturity(rate: Optional[float]) -> str:
    if rate is None:
        return "Faible"
    if rate >= 0.80:
        return "Très élevée"
    if rate >= 0.65:
        return "Élevée"
    if rate >= 0.45:
        return "Moyenne"
    return "Faible"


def is_soft_speciality(code: Optional[str], label: Optional[str]) -> bool:
    if code:
        code_text = str(code).strip()
        if any(code_text.startswith(prefix) for prefix in SOFT_CODES_PREFIXES):
            return True
    if label:
        upper = label.upper()
        return any(keyword in upper for keyword in SOFT_KEYWORDS)
    return False


def extract_department(code_postal: Optional[str]) -> Optional[str]:
    if not code_postal:
        return None
    text = code_postal.strip()
    if not text:
        return None
    if len(text) >= 2:
        if text[:2].isdigit():
            return text[:2]
    if len(text) >= 3 and text[:3].isdigit():
        return text[:3]
    return None


def format_city(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    cleaned = name.strip()
    if not cleaned:
        return None
    return cleaned.title()


def load_region_metrics() -> Dict[str, RegionMetrics]:
    metrics: Dict[str, RegionMetrics] = {
        code: RegionMetrics(code=code, name=name)
        for code, name in REGION_NAMES.items()
    }
    metrics["DOM-TOM"] = RegionMetrics(code="DOM-TOM", name="DOM-TOM")

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

                code_region = normalize_region(parse_int(values.get(COL_REGION)))
                metric = metrics[code_region]

                code_postal = values.get(COL_CODE_POSTAL)
                metric.record_cp(bool(code_postal and str(code_postal).strip()))

                effectif = parse_int(values.get(COL_EFFECTIF))
                if effectif is not None and TARGET_MIN <= effectif <= TARGET_MAX:
                    metric.of_3_10 += 1

                actions = parse_float(values.get(COL_ACTIONS))
                nb_stagiaires = parse_float(values.get(COL_NB_STAGIAIRES))

                if effectif is not None and TARGET_MIN <= effectif <= TARGET_MAX:
                    if actions is not None and actions > 0:
                        metric.certified += 1
                        if nb_stagiaires is not None and nb_stagiaires > 0:
                            metric.tam_total += 1
                            metric.tam_stag_sum += nb_stagiaires
                            metric.tam_actions_sum += actions
                            metric.tam_effectif_sum += effectif
                            metric.tam_stag_list.append(nb_stagiaires)
                            metric.tam_actions_list.append(actions)
                            metric.tam_effectif_list.append(effectif)

                            if effectif <= 5:
                                metric.tam_distribution["3-5"] += 1
                            elif effectif <= 8:
                                metric.tam_distribution["6-8"] += 1
                            else:
                                metric.tam_distribution["9-10"] += 1

                            department = extract_department(code_postal)
                            if department:
                                metric.departments[department] += 1

                            city = format_city(values.get(COL_VILLE))
                            if city:
                                metric.cities[city] += 1

                            speciality_pairs = []
                            for code_idx, label_idx in [
                                (21, COL_SPECIALITE1),
                                (23, COL_SPECIALITE2),
                                (25, COL_SPECIALITE3),
                            ]:
                                code_value = values.get(code_idx)
                                label_value = values.get(label_idx)
                                code_clean = code_value.strip() if code_value else None
                                label_clean = label_value.strip() if label_value else None
                                if code_clean or label_clean:
                                    speciality_pairs.append((code_clean, label_clean))

                            seen_labels = set()
                            for _, label in speciality_pairs:
                                if label and label not in seen_labels:
                                    metric.specialities[label] += 1
                                    seen_labels.add(label)

                            soft_flag = any(
                                is_soft_speciality(code, label)
                                for code, label in speciality_pairs
                            )
                            if soft_flag:
                                metric.soft_skills += 1

                elem.clear()

    return metrics


def compute_region_profiles(metrics: Dict[str, RegionMetrics]):
    total_base = sum(m.base_total for m in metrics.values())
    total_tam = sum(m.tam_total for m in metrics.values())
    total_stagiaires = sum(m.tam_stag_sum for m in metrics.values())
    total_actions = sum(m.tam_actions_sum for m in metrics.values())
    total_effectif = sum(m.tam_effectif_sum for m in metrics.values())
    total_of_3_10 = sum(m.of_3_10 for m in metrics.values())
    total_certified = sum(m.certified for m in metrics.values())

    national_mean_stag = (total_stagiaires / total_tam) if total_tam else 0
    national_mean_actions = (total_actions / total_tam) if total_tam else 0
    national_mean_effectif = (total_effectif / total_tam) if total_tam else 0
    national_qual_rate = (total_certified / total_of_3_10) if total_of_3_10 else 0

    derived = {}
    for code, metric in metrics.items():
        base_share = (metric.base_total / total_base) if total_base else 0
        tam_share = (metric.tam_total / total_tam) if total_tam else 0
        stag_share = (metric.tam_stag_sum / total_stagiaires) if total_stagiaires else 0
        stag_mean = safe_mean(metric.tam_stag_list)
        stag_median = safe_median(metric.tam_stag_list)
        actions_mean = safe_mean(metric.tam_actions_list)
        effectif_mean = safe_mean(metric.tam_effectif_list)
        cert_rate = (metric.certified / metric.of_3_10) if metric.of_3_10 else None
        cp_rate = (metric.cp_filled / metric.base_total) if metric.base_total else None
        production_month = (actions_mean / 12) if actions_mean is not None else None
        top_specs = metric.specialities.most_common(3)
        top_deps = metric.departments.most_common(3)
        top_cities = metric.cities.most_common(3)

        derived[code] = {
            "metric": metric,
            "base_share": base_share,
            "tam_share": tam_share,
            "stag_share": stag_share,
            "stag_mean": stag_mean,
            "stag_median": stag_median,
            "actions_mean": actions_mean,
            "effectif_mean": effectif_mean,
            "cert_rate": cert_rate,
            "cp_rate": cp_rate,
            "production_month": production_month,
            "top_specs": top_specs,
            "top_deps": top_deps,
            "top_cities": top_cities,
            "soft_pct": (metric.soft_skills / metric.tam_total) if metric.tam_total else None,
            "national_mean_stag": national_mean_stag,
            "national_mean_actions": national_mean_actions,
            "national_mean_effectif": national_mean_effectif,
            "national_cert_rate": national_qual_rate,
        }

    rankings = sorted(metrics.values(), key=lambda m: m.tam_total, reverse=True)
    rank_map = {m.code: idx + 1 for idx, m in enumerate(rankings)}

    return {
        "derived": derived,
        "totals": {
            "base": total_base,
            "tam": total_tam,
            "stagiaires": total_stagiaires,
            "actions": total_actions,
            "effectif": total_effectif,
            "qual_rate": national_qual_rate,
        },
        "rank": rank_map,
    }


def build_opportunity(insight_data) -> Tuple[str, str]:
    tam_share = insight_data["tam_share"]
    stag_mean = insight_data["stag_mean"] or 0
    national_mean_stag = insight_data["national_mean_stag"]
    cert_rate = insight_data["cert_rate"]
    national_cert_rate = insight_data["national_cert_rate"]
    top_specs = insight_data["top_specs"]
    top_cities = insight_data["top_cities"]

    if tam_share >= 0.15:
        insight = f"Poids lourd national avec {tam_share * 100:.1f}% du TAM."
        if top_cities:
            action = f"Structurer un pilotage dédié sur {top_cities[0][0]}."
        else:
            action = "Mettre en place une gouvernance régionale renforcée."
        return insight, action

    if cert_rate is not None and cert_rate < national_cert_rate:
        delta = (national_cert_rate - cert_rate) * 100
        insight = f"Taux Qualiopi à combler (-{delta:.1f} pts vs national)."
        if top_specs:
            action = f"Lancer un plan d'accompagnement Qualiopi sur {top_specs[0][0].lower()}."
        else:
            action = "Déployer un programme d'appui à la certification."
        return insight, action

    if stag_mean and stag_mean > national_mean_stag * 1.1:
        insight = f"Intensité stagiaires supérieure (+{(stag_mean / national_mean_stag - 1) * 100:.0f}%)."
        if top_cities:
            action = f"Capitaliser via des hubs multi-OF à {top_cities[0][0]}."
        else:
            action = "Créer des partenariats territoriaux pour absorber la demande."
        return insight, action

    if top_specs:
        insight = f"Marché équilibré dominé par {top_specs[0][0].lower()}."
        action = f"Développer des offres différenciantes sur {top_specs[0][0].lower()}."
    else:
        insight = "Marché diffus sans spécialité dominante."
        action = "Renforcer la collecte d'informations sectorielles."
    return insight, action


def write_region_fiches(derived_data, rank_map, totals):
    lines: List[str] = []
    lines.append("# Cartographie détaillée des régions")
    lines.append("")
    for code in REGION_ORDER:
        if code not in derived_data:
            continue
        data = derived_data[code]
        metric: RegionMetrics = data["metric"]
        rank = rank_map.get(code, len(derived_data))

        lines.append(f"## Région : {metric.name} (code {metric.code})")
        lines.append("")
        lines.append("### Démographie OF")
        lines.append(f"- Total OF base : {format_number(metric.base_total)} ({format_percent(data['base_share'])} France)")
        region_base_pct = (metric.of_3_10 / metric.base_total) if metric.base_total else None
        lines.append(
            f"- OF 3-10 formateurs : {format_number(metric.of_3_10)} "
            f"({format_percent(region_base_pct) if region_base_pct is not None else '-'} région)"
        )
        lines.append(
            f"- OF TAM qualifié : {format_number(metric.tam_total)} "
            f"({format_percent(data['tam_share'])} TAM France)"
        )
        lines.append(f"- Rang national : {rank}/14")
        lines.append("")

        lines.append("### Activité")
        lines.append(
            f"- Stagiaires total : {format_number(metric.tam_stag_sum)} "
            f"({format_percent(data['stag_share'])} France)"
        )
        lines.append(f"- Stagiaires moyen TAM : {format_number(data['stag_mean'], 1)}")
        lines.append(f"- Stagiaires médian TAM : {format_number(data['stag_median'], 1)}")
        lines.append(
            f"- Production estimée moy : {format_number(data['production_month'], 1)} livrables/mois"
        )
        lines.append("")

        lines.append("### Taille des structures")
        lines.append(f"- Effectif moyen TAM : {format_number(data['effectif_mean'], 1)} formateurs")
        lines.append("- Distribution 3-10 :")
        for label in ["3-5", "6-8", "9-10"]:
            count = metric.tam_distribution.get(label, 0)
            pct = (count / metric.tam_total) if metric.tam_total else None
            lines.append(
                f"  * {label} form : {format_number(count)} "
                f"({format_percent(pct) if pct is not None else '-'})"
            )
        lines.append("")

        lines.append("### Certification")
        lines.append(
            f"- Taux certif Qualiopi : {format_percent(data['cert_rate'])} "
            f"(vs {format_percent(data['national_cert_rate'])} national)"
        )
        lines.append(f"- OF certifiés actifs : {format_number(metric.tam_total)}")
        lines.append(f"- Maturité : {classify_maturity(data['cert_rate'])}")
        lines.append("")

        lines.append("### Spécialités dominantes")
        if data["top_specs"]:
            for idx, (label, count) in enumerate(data["top_specs"], start=1):
                pct = (count / metric.tam_total) if metric.tam_total else None
                lines.append(
                    f"{idx}. {label} : {format_number(count)} "
                    f"({format_percent(pct) if pct is not None else '-'})"
                )
        else:
            lines.append("Aucune spécialité dominante identifiée.")
        lines.append(
            f"- Soft skills : {format_number(metric.soft_skills)} "
            f"({format_percent(data['soft_pct']) if data['soft_pct'] is not None else '-' } région)"
        )
        lines.append("")

        lines.append("### Géographie")
        if data["top_deps"]:
            dep_parts = []
            for dep, count in data["top_deps"]:
                dep_parts.append(f"{dep} ({format_number(count)} OF)")
            lines.append("- Départements principaux : " + ", ".join(dep_parts))
        else:
            lines.append("- Départements principaux : -")
        if data["top_cities"]:
            city_names = [city for city, _ in data["top_cities"]]
            lines.append("- Villes principales : " + ", ".join(city_names))
        else:
            lines.append("- Villes principales : -")
        lines.append(
            f"- Complétude CP : {format_percent(data['cp_rate']) if data['cp_rate'] is not None else '-'}"
        )
        lines.append("")

        insight, action = build_opportunity(data)
        lines.append("### Opportunités")
        lines.append(f"- {insight}")
        lines.append(f"- {action}")
        lines.append("")

    path = os.path.join(OUTPUT_DIR, "region_fiches.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path


def write_benchmark_table(derived_data, rank_map):
    rows: List[List[str]] = []
    sorted_regions = sorted(
        derived_data.values(),
        key=lambda d: d["metric"].tam_total,
        reverse=True,
    )
    for idx, data in enumerate(sorted_regions, start=1):
        metric: RegionMetrics = data["metric"]
        top_spec = data["top_specs"][0][0] if data["top_specs"] else "-"
        rows.append(
            [
                str(idx),
                metric.code,
                metric.name,
                format_number(metric.tam_total),
                format_percent(data["tam_share"]),
                format_number(data["stag_mean"], 1),
                format_number(data["effectif_mean"], 1),
                format_percent(data["cert_rate"]),
                top_spec,
            ]
        )

    headers = [
        "Rang",
        "Code",
        "Nom",
        "OF_TAM",
        "% France",
        "Stag_moy",
        "Effectif_moy",
        "Taux_Qualiopi",
        "Top_spé",
    ]
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    path = os.path.join(OUTPUT_DIR, "benchmark_regions.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def perf_label(ratio: Optional[float]) -> str:
    if ratio is None or ratio == 0:
        return "-"
    diff = ratio - 1
    pct = abs(diff) * 100
    if ratio >= 1.05:
        return f"+{pct:.0f}%"
    if ratio <= 0.95:
        return f"-{pct:.0f}%"
    return "Moy"


def write_performance_table(derived_data, totals):
    national_mean_stag = (
        totals["stagiaires"] / totals["tam"] if totals["tam"] else 0
    )
    national_mean_actions = (
        totals["actions"] / totals["tam"] if totals["tam"] else 0
    )
    national_cert_rate = totals["qual_rate"]

    rows: List[List[str]] = []
    performance_records = []
    for data in derived_data.values():
        metric: RegionMetrics = data["metric"]
        stag_ratio = (
            (data["stag_mean"] / national_mean_stag)
            if national_mean_stag and data["stag_mean"]
            else None
        )
        prod_ratio = (
            (data["actions_mean"] / national_mean_actions)
            if national_mean_actions and data["actions_mean"]
            else None
        )
        maturity_ratio = (
            (data["cert_rate"] / national_cert_rate)
            if national_cert_rate and data["cert_rate"] is not None
            else 0
        )
        score = (
            data["tam_share"] * 0.4
            + (stag_ratio or 0) * 0.3
            + maturity_ratio * 0.3
        )
        performance_records.append((metric.name, score))
        rows.append(
            [
                metric.name,
                format_number(metric.tam_total),
                perf_label(stag_ratio),
                perf_label(prod_ratio),
                classify_maturity(data["cert_rate"]),
                f"{score:.2f}",
            ]
        )

    headers = [
        "Région",
        "TAM",
        "Performance_stag",
        "Performance_prod",
        "Maturité_Qualiopi",
        "Score_global",
    ]
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")

    path = os.path.join(OUTPUT_DIR, "performance_regions.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    return path, {name: score for name, score in performance_records}


def write_macro_zones(derived_data, totals):
    lines: List[str] = []
    headers = ["Macro-zone", "Régions", "Total OF", "% France", "Caractéristiques"]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for zone, cfg in MACRO_ZONES.items():
        tam_sum = sum(derived_data[code]["metric"].tam_total for code in cfg["regions"] if code in derived_data)
        share = (tam_sum / totals["tam"]) if totals["tam"] else 0
        region_labels = [
            derived_data[code]["metric"].name for code in cfg["regions"] if code in derived_data
        ]
        lines.append(
            "| "
            + " | ".join(
                [
                    zone,
                    ", ".join(region_labels),
                    format_number(tam_sum),
                    format_percent(share),
                    cfg["comment"],
                ]
            )
            + " |"
        )

    path = os.path.join(OUTPUT_DIR, "macro_zones.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def write_synthesis(derived_data, totals, scores):
    lines: List[str] = []
    lines.append("# Synthèse stratégique")
    lines.append("")
    lines.append("14 RÉGIONS ANALYSÉES")
    lines.append("")

    sorted_regions = sorted(
        derived_data.values(), key=lambda d: d["metric"].tam_total, reverse=True
    )
    lines.append("## Top 5 régions (OF TAM)")
    for idx, data in enumerate(sorted_regions[:5], start=1):
        metric: RegionMetrics = data["metric"]
        lines.append(
            f"{idx}. {metric.name} : {format_number(metric.tam_total)} OF "
            f"({format_percent(data['tam_share'])})"
        )
    lines.append("")

    lines.append("## Régions haute performance")
    high_perf = [name for name, score in scores.items() if score > 1.2]
    if high_perf:
        for name in sorted(high_perf):
            lines.append(f"- {name}")
    else:
        lines.append("- Aucune région > 1.2")
    lines.append("")

    avg_share = 1 / len(derived_data) if derived_data else 0
    opportunity_regions = []
    for data in derived_data.values():
        metric: RegionMetrics = data["metric"]
        tam_share = data["tam_share"]
        stag_ratio = (
            (data["stag_mean"] / data["national_mean_stag"])
            if data["national_mean_stag"] and data["stag_mean"]
            else None
        )
        if tam_share < avg_share and (stag_ratio and stag_ratio >= 1.05 or (data["cert_rate"] and data["cert_rate"] >= data["national_cert_rate"])):
            opportunity_regions.append(metric.name)

    if opportunity_regions:
        lines.append("## Régions opportunité")
        for name in sorted(opportunity_regions):
            lines.append(f"- {name}")
        lines.append("")

    idf_tam = derived_data.get("11", {}).get("metric").tam_total if "11" in derived_data else 0
    min_tam = min(
        (data["metric"].tam_total for code, data in derived_data.items() if data["metric"].tam_total > 0),
        default=0,
    )
    ratio = (idf_tam / min_tam) if idf_tam and min_tam else 0
    tam_values = [data["metric"].tam_total for data in derived_data.values() if data["metric"].tam_total > 0]
    coeff_var = 0
    if tam_values:
        mean_val = sum(tam_values) / len(tam_values)
        if mean_val:
            coeff_var = statistics.pstdev(tam_values) / mean_val

    lines.append("## Disparités régionales")
    lines.append(f"- Écart IDF vs région la + faible : {ratio:.1f} fois")
    lines.append(f"- Coefficient variation : {coeff_var:.2f}")

    path = os.path.join(OUTPUT_DIR, "synthese_regions.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def main():
    ensure_output_dir()
    metrics = load_region_metrics()
    summary = compute_region_profiles(metrics)
    derived = summary["derived"]
    totals = summary["totals"]
    rank_map = summary["rank"]

    write_region_fiches(derived, rank_map, totals)
    write_benchmark_table(derived, rank_map)
    perf_path, scores = write_performance_table(derived, totals)
    write_macro_zones(derived, totals)
    write_synthesis(derived, totals, scores)


if __name__ == "__main__":
    main()

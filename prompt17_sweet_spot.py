import csv
import math
import os
import statistics
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from analyze_specialites import MACRO_THEMES, classify_specialite

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_MARKDOWN = os.path.join("analysis_outputs", "prompt17_sweet_spot.md")
OUTPUT_CSV_TEMPLATE = os.path.join("analysis_outputs", "prompt17_segment_{segment}.csv")
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

SEGMENTS = {
    "A": {"label": "3 formateurs", "min": 3, "max": 3},
    "B": {"label": "4-5 formateurs", "min": 4, "max": 5},
    "C": {"label": "6-10 formateurs", "min": 6, "max": 10},
}

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

REGION_ORDER = [11, 84, 76, 93, 75, 44, 52, 32, 53, 28, 27, 24, 94, 1, 2, 3, 4, 6]


@dataclass
class Record:
    numero: str
    denomination: str
    effectif: Optional[int]
    nb_stagiaires: float
    qualiopi_actions: Optional[int]
    qualiopi_bilan: Optional[int]
    qualiopi_vae: Optional[int]
    qualiopi_apprentissage: Optional[int]
    region_code: Optional[int]
    specialite: Optional[str]

    @property
    def segment(self) -> Optional[str]:
        if self.effectif is None:
            return None
        for key, spec in SEGMENTS.items():
            if spec["min"] <= self.effectif <= spec["max"]:
                return key
        return None

    @property
    def stagiaires_par_formateur(self) -> Optional[float]:
        if self.effectif in (None, 0):
            return None
        return self.nb_stagiaires / self.effectif

    @property
    def production_estimee(self) -> Optional[float]:
        if self.effectif is None or self.nb_stagiaires is None:
            return None
        stag_mois = self.nb_stagiaires / 12.0
        return (stag_mois / 20.0) + (self.effectif * 2.0)

    @property
    def has_multi_cert(self) -> bool:
        if self.qualiopi_actions != 1:
            return False
        return any(
            cert == 1
            for cert in [self.qualiopi_bilan, self.qualiopi_vae, self.qualiopi_apprentissage]
        )

    @property
    def macro_theme(self) -> str:
        return classify_specialite(self.specialite)


def ensure_output_dir() -> None:
    os.makedirs(os.path.dirname(OUTPUT_MARKDOWN), exist_ok=True)


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
                "numero": None,
                "denomination": None,
                "effectif": None,
                "stagiaires": None,
                "actions": None,
                "bilan": None,
                "vae": None,
                "apprentissage": None,
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
                    header_lookup = {
                        "numero": "numeroDeclarationActivite",
                        "denomination": "denomination",
                        "effectif": "informationsDeclarees.effectifFormateurs",
                        "stagiaires": "informationsDeclarees.nbStagiaires",
                        "actions": "certifications.actionsDeFormation",
                        "bilan": "certifications.bilansDeCompetences",
                        "vae": "certifications.VAE",
                        "apprentissage": "certifications.actionsDeFormationParApprentissage",
                        "region": "adressePhysiqueOrganismeFormation.codeRegion",
                        "specialite": "informationsDeclarees.specialitesDeFormation.libelleSpecialite1",
                    }
                    for key, header_name in header_lookup.items():
                        target_indices[key] = next(
                            (idx for idx, name in header_map.items() if name == header_name),
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
                numero = values.get(target_indices["numero"], "").strip()
                denomination = values.get(target_indices["denomination"], "").strip()
                effectif = parse_int(values.get(target_indices["effectif"]))
                nb_stagiaires = parse_float(values.get(target_indices["stagiaires"])) or 0.0
                qualiopi_actions = parse_int(values.get(target_indices["actions"]))
                qualiopi_bilan = parse_int(values.get(target_indices["bilan"]))
                qualiopi_vae = parse_int(values.get(target_indices["vae"]))
                qualiopi_apprentissage = parse_int(values.get(target_indices["apprentissage"]))
                region_code = parse_int(values.get(target_indices["region"]))
                specialite_raw = values.get(target_indices["specialite"])
                specialite = specialite_raw.strip() if specialite_raw else None
                records.append(
                    Record(
                        numero=numero,
                        denomination=denomination,
                        effectif=effectif,
                        nb_stagiaires=nb_stagiaires,
                        qualiopi_actions=qualiopi_actions,
                        qualiopi_bilan=qualiopi_bilan,
                        qualiopi_vae=qualiopi_vae,
                        qualiopi_apprentissage=qualiopi_apprentissage,
                        region_code=region_code,
                        specialite=specialite,
                    )
                )
                elem.clear()
    return records


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


def compute_segment_metrics(records: List[Record]) -> Dict[str, Dict[str, float]]:
    segments: Dict[str, Dict[str, float]] = {}
    grouped: Dict[str, List[Record]] = {key: [] for key in SEGMENTS}
    for rec in records:
        seg = rec.segment
        if seg:
            grouped[seg].append(rec)
    total = sum(len(items) for items in grouped.values())
    overall_soft = sum(1 for rec in records if rec.macro_theme == "Soft Skills")
    for key, items in grouped.items():
        count = len(items)
        stag_values = [rec.nb_stagiaires for rec in items]
        stag_mean = sum(stag_values) / count if count else 0.0
        stag_median = statistics.median(stag_values) if count else 0.0
        total_effectif = sum(rec.effectif or 0 for rec in items)
        stag_per_form = (sum(stag_values) / total_effectif) if total_effectif else 0.0
        prod_values = [rec.production_estimee for rec in items if rec.production_estimee is not None]
        prod_mean = sum(prod_values) / len(prod_values) if prod_values else 0.0
        soft_count = sum(1 for rec in items if rec.macro_theme == "Soft Skills")
        multi_cert = sum(1 for rec in items if rec.has_multi_cert)
        segments[key] = {
            "count": count,
            "pct_tam": (count / total * 100) if total else 0.0,
            "stag_mean": stag_mean,
            "stag_median": stag_median,
            "stag_per_form": stag_per_form,
            "production": prod_mean,
            "soft_share": (soft_count / count * 100) if count else 0.0,
            "cert_rate": (multi_cert / count * 100) if count else 0.0,
        }
    return segments


def normalize_scores(values: Dict[str, float]) -> Dict[str, float]:
    if not values:
        return {}
    min_val = min(values.values())
    max_val = max(values.values())
    if math.isclose(min_val, max_val):
        return {k: 10.0 for k in values}
    return {k: 10.0 * (v - min_val) / (max_val - min_val) for k, v in values.items()}


def compute_score_global(metrics: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    criteria = ["count", "stag_mean", "stag_median", "stag_per_form", "production", "soft_share", "cert_rate"]
    scores: Dict[str, float] = {key: 0.0 for key in metrics}
    for criterion in criteria:
        values = {key: metrics[key][criterion] for key in metrics}
        normalized = normalize_scores(values)
        for key, score in normalized.items():
            scores[key] += score
    num_criteria = len(criteria)
    return {key: (value / num_criteria) for key, value in scores.items()}


def render_table(headers: List[str], rows: List[List[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n" + "\n".join(lines) + "\n"


def format_int(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def format_float(value: float, decimals: int = 1) -> str:
    return f"{value:,.{decimals}f}".replace(",", " ")


def format_percent(value: float, decimals: int = 1) -> str:
    return f"{value:.{decimals}f}%"


def format_best_segment(metrics: Dict[str, float]) -> str:
    if not metrics:
        return "-"
    max_value = max(metrics.values())
    winners = [key for key, value in metrics.items() if math.isclose(value, max_value)]
    return "/".join(winners)


def build_table1(metrics: Dict[str, Dict[str, float]]) -> str:
    score_global = compute_score_global(metrics)
    for key in metrics:
        metrics[key]["score_global"] = score_global.get(key, 0.0)
    headers = [
        "Métrique",
        "Segment A (3)",
        "Segment B (4-5)",
        "Segment C (6-10)",
        "Meilleur",
    ]
    rows = []
    rows.append([
        "Nombre OF",
        format_int(int(metrics["A"]["count"])),
        format_int(int(metrics["B"]["count"])),
        format_int(int(metrics["C"]["count"])),
        format_best_segment({k: metrics[k]["count"] for k in metrics}),
    ])
    rows.append([
        "% TAM",
        format_percent(metrics["A"]["pct_tam"]),
        format_percent(metrics["B"]["pct_tam"]),
        format_percent(metrics["C"]["pct_tam"]),
        format_best_segment({k: metrics[k]["pct_tam"] for k in metrics}),
    ])
    rows.append([
        "Stagiaires moyen",
        format_float(metrics["A"]["stag_mean"], 1),
        format_float(metrics["B"]["stag_mean"], 1),
        format_float(metrics["C"]["stag_mean"], 1),
        format_best_segment({k: metrics[k]["stag_mean"] for k in metrics}),
    ])
    rows.append([
        "Stagiaires médian",
        format_float(metrics["A"]["stag_median"], 1),
        format_float(metrics["B"]["stag_median"], 1),
        format_float(metrics["C"]["stag_median"], 1),
        format_best_segment({k: metrics[k]["stag_median"] for k in metrics}),
    ])
    rows.append([
        "Stagiaires / formateur",
        format_float(metrics["A"]["stag_per_form"], 1),
        format_float(metrics["B"]["stag_per_form"], 1),
        format_float(metrics["C"]["stag_per_form"], 1),
        format_best_segment({k: metrics[k]["stag_per_form"] for k in metrics}),
    ])
    rows.append([
        "Production estimée (livr./mois)",
        format_float(metrics["A"]["production"], 2),
        format_float(metrics["B"]["production"], 2),
        format_float(metrics["C"]["production"], 2),
        format_best_segment({k: metrics[k]["production"] for k in metrics}),
    ])
    rows.append([
        "% Soft Skills",
        format_percent(metrics["A"]["soft_share"], 1),
        format_percent(metrics["B"]["soft_share"], 1),
        format_percent(metrics["C"]["soft_share"], 1),
        format_best_segment({k: metrics[k]["soft_share"] for k in metrics}),
    ])
    rows.append([
        "Taux multi-certif",
        format_percent(metrics["A"]["cert_rate"], 1),
        format_percent(metrics["B"]["cert_rate"], 1),
        format_percent(metrics["C"]["cert_rate"], 1),
        format_best_segment({k: metrics[k]["cert_rate"] for k in metrics}),
    ])
    rows.append([
        "Score global (0-10)",
        format_float(score_global.get("A", 0.0), 2),
        format_float(score_global.get("B", 0.0), 2),
        format_float(score_global.get("C", 0.0), 2),
        format_best_segment(score_global),
    ])
    return "### Tableau 1 : Comparaison segments" + render_table(headers, rows)


def build_table2(records: List[Record]) -> Tuple[str, Dict[int, Dict[str, int]]]:
    region_segment_counts: Dict[int, Dict[str, int]] = defaultdict(lambda: {key: 0 for key in SEGMENTS})
    for rec in records:
        if rec.region_code is None:
            continue
        seg = rec.segment
        if not seg:
            continue
        region_segment_counts[rec.region_code][seg] += 1
    rows: List[List[str]] = []
    headers = ["Région", "Segment A", "Segment B", "Segment C", "Dominant"]
    for code in REGION_ORDER:
        counts = region_segment_counts.get(code)
        if not counts:
            continue
        name = REGION_NAMES.get(code, str(code))
        dominant = format_best_segment(counts) if sum(counts.values()) else "-"
        rows.append(
            [
                name,
                format_int(counts.get("A", 0)),
                format_int(counts.get("B", 0)),
                format_int(counts.get("C", 0)),
                dominant,
            ]
        )
    return "### Tableau 2 : Segments par région" + render_table(headers, rows), region_segment_counts


def build_table3(records: List[Record]) -> str:
    segment_counts: Dict[str, Counter] = {key: Counter() for key in SEGMENTS}
    for rec in records:
        seg = rec.segment
        if not seg:
            continue
        segment_counts[seg][rec.macro_theme] += 1
    rows: List[List[str]] = []
    headers = ["Macro-thème", "% dans A", "% dans B", "% dans C", "Sur-représenté"]
    for theme in MACRO_THEMES:
        row_values = {}
        for seg in SEGMENTS:
            total = sum(segment_counts[seg].values())
            share = (segment_counts[seg][theme] / total * 100) if total else 0.0
            row_values[seg] = share
        rows.append(
            [
                theme,
                format_percent(row_values["A"], 1),
                format_percent(row_values["B"], 1),
                format_percent(row_values["C"], 1),
                format_best_segment(row_values),
            ]
        )
    return "### Tableau 3 : Macro-thèmes par segment" + render_table(headers, rows)


def build_table4(records: List[Record]) -> str:
    bins = [
        ("<5 livrables", None, 5.0),
        ("5-10 livrables", 5.0, 10.0),
        ("10-15 livrables", 10.0, 15.0),
        ("15+ livrables", 15.0, None),
    ]
    headers = ["Tranche prod", "Segment A", "Segment B", "Segment C"]
    rows: List[List[str]] = []
    segment_groups: Dict[str, List[Record]] = {key: [] for key in SEGMENTS}
    for rec in records:
        seg = rec.segment
        if not seg:
            continue
        segment_groups[seg].append(rec)
    for label, lower, upper in bins:
        row: List[str] = [label]
        for seg in SEGMENTS:
            items = [rec for rec in segment_groups[seg] if rec.production_estimee is not None]
            if not items:
                row.append("0.0%")
                continue
            filtered = []
            for rec in items:
                prod = rec.production_estimee
                if prod is None:
                    continue
                if lower is not None and prod < lower:
                    continue
                if upper is not None and prod >= upper:
                    continue
                filtered.append(rec)
            share = (len(filtered) / len(items) * 100) if items else 0.0
            row.append(format_percent(share, 1))
        rows.append(row)
    return "### Tableau 4 : Distribution production par segment" + render_table(headers, rows)


def build_table5(records: List[Record]) -> Tuple[str, Dict[str, float]]:
    headers = ["Segment", "OF ≥500 stag", "% segment", "vs autres"]
    rows: List[List[str]] = []
    segment_groups: Dict[str, List[Record]] = {key: [] for key in SEGMENTS}
    for rec in records:
        seg = rec.segment
        if seg:
            segment_groups[seg].append(rec)
    overall_high = sum(1 for rec in records if rec.nb_stagiaires >= 500)
    overall_total = len(records)
    share_by_segment: Dict[str, float] = {}
    for seg, items in segment_groups.items():
        high = [rec for rec in items if rec.nb_stagiaires >= 500]
        count_high = len(high)
        pct_segment = (count_high / len(items) * 100) if items else 0.0
        share_by_segment[seg] = (count_high / overall_high * 100) if overall_high else 0.0
        if seg == "B":
            base_share = (len(items) / overall_total * 100) if overall_total else 0.0
            delta = share_by_segment[seg] - base_share
            label = "Sur-représenté" if delta > 0 else "Sous-représenté" if delta < 0 else "Équivalent"
        elif seg == "A":
            label = "-"
        else:
            label = "-"
        rows.append([
            f"{seg} ({SEGMENTS[seg]['label']})",
            format_int(count_high),
            format_percent(pct_segment, 1),
            label,
        ])
    return "### Tableau 5 : Haute activité par segment" + render_table(headers, rows), share_by_segment


def compute_concentration(region_counts: Dict[int, Dict[str, int]]) -> Dict[str, float]:
    concentration: Dict[str, float] = {key: 0.0 for key in SEGMENTS}
    totals: Dict[str, int] = {key: 0 for key in SEGMENTS}
    for counts in region_counts.values():
        for seg in SEGMENTS:
            totals[seg] += counts.get(seg, 0)
    for seg in SEGMENTS:
        top_regions = sorted(
            ((code, counts.get(seg, 0)) for code, counts in region_counts.items()),
            key=lambda item: item[1],
            reverse=True,
        )[:3]
        top_sum = sum(count for _, count in top_regions)
        total = totals[seg]
        concentration[seg] = (top_sum / total * 100) if total else 0.0
    return concentration


def build_table6(metrics: Dict[str, Dict[str, float]], concentration: Dict[str, float]) -> Tuple[str, Dict[str, float]]:
    criteria = {
        "Nombre OF (TAM)": (30, {seg: metrics[seg]["count"] for seg in SEGMENTS}),
        "Stagiaires moyen": (25, {seg: metrics[seg]["stag_mean"] for seg in SEGMENTS}),
        "Production estimée": (25, {seg: metrics[seg]["production"] for seg in SEGMENTS}),
        "% Soft Skills": (10, {seg: metrics[seg]["soft_share"] for seg in SEGMENTS}),
        "Concentration géo": (10, concentration),
    }
    score_components: Dict[str, Dict[str, float]] = {seg: {} for seg in SEGMENTS}
    weighted_scores: Dict[str, float] = {seg: 0.0 for seg in SEGMENTS}
    for criterion, (weight, values) in criteria.items():
        normalized = normalize_scores(values)
        for seg in SEGMENTS:
            score = normalized.get(seg, 0.0)
            score_components[seg][criterion] = score
            weighted_scores[seg] += score * (weight / 100)
    headers = ["Critère", "Poids", "Segment A", "Segment B", "Segment C", "Gagnant"]
    rows: List[List[str]] = []
    for criterion, (weight, values) in criteria.items():
        rows.append(
            [
                criterion,
                f"{weight}%",
                format_float(score_components["A"].get(criterion, 0.0), 2),
                format_float(score_components["B"].get(criterion, 0.0), 2),
                format_float(score_components["C"].get(criterion, 0.0), 2),
                format_best_segment(normalize_scores(values)),
            ]
        )
    rows.append(
        [
            "SCORE PONDÉRÉ",
            "100%",
            format_float(weighted_scores["A"], 2),
            format_float(weighted_scores["B"], 2),
            format_float(weighted_scores["C"], 2),
            format_best_segment(weighted_scores),
        ]
    )
    return "### Tableau 6 : Matrice décision" + render_table(headers, rows), weighted_scores


def build_table7(segment_key: str, metrics: Dict[str, Dict[str, float]], region_counts: Dict[int, Dict[str, int]], records: List[Record]) -> str:
    segment_records = [rec for rec in records if rec.segment == segment_key]
    segment_metrics = metrics[segment_key]
    total_records = sum(len([rec for rec in records if rec.segment == seg]) for seg in SEGMENTS)
    tam_share = (segment_metrics["count"] / total_records * 100) if total_records else 0.0
    avg_production = segment_metrics["production"]
    macro_counter = Counter(rec.macro_theme for rec in segment_records)
    top_macro = macro_counter.most_common(1)[0][0] if macro_counter else "-"
    region_counter = Counter()
    for code, counts in region_counts.items():
        region_counter[code] += counts.get(segment_key, 0)
    top_region_code, top_region_count = (region_counter.most_common(1)[0] if region_counter else (None, 0))
    top_region = REGION_NAMES.get(top_region_code, str(top_region_code)) if top_region_code is not None else "-"
    high_activity_share = (
        sum(1 for rec in segment_records if rec.nb_stagiaires >= 500) / len(segment_records) * 100
        if segment_records
        else 0.0
    )
    headers = ["Caractéristique", "Valeur", "Source"]
    rows = [
        ["Effectif", SEGMENTS[segment_key]["label"], "Segment"],
        ["Nombre OF", format_int(int(segment_metrics["count"])), "Analyse 1"],
        ["% TAM total", format_percent(tam_share, 1), "Analyse 1"],
        ["Stagiaires moyen", format_float(segment_metrics["stag_mean"], 1), "Analyse 1"],
        ["Production moy", format_float(avg_production, 2), "Analyse 1"],
        ["Spécialité dominante", top_macro, "Analyse 3"],
        ["Région dominante", top_region, "Analyse 2"],
        ["% haute activité", format_percent(high_activity_share, 1), "Analyse 5"],
    ]
    title = f"### Tableau 7 : Profil type {SEGMENTS[segment_key]['label']}"
    return title + render_table(headers, rows)


def analyze_region_bias(region_counts: Dict[int, Dict[str, int]]) -> List[Tuple[str, float, float]]:
    total_by_region: Dict[int, int] = {}
    b_counts: Dict[int, int] = {}
    for code, counts in region_counts.items():
        total = sum(counts.get(seg, 0) for seg in SEGMENTS)
        if total == 0:
            continue
        total_by_region[code] = total
        b_counts[code] = counts.get("B", 0)
    overall_b = sum(b_counts.values())
    overall_total = sum(total_by_region.values())
    overall_share = (overall_b / overall_total * 100) if overall_total else 0.0
    deltas: List[Tuple[str, float, float]] = []
    for code, total in total_by_region.items():
        share = (b_counts.get(code, 0) / total * 100) if total else 0.0
        delta = share - overall_share
        name = REGION_NAMES.get(code, str(code))
        deltas.append((name, share, delta))
    deltas.sort(key=lambda item: item[2], reverse=True)
    return deltas


def build_summary(
    winner: str,
    metrics: Dict[str, Dict[str, float]],
    region_bias: List[Tuple[str, float, float]],
    high_activity_shares: Dict[str, float],
) -> str:
    lines: List[str] = []
    winner_label = SEGMENTS[winner]["label"]
    lines.append(f"SWEET SPOT VALIDÉ : Segment {winner}")
    lines.append("")
    lines.append("Justification :")
    lines.append(
        f"- Nombre OF : {format_int(int(metrics[winner]['count']))} ({format_percent(metrics[winner]['pct_tam'], 1)} TAM)"
    )
    lines.append(
        f"- Activité : {format_float(metrics[winner]['stag_mean'], 1)} stagiaires/an (médiane {format_float(metrics[winner]['stag_median'], 1)})"
    )
    lines.append(
        f"- Production : {format_float(metrics[winner]['production'], 2)} livr./mois moyens"
    )
    lines.append(
        f"- Soft skills : {format_percent(metrics[winner]['soft_share'], 1)} du segment"
    )
    lines.append("")
    lines.append("Comparaison segments :")
    lines.append(
        f"- 3 formateurs : {format_int(int(metrics['A']['count']))} OF, {format_float(metrics['A']['stag_mean'], 1)} stag/an, {format_float(metrics['A']['production'], 2)} livr./mois"
    )
    lines.append(
        f"- 4-5 formateurs : {format_int(int(metrics['B']['count']))} OF, {format_float(metrics['B']['stag_mean'], 1)} stag/an, {format_float(metrics['B']['production'], 2)} livr./mois"
    )
    lines.append(
        f"- 6-10 formateurs : {format_int(int(metrics['C']['count']))} OF, {format_float(metrics['C']['stag_mean'], 1)} stag/an, {format_float(metrics['C']['production'], 2)} livr./mois"
    )
    lines.append("")
    if winner == "B":
        lines.append("DÉCISION AVATAR V2 :")
        lines.append(f"✅ Affiner cible sur {winner_label}")
        lines.append(
            f"✅ TAM affiné : {format_int(int(metrics[winner]['count']))} OF ({format_percent(metrics[winner]['pct_tam'], 1)} TAM)"
        )
        lines.append("✅ Messaging : \"OF 4-5 formateurs\"")
    else:
        lines.append("DÉCISION AVATAR V2 :")
        lines.append("⚠️ Garder cible 3-10 large")
        lines.append("⚠️ Pas d'avantage net 4-5")
        top_regions = [item for item in region_bias if item[2] > 3][:3]
        if top_regions:
            regions_text = ", ".join(
                f"{name} (+{format_percent(delta, 1)})" for name, _, delta in top_regions
            )
            lines.append(
                f"⚠️ Segment 4-5 surtout présent dans {regions_text}"
            )
        b_share_high = high_activity_shares.get("B", 0.0)
        lines.append(
            f"⚠️ Segment 4-5 = {format_percent(b_share_high, 1)} des OF ≥500 stag (vs {format_percent(metrics['B']['pct_tam'], 1)} du TAM)"
        )
    lines.append("")
    lines.append("Actions :")
    lines.append("- Ajuster documents marketing selon résultat")
    lines.append("- Recalculer TAM final PROMPT 18")
    return "\n".join(lines)


def export_segment_csv(segment: str, records: List[Record]) -> str:
    path = OUTPUT_CSV_TEMPLATE.format(segment=segment)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "numeroDeclarationActivite",
                "denomination",
                "region",
                "effectifFormateurs",
                "stagiaires",
                "production_estimee",
                "macro_theme",
                "multi_cert",
            ]
        )
        for rec in records:
            if rec.segment != segment:
                continue
            region = REGION_NAMES.get(rec.region_code, str(rec.region_code) if rec.region_code is not None else "-")
            writer.writerow(
                [
                    rec.numero,
                    rec.denomination,
                    region,
                    rec.effectif,
                    int(round(rec.nb_stagiaires)),
                    f"{rec.production_estimee:.2f}" if rec.production_estimee is not None else "",
                    rec.macro_theme,
                    "Oui" if rec.has_multi_cert else "Non",
                ]
            )
    return path


def main() -> None:
    ensure_output_dir()
    records = load_records()
    tam_records = filter_tam(records)
    metrics = compute_segment_metrics(tam_records)
    table1 = build_table1(metrics)
    table2, region_counts = build_table2(tam_records)
    table3 = build_table3(tam_records)
    table4 = build_table4(tam_records)
    table5, high_activity_share = build_table5(tam_records)
    concentration = compute_concentration(region_counts)
    table6, weighted_scores = build_table6(metrics, concentration)
    winner = max(weighted_scores, key=weighted_scores.get)
    table7 = build_table7(winner, metrics, region_counts, tam_records)
    csv_path = export_segment_csv(winner, tam_records)
    region_bias = analyze_region_bias(region_counts)
    summary = build_summary(winner, metrics, region_bias, high_activity_share)
    content = "\n".join([table1, table2, table3, table4, table5, table6, table7, "\n" + summary + "\n", f"CSV export : {os.path.basename(csv_path)}"])
    with open(OUTPUT_MARKDOWN, "w", encoding="utf-8") as f:
        f.write(content)


if __name__ == "__main__":
    main()

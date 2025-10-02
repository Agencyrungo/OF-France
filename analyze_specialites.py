import csv
import os
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
OUTPUT_MARKDOWN = os.path.join(OUTPUT_DIR, "specialites_analysis.md")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "specialites_export.csv")

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
TARGET_MIN = 3
TARGET_MAX = 10

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
    region_code: Optional[int]
    effectif: Optional[int]
    nb_stagiaires: Optional[float]
    actions_cert: Optional[float]
    specialites: Tuple[Optional[Tuple[str, str]], Optional[Tuple[str, str]], Optional[Tuple[str, str]]]

    @property
    def spec1(self) -> Optional[Tuple[str, str]]:
        return self.specialites[0]

    @property
    def spec2(self) -> Optional[Tuple[str, str]]:
        return self.specialites[1]

    @property
    def spec3(self) -> Optional[Tuple[str, str]]:
        return self.specialites[2]


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


def clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def load_records() -> List[Record]:
    records: List[Record] = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            header_map: Dict[int, str] = {}
            idx_region = idx_effectif = idx_stagiaires = idx_actions = None
            idx_spec1_code = idx_spec1_label = None
            idx_spec2_code = idx_spec2_label = None
            idx_spec3_code = idx_spec3_label = None
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
                    idx_region = next((idx for idx, name in header_map.items() if name == "adressePhysiqueOrganismeFormation.codeRegion"), None)
                    idx_actions = next((idx for idx, name in header_map.items() if name == "certifications.actionsDeFormation"), None)
                    idx_spec1_code = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.specialitesDeFormation.codeSpecialite1"), None)
                    idx_spec1_label = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.specialitesDeFormation.libelleSpecialite1"), None)
                    idx_spec2_code = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.specialitesDeFormation.codeSpecialite2"), None)
                    idx_spec2_label = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.specialitesDeFormation.libelleSpecialite2"), None)
                    idx_spec3_code = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.specialitesDeFormation.codeSpecialite3"), None)
                    idx_spec3_label = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.specialitesDeFormation.libelleSpecialite3"), None)
                    idx_stagiaires = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.nbStagiaires"), None)
                    idx_effectif = next((idx for idx, name in header_map.items() if name == "informationsDeclarees.effectifFormateurs"), None)
                    elem.clear()
                    continue

                target_indices = {
                    idx
                    for idx in [
                        idx_region,
                        idx_actions,
                        idx_spec1_code,
                        idx_spec1_label,
                        idx_spec2_code,
                        idx_spec2_label,
                        idx_spec3_code,
                        idx_spec3_label,
                        idx_stagiaires,
                        idx_effectif,
                    ]
                    if idx is not None
                }
                if not target_indices:
                    continue

                values: Dict[int, str] = {}
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

                spec1 = None
                if idx_spec1_code is not None or idx_spec1_label is not None:
                    spec1 = (
                        clean_text(values.get(idx_spec1_code)),
                        clean_text(values.get(idx_spec1_label)),
                    )
                    if not spec1[0] and not spec1[1]:
                        spec1 = None
                spec2 = None
                if idx_spec2_code is not None or idx_spec2_label is not None:
                    spec2 = (
                        clean_text(values.get(idx_spec2_code)),
                        clean_text(values.get(idx_spec2_label)),
                    )
                    if not spec2[0] and not spec2[1]:
                        spec2 = None
                spec3 = None
                if idx_spec3_code is not None or idx_spec3_label is not None:
                    spec3 = (
                        clean_text(values.get(idx_spec3_code)),
                        clean_text(values.get(idx_spec3_label)),
                    )
                    if not spec3[0] and not spec3[1]:
                        spec3 = None

                record = Record(
                    region_code=parse_int(values.get(idx_region)) if idx_region is not None else None,
                    effectif=parse_int(values.get(idx_effectif)) if idx_effectif is not None else None,
                    nb_stagiaires=parse_float(values.get(idx_stagiaires)) if idx_stagiaires is not None else None,
                    actions_cert=parse_float(values.get(idx_actions)) if idx_actions is not None else None,
                    specialites=(spec1, spec2, spec3),
                )
                records.append(record)
                elem.clear()
    return records


def format_int(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def format_float(value: float, decimals: int = 1) -> str:
    return f"{value:,.{decimals}f}".replace(",", " ")


def percent(part: int, total: int) -> float:
    if total == 0:
        return 0.0
    return part / total * 100


def format_percent(value: float, decimals: int = 1) -> str:
    return f"{value:.{decimals}f}%"


MACRO_THEMES = [
    "Soft Skills",
    "Tech/Digital",
    "Commerce/Gestion",
    "Santé",
    "Langues",
    "Juridique",
    "Industrie",
    "Services",
    "Sécurité",
    "Autre",
]


SPECIFIC_MAPPING: Dict[str, str] = {
    "enseignement, formation": "Soft Skills",
    "ressources humaines, gestion du personnel, gestion de l'emploi": "Soft Skills",
    "développement des capacités comportementales et relationnelles": "Soft Skills",
    "développement des capacités d'orientation, d'insertion ou de réinsertion sociales et professionnelles": "Soft Skills",
    "formations générales": "Autre",
    "pluridisciplinaire": "Autre",
    "finances, banque, assurances": "Commerce/Gestion",
    "banque et assurances": "Commerce/Gestion",
    "comptabilité, gestion": "Commerce/Gestion",
    "techniques de vente": "Commerce/Gestion",
    "commerce, vente": "Commerce/Gestion",
    "marketing": "Commerce/Gestion",
    "soins infirmiers": "Santé",
    "santé": "Santé",
    "sanitaire et social": "Santé",
    "travail social": "Santé",
    "action sociale": "Santé",
    "services domestiques": "Services",
    "services à la personne": "Services",
    "transport, manutention, magasinage": "Services",
    "logistique, transport": "Services",
    "bâtiment et travaux publics": "Industrie",
    "génie civil, construction, bois": "Industrie",
    "mécanique générale": "Industrie",
    "mécanique et structures métalliques": "Industrie",
    "maintenance industrielle": "Industrie",
    "électronique": "Tech/Digital",
    "électricité": "Industrie",
    "énergie": "Industrie",
    "informatique": "Tech/Digital",
    "programmation, développement": "Tech/Digital",
    "réseaux informatiques": "Tech/Digital",
    "langues vivantes": "Langues",
    "linguistique": "Langues",
    "traduction, interprétation": "Langues",
    "droit": "Juridique",
    "sécurité des biens et des personnes": "Sécurité",
    "sécurité, armée, police": "Sécurité",
    "hôtellerie, restauration": "Services",
    "tourisme": "Services",
    "esthétique, coiffure": "Services",
    "coiffure": "Services",
    "esthétique": "Services",
    "agriculture": "Autre",
    "agronomie": "Autre",
    "environnement": "Autre",
}


KEYWORD_RULES: List[Tuple[str, Tuple[str, ...]]] = [
    ("Tech/Digital", ("informatique", "numér", "programm", "réseau", "logiciel", "digital", "donnée", "cyber", "cloud", "web", "intelligence artificielle", "information")),
    ("Soft Skills", ("orientation", "ressources humaines", "gestion du personnel", "enseignement", "pédagog", "insertion", "comportement", "formation de formateurs")),
    ("Commerce/Gestion", ("vente", "commercial", "marketing", "gestion", "finance", "banque", "assurance", "comptabil", "achats", "immobilier")),
    ("Santé", ("sant", "médic", "paraméd", "infirm", "social", "soin", "pharma", "handicap")),
    ("Langues", ("langue", "lingu", "tradu", "interpr")),
    ("Juridique", ("droit", "jurid", "justice", "crimin", "sciences politiques")),
    ("Industrie", ("mécan", "industri", "électric", "électrotech", "maintenance", "fabrication", "production", "chim", "bâtiment", "travaux publics", "construction", "métall", "plasturg", "energie")),
    ("Services", ("service", "transport", "logist", "coiff", "esthé", "restauration", "hôtel", "tourisme", "nettoyage", "sport", "animation", "santé animale", "assistan", "secrét")),
    ("Sécurité", ("sécur", "police", "gendar", "sûreté", "pompier", "secours", "défense")),
]


def normalize_label(label: str) -> str:
    return label.strip().lower()


def classify_specialite(label: Optional[str]) -> str:
    if not label:
        return "Autre"
    norm = normalize_label(label)
    if norm in SPECIFIC_MAPPING:
        return SPECIFIC_MAPPING[norm]
    if "formations générales" in norm or "non class" in norm:
        return "Autre"
    for theme, keywords in KEYWORD_RULES:
        if any(keyword in norm for keyword in keywords):
            return theme
    return "Autre"


def is_tam(record: Record) -> bool:
    if record.effectif is None or not (TARGET_MIN <= record.effectif <= TARGET_MAX):
        return False
    if record.actions_cert is None:
        return False
    if record.nb_stagiaires is None or record.nb_stagiaires <= 0:
        return False
    return True


def safe_div(num: float, den: int) -> float:
    return num / den if den else 0.0


def compute_top_specialites(records: List[Record]) -> Tuple[List[Dict[str, object]], int, int]:
    total_base = len(records)
    tam_records = [r for r in records if is_tam(r)]
    total_tam = len(tam_records)

    base_counter: Dict[Tuple[str, str], int] = Counter()
    tam_counter: Dict[Tuple[str, str], int] = Counter()

    for rec in records:
        if rec.spec1 is None:
            continue
        key = rec.spec1
        base_counter[key] += 1
    for rec in tam_records:
        if rec.spec1 is None:
            continue
        key = rec.spec1
        tam_counter[key] += 1

    rows: List[Dict[str, object]] = []
    for (code, label), count in base_counter.most_common(50):
        tam_count = tam_counter.get((code, label), 0)
        rows.append(
            {
                "code": code or "-",
                "label": label or "Non renseigné",
                "base_count": count,
                "base_pct": percent(count, total_base),
                "tam_count": tam_count,
                "tam_pct": percent(tam_count, total_tam),
            }
        )

    return rows, total_base, total_tam


def compute_macro_themes(records: List[Record], total_base: int, total_tam: int) -> Tuple[List[Dict[str, object]], Dict[str, Dict[str, object]]]:
    base_counts: Dict[str, int] = Counter()
    tam_counts: Dict[str, int] = Counter()
    tam_stag_sum: Dict[str, float] = defaultdict(float)
    tam_prod_sum: Dict[str, float] = defaultdict(float)

    tam_records = [r for r in records if is_tam(r)]

    for rec in records:
        if rec.spec1 is None:
            continue
        theme = classify_specialite(rec.spec1[1])
        base_counts[theme] += 1

    for rec in tam_records:
        if rec.spec1 is None:
            continue
        theme = classify_specialite(rec.spec1[1])
        tam_counts[theme] += 1
        if rec.nb_stagiaires is not None:
            tam_stag_sum[theme] += rec.nb_stagiaires
            if rec.effectif:
                tam_prod_sum[theme] += rec.nb_stagiaires / rec.effectif

    macro_rows: List[Dict[str, object]] = []
    theme_stats: Dict[str, Dict[str, object]] = {}

    for theme in MACRO_THEMES:
        base_count = base_counts.get(theme, 0)
        tam_count = tam_counts.get(theme, 0)
        stag_total = tam_stag_sum.get(theme, 0.0)
        prod_total = tam_prod_sum.get(theme, 0.0)
        stag_mean = safe_div(stag_total, tam_count)
        prod_mean = safe_div(prod_total, tam_count)
        macro_rows.append(
            {
                "theme": theme,
                "base_count": base_count,
                "base_pct": percent(base_count, total_base),
                "tam_count": tam_count,
                "tam_pct": percent(tam_count, total_tam),
                "stag_mean": stag_mean,
            }
        )
        theme_stats[theme] = {
            "tam_count": tam_count,
            "tam_pct": percent(tam_count, total_tam),
            "stag_mean": stag_mean,
            "prod_mean": prod_mean,
        }

    macro_rows.sort(key=lambda item: item["tam_pct"], reverse=True)
    return macro_rows, theme_stats


def compute_top_specialites_by_theme(records: List[Record]) -> Dict[str, List[str]]:
    tam_records = [r for r in records if is_tam(r) and r.spec1 is not None]
    theme_counter: Dict[str, Counter] = defaultdict(Counter)
    for rec in tam_records:
        code, label = rec.spec1
        theme = classify_specialite(label)
        theme_counter[theme][label or "Non renseigné"] += 1

    top_map: Dict[str, List[str]] = {}
    for theme, counter in theme_counter.items():
        names = [label for label, _ in counter.most_common(3)]
        top_map[theme] = names
    return top_map


def compute_specialites_secondary(records: List[Record]) -> Tuple[List[Dict[str, object]], List[Dict[str, object]], int, int]:
    spec2_records = [r for r in records if r.spec2 is not None]
    spec3_records = [r for r in records if r.spec3 is not None]

    total_spec2 = len(spec2_records)
    total_spec3 = len(spec3_records)

    counter2: Counter = Counter()
    counter3: Counter = Counter()

    for rec in spec2_records:
        code, label = rec.spec2
        counter2[label or "Non renseigné"] += 1

    for rec in spec3_records:
        code, label = rec.spec3
        counter3[label or "Non renseigné"] += 1

    top20_spec2 = [
        {
            "label": label,
            "count": count,
            "pct": percent(count, total_spec2),
        }
        for label, count in counter2.most_common(20)
    ]
    top20_spec3 = [
        {
            "label": label,
            "count": count,
            "pct": percent(count, total_spec3),
        }
        for label, count in counter3.most_common(20)
    ]

    return top20_spec2, top20_spec3, total_spec2, total_spec3


def compute_macro_theme_priorities(records: List[Record], theme_stats: Dict[str, Dict[str, object]]) -> List[Dict[str, object]]:
    tam_records = [r for r in records if is_tam(r) and r.spec1 is not None]
    total_tam = len(tam_records)
    if total_tam == 0:
        return []
    overall_stag_mean = sum(r.nb_stagiaires or 0.0 for r in tam_records) / total_tam
    overall_prod_mean = sum((r.nb_stagiaires or 0.0) / (r.effectif or 1) for r in tam_records) / total_tam

    rows: List[Dict[str, object]] = []
    for theme in MACRO_THEMES:
        stats = theme_stats.get(theme, {})
        tam_count = int(stats.get("tam_count", 0))
        tam_pct = float(stats.get("tam_pct", 0.0))
        stag_mean = float(stats.get("stag_mean", 0.0))
        prod_mean = float(stats.get("prod_mean", 0.0))
        if tam_count == 0:
            priority = "BASSE"
        elif tam_pct > 15 and (stag_mean > overall_stag_mean or prod_mean > overall_prod_mean):
            priority = "HAUTE"
        elif 10 <= tam_pct <= 15:
            priority = "MOYENNE"
        elif tam_pct > 15:
            priority = "MOYENNE"
        else:
            priority = "BASSE"
        rows.append(
            {
                "theme": theme,
                "tam_count": tam_count,
                "tam_pct": tam_pct,
                "stag_mean": stag_mean,
                "prod_mean": prod_mean,
                "priority": priority,
            }
        )
    rows.sort(key=lambda item: item["tam_pct"], reverse=True)
    return rows


def compute_niches(records: List[Record], total_base: int, total_tam: int) -> List[Dict[str, object]]:
    base_counter: Counter = Counter()
    tam_counter: Counter = Counter()
    tam_records = [r for r in records if is_tam(r)]

    for rec in records:
        if rec.spec1 is None:
            continue
        base_counter[rec.spec1[1] or "Non renseigné"] += 1
    for rec in tam_records:
        if rec.spec1 is None:
            continue
        tam_counter[rec.spec1[1] or "Non renseigné"] += 1

    niches: List[Dict[str, object]] = []
    for label, base_count in base_counter.items():
        base_pct = percent(base_count, total_base)
        if base_pct >= 2:
            continue
        tam_count = tam_counter.get(label, 0)
        tam_pct = percent(tam_count, total_tam)
        if tam_pct <= 3:
            continue
        ratio = (tam_pct / base_pct) if base_pct > 0 else 0
        if ratio <= 1.5:
            continue
        niches.append(
            {
                "label": label,
                "base_count": base_count,
                "base_pct": base_pct,
                "tam_count": tam_count,
                "tam_pct": tam_pct,
                "ratio": ratio,
            }
        )

    niches.sort(key=lambda item: item["ratio"], reverse=True)
    return niches


def compute_regional_diversity(records: List[Record]) -> List[Dict[str, object]]:
    region_groups: Dict[int, List[Record]] = defaultdict(list)
    for rec in records:
        if rec.region_code is None or rec.spec1 is None:
            continue
        region_groups[rec.region_code].append(rec)

    rows: List[Dict[str, object]] = []
    for region_code, recs in region_groups.items():
        total = len(recs)
        counter: Counter = Counter()
        for rec in recs:
            label = rec.spec1[1] or "Non renseigné"
            counter[label] += 1
        dominant_label, dominant_count = counter.most_common(1)[0]
        dominant_pct = percent(dominant_count, total)
        if dominant_pct > 40:
            diversity = "Faible"
        elif dominant_pct >= 25:
            diversity = "Moyenne"
        else:
            diversity = "Forte"
        rows.append(
            {
                "region_code": region_code,
                "region_name": REGION_NAMES.get(region_code, "Autres DOM-TOM"),
                "distinct": len(counter),
                "dominant": dominant_label,
                "dominant_pct": dominant_pct,
                "diversity": diversity,
            }
        )

    rows.sort(key=lambda item: item["region_name"])
    return rows


def write_markdown(
    top50: List[Dict[str, object]],
    macro_rows: List[Dict[str, object]],
    macro_tops: Dict[str, List[str]],
    spec2_rows: List[Dict[str, object]],
    spec3_rows: List[Dict[str, object]],
    total_spec2: int,
    total_spec3: int,
    tam_macro_rows: List[Dict[str, object]],
    niches: List[Dict[str, object]],
    regional_rows: List[Dict[str, object]],
    totals: Dict[str, float],
) -> None:
    lines: List[str] = []
    lines.append("# Analyse des spécialités OF France 2025")
    lines.append("")

    lines.append("## Tableau 1 : Top 50 des spécialités principales (Spé 1)")
    lines.append("| Rang | Code NSF | Libellé spécialité | OF total | % base | OF TAM | % TAM |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for rank, row in enumerate(top50, start=1):
        lines.append(
            "| {rank} | {code} | {label} | {base_count} | {base_pct:.1f}% | {tam_count} | {tam_pct:.1f}% |".format(
                rank=rank,
                code=row["code"],
                label=row["label"],
                base_count=format_int(row["base_count"]),
                base_pct=row["base_pct"],
                tam_count=format_int(row["tam_count"]),
                tam_pct=row["tam_pct"],
            )
        )
    lines.append("")

    lines.append("## Tableau 2 : Répartition par macro-thème")
    lines.append("| Macro-thème | OF total | % base | OF TAM | % TAM | Stag. moyen (TAM) | Spés principales |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for row in macro_rows:
        top_specs = ", ".join(macro_tops.get(row["theme"], [])) or "-"
        lines.append(
            "| {theme} | {base_count} | {base_pct:.1f}% | {tam_count} | {tam_pct:.1f}% | {stag_mean} | {tops} |".format(
                theme=row["theme"],
                base_count=format_int(row["base_count"]),
                base_pct=row["base_pct"],
                tam_count=format_int(row["tam_count"]),
                tam_pct=row["tam_pct"],
                stag_mean=format_float(row["stag_mean"], 1) if row["tam_count"] else "-",
                tops=top_specs,
            )
        )
    lines.append("")

    lines.append("## Tableau 3a : Top 20 spécialités secondaires (Spé 2)")
    lines.append(
        "| Rang | Libellé spécialité | OF avec Spé2 | % des {total} |".format(
            total=format_int(total_spec2)
        )
    )
    lines.append("| --- | --- | --- | --- |")
    for rank, row in enumerate(spec2_rows, start=1):
        lines.append(
            "| {rank} | {label} | {count} | {pct:.1f}% |".format(
                rank=rank,
                label=row["label"],
                count=format_int(row["count"]),
                pct=row["pct"],
            )
        )
    lines.append("")

    lines.append("## Tableau 3b : Top 20 spécialités tertiaires (Spé 3)")
    lines.append(
        "| Rang | Libellé spécialité | OF avec Spé3 | % des {total} |".format(
            total=format_int(total_spec3)
        )
    )
    lines.append("| --- | --- | --- | --- |")
    for rank, row in enumerate(spec3_rows, start=1):
        lines.append(
            "| {rank} | {label} | {count} | {pct:.1f}% |".format(
                rank=rank,
                label=row["label"],
                count=format_int(row["count"]),
                pct=row["pct"],
            )
        )
    lines.append("")

    lines.append("## Tableau 4 : TAM qualifié par macro-thème")
    lines.append("| Macro-thème | OF TAM | % TAM | Stag. moyen | Prod est. | Priorité |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for row in tam_macro_rows:
        lines.append(
            "| {theme} | {tam_count} | {tam_pct:.1f}% | {stag_mean} | {prod_mean} | {priority} |".format(
                theme=row["theme"],
                tam_count=format_int(row["tam_count"]),
                tam_pct=row["tam_pct"],
                stag_mean=format_float(row["stag_mean"], 1) if row["tam_count"] else "-",
                prod_mean=format_float(row["prod_mean"], 2) if row["tam_count"] else "-",
                priority=row["priority"],
            )
        )
    lines.append("")

    lines.append("## Tableau 5 : Niches sur-représentées dans le TAM")
    lines.append("| Spécialité | OF base | % base | OF TAM | % TAM | Ratio TAM/base |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    if niches:
        for row in niches:
            lines.append(
                "| {label} | {base_count} | {base_pct:.2f}% | {tam_count} | {tam_pct:.2f}% | {ratio:.2f} |".format(
                    label=row["label"],
                    base_count=format_int(row["base_count"]),
                    base_pct=row["base_pct"],
                    tam_count=format_int(row["tam_count"]),
                    tam_pct=row["tam_pct"],
                    ratio=row["ratio"],
                )
            )
    else:
        lines.append("| Aucune spécialité | - | - | - | - | - |")
    lines.append("")

    lines.append("## Tableau 6 : Diversité des spécialités par région")
    lines.append("| Région | Nb spés différentes | Spé dominante | % spé dominante | Diversité |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in regional_rows:
        lines.append(
            "| {region} | {distinct} | {dominant} | {pct:.1f}% | {diversity} |".format(
                region=row["region_name"],
                distinct=format_int(row["distinct"]),
                dominant=row["dominant"],
                pct=row["dominant_pct"],
                diversity=row["diversity"],
            )
        )
    lines.append("")

    lines.append("## Synthèse")
    lines.append(
        "Spé 1 : {spec1_count:,} OF renseignés ({spec1_pct:.1f}%).".format(
            spec1_count=int(totals["spec1_count"]),
            spec1_pct=totals["spec1_pct"],
        ).replace(",", " ")
    )
    lines.append(
        "Spé 2 : {spec2_count:,} OF ({spec2_pct:.1f}%).".format(
            spec2_count=int(totals["spec2_count"]),
            spec2_pct=totals["spec2_pct"],
        ).replace(",", " ")
    )
    lines.append(
        "Spé 3 : {spec3_count:,} OF ({spec3_pct:.1f}%).".format(
            spec3_count=int(totals["spec3_count"]),
            spec3_pct=totals["spec3_pct"],
        ).replace(",", " ")
    )
    lines.append("")

    lines.append(
        "Top 5 spécialités : {top_list}.".format(
            top_list=", ".join(
                [
                    "{label} : {count} OF ({base_pct:.1f}% base, {tam_pct:.1f}% TAM)".format(
                        label=row["label"],
                        count=format_int(row["base_count"]),
                        base_pct=row["base_pct"],
                        tam_pct=row["tam_pct"],
                    )
                    for row in top50[:5]
                ]
            )
        )
    )
    top_macro_summary = ", ".join(
        "{idx}. {theme} : {tam_pct:.1f}% TAM".format(idx=rank, theme=row["theme"], tam_pct=row["tam_pct"])
        for rank, row in enumerate(tam_macro_rows[:3], start=1)
    )
    if top_macro_summary:
        lines.append(f"Macro-thèmes prioritaires : {top_macro_summary}.")
    else:
        lines.append("Macro-thèmes prioritaires : aucun.")
    if niches:
        lines.append(
            "Niches émergentes : "
            + ", ".join(
                "{label} : Sur-représentation ×{ratio:.1f}".format(label=row["label"], ratio=row["ratio"])
                for row in niches
            )
            + "."
        )
    else:
        lines.append("Niches émergentes : aucune spécialité sur-représentée.")
    lines.append("")

    with open(OUTPUT_MARKDOWN, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def write_csv(top50: List[Dict[str, object]], macro_rows: List[Dict[str, object]], niches: List[Dict[str, object]]) -> None:
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["table", "rang", "code", "label", "of_total", "pct_base", "of_tam", "pct_tam", "extra"])
        for rank, row in enumerate(top50, start=1):
            writer.writerow(
                [
                    "top50",
                    rank,
                    row["code"],
                    row["label"],
                    row["base_count"],
                    round(row["base_pct"], 2),
                    row["tam_count"],
                    round(row["tam_pct"], 2),
                    "",
                ]
            )
        for row in macro_rows:
            writer.writerow(
                [
                    "macro_theme",
                    "",
                    "",
                    row["theme"],
                    row["base_count"],
                    round(row["base_pct"], 2),
                    row["tam_count"],
                    round(row["tam_pct"], 2),
                    round(row["stag_mean"], 2) if row["tam_count"] else "",
                ]
            )
        for row in niches:
            writer.writerow(
                [
                    "niche",
                    "",
                    "",
                    row["label"],
                    row["base_count"],
                    round(row["base_pct"], 2),
                    row["tam_count"],
                    round(row["tam_pct"], 2),
                    round(row["ratio"], 2),
                ]
            )


def main() -> None:
    ensure_output_dir()
    records = load_records()

    top50, total_base, total_tam = compute_top_specialites(records)
    macro_rows, theme_stats = compute_macro_themes(records, total_base, total_tam)
    macro_tops = compute_top_specialites_by_theme(records)
    spec2_rows, spec3_rows, total_spec2, total_spec3 = compute_specialites_secondary(records)
    tam_macro_rows = compute_macro_theme_priorities(records, theme_stats)
    niches = compute_niches(records, total_base, total_tam)
    regional_rows = compute_regional_diversity(records)

    spec1_count = sum(1 for r in records if r.spec1 is not None)
    totals = {
        "spec1_count": spec1_count,
        "spec2_count": total_spec2,
        "spec3_count": total_spec3,
        "spec1_pct": percent(spec1_count, total_base),
        "spec2_pct": percent(total_spec2, total_base),
        "spec3_pct": percent(total_spec3, total_base),
        "total_base": total_base,
        "total_tam": total_tam,
    }

    write_markdown(
        top50,
        macro_rows,
        macro_tops,
        spec2_rows,
        spec3_rows,
        total_spec2,
        total_spec3,
        tam_macro_rows,
        niches,
        regional_rows,
        totals,
    )
    write_csv(top50, macro_rows, niches)


if __name__ == "__main__":
    main()

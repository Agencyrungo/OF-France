import csv
import os
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
OUTPUT_MARKDOWN = os.path.join(OUTPUT_DIR, "prompt12_haute_activite.md")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "prompt12_top50_haute_activite.csv")

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
    (
        "Tech/Digital",
        (
            "informatique",
            "numér",
            "programm",
            "réseau",
            "logiciel",
            "digital",
            "donnée",
            "cyber",
            "cloud",
            "web",
            "intelligence artificielle",
            "information",
        ),
    ),
    (
        "Soft Skills",
        (
            "orientation",
            "ressources humaines",
            "gestion du personnel",
            "enseignement",
            "pédagog",
            "insertion",
            "comportement",
            "formation de formateurs",
        ),
    ),
    (
        "Commerce/Gestion",
        (
            "vente",
            "commercial",
            "marketing",
            "gestion",
            "finance",
            "banque",
            "assurance",
            "comptabil",
            "achats",
            "immobilier",
        ),
    ),
    (
        "Santé",
        (
            "sant",
            "médic",
            "paraméd",
            "infirm",
            "social",
            "soin",
            "pharma",
            "handicap",
        ),
    ),
    ("Langues", ("langue", "lingu", "tradu", "interpr")),
    ("Juridique", ("droit", "jurid", "justice", "crimin", "sciences politiques")),
    (
        "Industrie",
        (
            "mécan",
            "industri",
            "électric",
            "électrotech",
            "maintenance",
            "fabrication",
            "production",
            "chim",
            "bâtiment",
            "travaux publics",
            "construction",
            "métall",
            "plasturg",
            "energie",
        ),
    ),
    (
        "Services",
        (
            "service",
            "transport",
            "logist",
            "coiff",
            "esthé",
            "restauration",
            "hôtel",
            "tourisme",
            "nettoyage",
            "sport",
            "animation",
            "santé animale",
            "assistan",
            "secrét",
        ),
    ),
    ("Sécurité", ("sécur", "police", "gendar", "sûreté", "pompier", "secours", "défense")),
]


@dataclass
class Record:
    denomination: str
    nb_stagiaires: float
    effectif: Optional[int]
    actions: Optional[int]
    region_code: Optional[int]
    specialite: Optional[str]
    adresse: Optional[str]
    code_postal: Optional[str]
    ville: Optional[str]


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def column_ref_to_index(ref: str) -> int:
    letters = "".join(ch for ch in ref if ch.isalpha())
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def load_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    shared: List[str] = []
    path = "xl/sharedStrings.xml"
    if path not in zf.namelist():
        return shared
    with zf.open(path) as f:
        for _, elem in ET.iterparse(f, events=("end",)):
            if elem.tag == NS + "si":
                text = "".join(t.text or "" for t in elem.findall('.//' + NS + 't'))
                shared.append(text)
                elem.clear()
    return shared


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
    text = value.strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    text = value.strip()
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
                "adresse": None,
                "code_postal": None,
                "ville": None,
            }
            for _, elem in ET.iterparse(f, events=("end",)):
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
                    target_indices["adresse"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "adressePhysiqueOrganismeFormation.voie"
                        ),
                        None,
                    )
                    target_indices["code_postal"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "adressePhysiqueOrganismeFormation.codePostal"
                        ),
                        None,
                    )
                    target_indices["ville"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "adressePhysiqueOrganismeFormation.ville"
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
                adresse = values.get(target_indices["adresse"])
                if adresse:
                    adresse = adresse.strip()
                code_postal = values.get(target_indices["code_postal"])
                if code_postal:
                    code_postal = code_postal.strip()
                ville = values.get(target_indices["ville"])
                if ville:
                    ville = ville.strip()
                records.append(
                    Record(
                        denomination=denomination.strip(),
                        nb_stagiaires=nb_stagiaires,
                        effectif=effectif,
                        actions=actions,
                        region_code=region_code,
                        specialite=specialite,
                        adresse=adresse,
                        code_postal=code_postal,
                        ville=ville,
                    )
                )
                elem.clear()
    return records


def is_tam(record: Record) -> bool:
    if record.effectif is None or not (TARGET_MIN <= record.effectif <= TARGET_MAX):
        return False
    if record.actions != 1:
        return False
    if record.nb_stagiaires <= 0:
        return False
    return True


def classify_specialite(label: Optional[str]) -> str:
    if not label:
        return "Autre"
    norm = label.strip().lower()
    if norm in SPECIFIC_MAPPING:
        return SPECIFIC_MAPPING[norm]
    if "formations générales" in norm or "non class" in norm:
        return "Autre"
    for theme, keywords in KEYWORD_RULES:
        if any(keyword in norm for keyword in keywords):
            return theme
    return "Autre"


def format_int(value: float) -> str:
    return f"{int(round(value)):,}".replace(",", " ")


def format_float(value: float, decimals: int = 1) -> str:
    return f"{value:,.{decimals}f}".replace(",", " ")


def format_percent(value: float, decimals: int = 1) -> str:
    return f"{value:.{decimals}f}%"


def safe_div(num: float, den: float) -> float:
    return num / den if den else 0.0


def compute_prod(nb_stagiaires: float) -> float:
    return nb_stagiaires / 12.0


def derive_dept(code_postal: Optional[str]) -> str:
    if not code_postal:
        return "-"
    code = code_postal.strip()
    if len(code) < 2:
        return "-"
    return code[:2]


def build_table1(
    high_records: List[Record], tam_records: List[Record]
) -> Tuple[List[List[str]], List[Dict[str, float]]]:
    tranches = [
        ("500-1000", 500, 1000),
        ("1000-2000", 1000, 2000),
        ("2000-5000", 2000, 5000),
        ("5000+", 5000, None),
    ]
    tam_total = len(tam_records)
    total_high = len(high_records)
    rows: List[List[str]] = []
    distribution: List[Dict[str, float]] = []
    for name, lower, upper in tranches:
        subset = [
            r
            for r in high_records
            if r.nb_stagiaires >= lower and (upper is None or r.nb_stagiaires < upper)
        ]
        if not subset:
            rows.append([name, "0", "0.0%", "-", "-", "-"])
            distribution.append({"name": name, "count": 0.0, "share_high": 0.0})
            continue
        count = len(subset)
        pct_tam = count / tam_total * 100 if tam_total else 0.0
        share_high = count / total_high * 100 if total_high else 0.0
        effectif_mean = sum(r.effectif or 0 for r in subset) / count
        stag_form = safe_div(sum(r.nb_stagiaires for r in subset), sum(r.effectif or 0 for r in subset))
        prod_mean = sum(compute_prod(r.nb_stagiaires) for r in subset) / count
        rows.append(
            [
                name,
                format_int(count),
                format_percent(pct_tam, 1),
                format_float(effectif_mean, 1),
                format_float(stag_form, 1),
                format_float(prod_mean, 1),
            ]
        )
        distribution.append({"name": name, "count": float(count), "share_high": share_high})
    total_count = len(high_records)
    total_pct = total_count / tam_total * 100 if tam_total else 0.0
    effectif_mean = sum(r.effectif or 0 for r in high_records) / total_count if total_count else 0.0
    stag_form = safe_div(
        sum(r.nb_stagiaires for r in high_records), sum(r.effectif or 0 for r in high_records)
    )
    prod_mean = (
        sum(compute_prod(r.nb_stagiaires) for r in high_records) / total_count if total_count else 0.0
    )
    rows.append(
        [
            "TOTAL ≥500",
            format_int(total_count),
            format_percent(total_pct, 1),
            format_float(effectif_mean, 1),
            format_float(stag_form, 1),
            format_float(prod_mean, 1),
        ]
    )
    return rows, distribution


def build_table2(
    high_records: List[Record], tam_records: List[Record]
) -> Tuple[List[List[str]], Dict[str, float]]:
    high_count = len(high_records)
    tam_count = len(tam_records)
    share = high_count / tam_count * 100 if tam_count else 0.0

    def mean_effectif(records: List[Record]) -> float:
        return sum(r.effectif or 0 for r in records) / len(records) if records else 0.0

    def mean_stagiaires(records: List[Record]) -> float:
        return sum(r.nb_stagiaires for r in records) / len(records) if records else 0.0

    def ratio_stag_form(records: List[Record]) -> float:
        return safe_div(sum(r.nb_stagiaires for r in records), sum(r.effectif or 0 for r in records))

    def mean_prod(records: List[Record]) -> float:
        return sum(compute_prod(r.nb_stagiaires) for r in records) / len(records) if records else 0.0

    high_effectif = mean_effectif(high_records)
    tam_effectif = mean_effectif(tam_records)
    high_stag = mean_stagiaires(high_records)
    tam_stag = mean_stagiaires(tam_records)
    high_ratio = ratio_stag_form(high_records)
    tam_ratio = ratio_stag_form(tam_records)
    high_prod = mean_prod(high_records)
    tam_prod = mean_prod(tam_records)

    def format_ecart(high: float, base: float) -> str:
        if base == 0:
            return "-"
        delta = (high / base) - 1
        sign = "+" if delta >= 0 else ""
        return f"{sign}{delta * 100:.1f}%"

    rows = [
        ["Nombre OF", format_int(high_count), format_int(tam_count), format_percent(share, 1)],
        ["Part du TAM", format_percent(share, 1), "100%", "-"],
        [
            "Effectif moyen",
            format_float(high_effectif, 1),
            format_float(tam_effectif, 1),
            format_ecart(high_effectif, tam_effectif),
        ],
        [
            "Stagiaires moyen",
            format_float(high_stag, 0),
            format_float(tam_stag, 0),
            format_ecart(high_stag, tam_stag),
        ],
        [
            "Stagiaires / formateur",
            format_float(high_ratio, 1),
            format_float(tam_ratio, 1),
            format_ecart(high_ratio, tam_ratio),
        ],
        [
            "Prod est. (livr./mois)",
            format_float(high_prod, 1),
            format_float(tam_prod, 1),
            format_ecart(high_prod, tam_prod),
        ],
    ]
    stats = {
        "share": share,
        "high_effectif": high_effectif,
        "high_stag": high_stag,
        "high_ratio": high_ratio,
        "high_prod": high_prod,
    }
    return rows, stats


def build_table3(
    high_records: List[Record], tam_records: List[Record]
) -> Tuple[List[List[str]], List[Dict[str, float]]]:
    region_totals: Dict[str, Dict[str, float]] = {}
    for rec in tam_records:
        if rec.region_code is None:
            continue
        label = REGION_NAMES.get(rec.region_code, "Autres régions")
        stats = region_totals.setdefault(label, {"tam": 0.0, "high": 0.0})
        stats["tam"] += 1
    for rec in high_records:
        if rec.region_code is None:
            continue
        label = REGION_NAMES.get(rec.region_code, "Autres régions")
        stats = region_totals.setdefault(label, {"tam": 0.0, "high": 0.0})
        stats["high"] += 1

    total_high = len(high_records)
    rows: List[List[str]] = []
    region_details: List[Dict[str, float]] = []
    for label, values in sorted(region_totals.items(), key=lambda item: item[1]["high"], reverse=True):
        count_high = values["high"]
        if count_high == 0:
            continue
        count_region = values["tam"]
        pct_region = count_high / count_region * 100 if count_region else 0.0
        pct_national = count_high / total_high * 100 if total_high else 0.0
        rows.append(
            [
                label,
                format_int(count_high),
                format_percent(pct_region, 1),
                format_percent(pct_national, 1),
            ]
        )
        region_details.append({"region": label, "share": pct_national})
    rows.append([
        "TOTAL",
        format_int(total_high),
        "-",
        "100.0%",
    ])
    return rows, region_details


def build_table4(
    high_records: List[Record], tam_records: List[Record]
) -> Tuple[List[List[str]], List[Dict[str, float]]]:
    theme_high: Dict[str, int] = {theme: 0 for theme in MACRO_THEMES}
    theme_tam: Dict[str, int] = {theme: 0 for theme in MACRO_THEMES}
    for rec in tam_records:
        theme = classify_specialite(rec.specialite)
        theme_tam[theme] = theme_tam.get(theme, 0) + 1
    for rec in high_records:
        theme = classify_specialite(rec.specialite)
        theme_high[theme] = theme_high.get(theme, 0) + 1
    total_high = sum(theme_high.values())
    total_tam = sum(theme_tam.values())
    rows: List[List[str]] = []
    stats: List[Dict[str, float]] = []
    for theme in MACRO_THEMES:
        high_count = theme_high.get(theme, 0)
        tam_count = theme_tam.get(theme, 0)
        share_high = high_count / total_high * 100 if total_high else 0.0
        share_tam = tam_count / total_tam * 100 if total_tam else 0.0
        diff = share_high - share_tam
        if share_tam == 0 and share_high > 0:
            status = "Sur-représenté"
        elif share_high >= share_tam * 1.1:
            status = "Sur-représenté"
        elif share_high <= share_tam * 0.9:
            status = "Sous-représenté"
        else:
            status = "Aligné"
        rows.append(
            [
                theme,
                format_int(high_count),
                format_percent(share_high, 1),
                f"{diff:+.1f} pp",
                status,
            ]
        )
        stats.append({"theme": theme, "share": share_high})
    rows.sort(key=lambda row: float(row[2].rstrip("%")), reverse=True)
    stats.sort(key=lambda item: item["share"], reverse=True)
    return rows, stats


def build_table5(high_records: List[Record]) -> Tuple[List[List[str]], List[Dict[str, str]]]:
    sorted_records = sorted(high_records, key=lambda r: r.nb_stagiaires, reverse=True)
    rows: List[List[str]] = []
    csv_rows: List[Dict[str, str]] = []
    for rank, rec in enumerate(sorted_records[:50], start=1):
        dept = derive_dept(rec.code_postal)
        ratio = safe_div(rec.nb_stagiaires, rec.effectif or 0)
        prod = compute_prod(rec.nb_stagiaires)
        rows.append(
            [
                str(rank),
                rec.denomination or "-",
                dept,
                format_int(rec.effectif or 0),
                format_int(rec.nb_stagiaires),
                format_float(ratio, 1),
                rec.specialite or "-",
                format_float(prod, 1),
            ]
        )
        csv_rows.append(
            {
                "rang": str(rank),
                "denomination": rec.denomination,
                "code_postal": rec.code_postal or "",
                "ville": rec.ville or "",
                "adresse": rec.adresse or "",
                "region": REGION_NAMES.get(rec.region_code, "Autres régions"),
                "dept": dept,
                "effectif": str(rec.effectif or ""),
                "nb_stagiaires": str(int(round(rec.nb_stagiaires))),
                "stagiaires_par_formateur": f"{ratio:.1f}" if ratio else "",
                "specialite": rec.specialite or "",
                "production_estimee_livrables_par_mois": f"{prod:.1f}",
            }
        )
    return rows, csv_rows


def build_table6(high_records: List[Record]) -> Tuple[List[List[str]], Dict[str, float]]:
    prod_mean = sum(compute_prod(r.nb_stagiaires) for r in high_records) / len(high_records) if high_records else 0.0
    heures_gagnees = prod_mean * 2
    valeur = heures_gagnees * 120
    cout = 299
    roi_net = valeur - cout
    multiplicateur = valeur / cout if cout else 0.0
    rows = [
        ["Prod est. moyenne (livr./mois)", format_float(prod_mean, 1)],
        ["Temps gagné estimé (2h/livrable)", format_float(heures_gagnees, 1)],
        ["Valeur temps (TJM 120€)", f"{valeur:,.0f}€".replace(",", " ")],
        ["Coût Qalia", "299€"],
        ["ROI net", f"{roi_net:,.0f}€".replace(",", " ")],
        ["~ROI", f"×{multiplicateur:.1f}"],
    ]
    stats = {
        "prod_mean": prod_mean,
        "heures": heures_gagnees,
        "valeur": valeur,
        "roi_net": roi_net,
        "multiplicateur": multiplicateur,
    }
    return rows, stats


def write_csv(rows: List[Dict[str, str]]) -> None:
    fieldnames = [
        "rang",
        "denomination",
        "code_postal",
        "ville",
        "adresse",
        "region",
        "dept",
        "effectif",
        "nb_stagiaires",
        "stagiaires_par_formateur",
        "specialite",
        "production_estimee_livrables_par_mois",
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def render_table(title: str, headers: List[str], rows: List[List[str]]) -> List[str]:
    lines = [title]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    return lines


def generate_markdown() -> None:
    ensure_output_dir()
    records = load_records()
    tam_records = [r for r in records if is_tam(r)]
    high_records = [r for r in tam_records if r.nb_stagiaires >= 500]

    table1, tranche_stats = build_table1(high_records, tam_records)
    table2, profile_stats = build_table2(high_records, tam_records)
    table3, region_stats = build_table3(high_records, tam_records)
    table4, theme_stats = build_table4(high_records, tam_records)
    table5, csv_rows = build_table5(high_records)
    table6, roi_stats = build_table6(high_records)

    write_csv(csv_rows)

    lines: List[str] = []
    lines.extend(render_table("## Tableau 1 : Tranches haute activité", [
        "Tranche stag./an",
        "OF",
        "% TAM",
        "Effectif moy.",
        "Stag./form",
        "Prod est. (livr./mois)",
    ], table1))

    lines.extend(render_table("## Tableau 2 : Profil type haute activité", [
        "Métrique",
        "Haute activité (≥500)",
        "TAM général",
        "Écart",
    ], table2))

    lines.extend(render_table("## Tableau 3 : Répartition géographique", [
        "Région",
        "OF ≥500 stag.",
        "% région",
        "% haute_act national",
    ], table3))

    lines.extend(render_table("## Tableau 4 : Spécialités haute activité", [
        "Macro-thème",
        "OF ≥500 stag.",
        "% macro",
        "vs TAM général",
        "Statut",
    ], table4))

    lines.extend(render_table("## Tableau 5 : Top 50 OF ultra-actifs", [
        "Rang",
        "Dénomination",
        "Dept",
        "Effectif",
        "Stagiaires",
        "Stag./form",
        "Spécialité",
        "Prod est. (livr./mois)",
    ], table5))

    lines.extend(render_table("## Tableau 6 : ROI Qalia haute activité", [
        "Métrique",
        "Valeur",
    ], table6))

    total_high = len(high_records)
    distribution_map = {item["name"]: item["share_high"] for item in tranche_stats}
    dist_500_1000 = distribution_map.get("500-1000", 0.0)
    dist_1000_2000 = distribution_map.get("1000-2000", 0.0)
    dist_2000_plus = distribution_map.get("2000-5000", 0.0) + distribution_map.get("5000+", 0.0)
    top_regions = region_stats[:3]
    top_region_share = sum(item["share"] for item in top_regions)
    top_region_labels = ", ".join(item["region"] for item in top_regions)
    dominant_themes: List[str] = []
    for item in theme_stats:
        if item["theme"] == "Autre":
            continue
        dominant_themes.append(item["theme"])
        if len(dominant_themes) == 3:
            break
    for item in theme_stats:
        if len(dominant_themes) >= 3:
            break
        if item["theme"] not in dominant_themes:
            dominant_themes.append(item["theme"])
    dominant_theme_text = ", ".join(dominant_themes[:3])

    lines.append("## Synthèse")
    lines.append("HAUTE ACTIVITÉ (≥500 stagiaires) :")
    lines.append("")
    lines.append(
        f"- Nombre OF : {format_int(total_high)} ({format_percent(profile_stats['share'], 1)} du TAM)"
    )
    lines.append("")
    lines.append("Distribution :")
    lines.append(f"- 500-1000 : {format_percent(dist_500_1000, 1)}")
    lines.append(f"- 1000-2000 : {format_percent(dist_1000_2000, 1)}")
    lines.append(f"- 2000+ : {format_percent(dist_2000_plus, 1)}")
    lines.append("")
    lines.append("Profil :")
    lines.append(f"- Effectif moyen : {format_float(profile_stats['high_effectif'], 1)} formateurs")
    lines.append(f"- Stagiaires moyen : {format_float(profile_stats['high_stag'], 0)} / an")
    lines.append(f"- Production estimée : {format_float(roi_stats['prod_mean'], 1)} livrables/mois")
    lines.append("")
    lines.append("Concentration :")
    lines.append(
        f"- Top 3 régions ({top_region_labels}) : {format_percent(top_region_share, 1)} de la haute activité"
    )
    lines.append(f"- Spécialités dominantes : {dominant_theme_text}")
    lines.append("")
    lines.append("Opportunité :")
    lines.append("- Segment premium identifié")
    lines.append(
        f"- ROI Qalia : ×{roi_stats['multiplicateur']:.1f} (vs ×6.8 standard)"
    )
    lines.append("- Recommandation : Pricing Team+ 499€/mois")
    lines.append("")
    lines.append("Actions :")
    lines.append("- Ciblage prioritaire haute activité")
    lines.append("- Messaging \"Power users\"")
    lines.append("- Cas d'usage production intensive")
    lines.append("")

    with open(OUTPUT_MARKDOWN, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    generate_markdown()

import csv
import os
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

# Column indices from header inspection
COL_DENOMINATION = 2
COL_CODE_POSTAL = 6
COL_VILLE = 7
COL_CODE_REGION = 8
COL_ACTIONS_FORMATION = 9
COL_NB_STAGIAIRES = 27
COL_EFFECTIF_FORMATEURS = 29
COL_SPEC1 = 22

TARGET_MIN_EFFECTIF = 3
TARGET_MAX_EFFECTIF = 10

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

CITY_METRO_POP = {
    "PARIS": 10800000,
    "LYON": 2400000,
    "VILLEURBANNE": 2400000,
    "MARSEILLE": 1800000,
    "AIX EN PROVENCE": 900000,
    "TOULOUSE": 1300000,
    "BORDEAUX": 1000000,
    "LILLE": 1200000,
    "ROUBAIX": 1200000,
    "TOURCOING": 1200000,
    "NANTES": 980000,
    "STRASBOURG": 850000,
    "MONTPELLIER": 780000,
    "RENNES": 750000,
    "GRENOBLE": 680000,
    "ST ETIENNE": 520000,
    "SAINT-ETIENNE": 520000,
    "SAINT ÉTIENNE": 520000,
    "NICE": 950000,
    "CANNES": 740000,
    "ANTIBES": 740000,
    "TOULON": 650000,
    "DIJON": 390000,
    "ANGERS": 420000,
    "AVIGNON": 300000,
    "METZ": 430000,
    "NANCY": 430000,
    "REIMS": 320000,
    "LE HAVRE": 330000,
    "BREST": 380000,
    "ORLEANS": 450000,
    "TOURS": 500000,
    "CLERMONT FERRAND": 480000,
    "CLERMONT-FERRAND": 480000,
    "PERPIGNAN": 320000,
    "PAU": 240000,
    "BAYONNE": 300000,
    "POITIERS": 250000,
    "LA ROCHELLE": 210000,
    "AMIENS": 320000,
    "CAEN": 420000,
    "ROUEN": 660000,
    "LIMOGES": 270000,
    "BESANCON": 280000,
    "BESANÇON": 280000,
    "ANNEMASSE": 250000,
    "ANNECY": 240000,
    "VALENCIENNES": 350000,
    "NIMES": 260000,
    "NÎMES": 260000,
    "NIORT": 200000,
    "QUIMPER": 200000,
    "COLMAR": 200000,
    "MULHOUSE": 280000,
    "SAINT-DENIS": 180000,
    "SAINT DENIS": 180000,
    "FORT DE FRANCE": 160000,
    "PAPEETE": 150000,
    "SAINT PIERRE": 120000,
    "AJACCIO": 170000,
    "BASTIA": 130000,
    "CHAMBERY": 250000,
    "CHAMBÉRY": 250000,
    "BAYEUX": 200000,
    "VANNES": 210000,
    "LORIENT": 210000,
    "MONTREUIL": 10800000,
    "SAINT MAUR DES FOSSES": 10800000,
    "VERSAILLES": 10800000,
    "NANTERRE": 10800000,
    "BOULOGNE BILLANCOURT": 10800000,
    "COURBEVOIE": 10800000,
}

CLUSTERS = [
    ("Grand Paris", ["75", "92", "93", "94", "91", "78", "95", "77"], 30),
    ("Lyon Métropole", ["69", "01", "38", "42"], 50),
    ("Aix-Marseille-Provence", ["13", "83", "84"], 40),
    ("Lille - Flandres", ["59", "62", "80"], 45),
    ("Toulouse & Occitanie Ouest", ["31", "32", "82", "81"], 50),
    ("Bordeaux - Nouvelle Aquitaine", ["33", "24", "47", "40"], 50),
    ("Nantes - Bretagne Sud", ["44", "49", "56", "85"], 45),
    ("Nice Côte d'Azur", ["06", "83"], 35),
    ("Strasbourg - Rhin Supérieur", ["67", "68"], 40),
    ("Rennes - Bretagne", ["35", "22", "29", "56"], 60),
]

PARIS_ARR_INFO = {
    1: ("Louvre, Châtelet", "Métro 1/7/14"),
    2: ("Bourse, Opéra", "Métro 3/7/14"),
    3: ("Haut Marais", "Métro 3/11"),
    4: ("Hôtel de Ville, Île de la Cité", "Métro 1/7"),
    5: ("Quartier Latin", "Métro 7/10/RER B"),
    6: ("Saint-Germain-des-Prés", "Métro 4/10"),
    7: ("Invalides, Ecole Militaire", "Métro 8/13"),
    8: ("Champs-Élysées", "Métro 1/9"),
    9: ("Grands Boulevards", "Métro 7/12"),
    10: ("Gares du Nord & Est", "Métro 4/5/RER B D"),
    11: ("Bastille, Oberkampf", "Métro 5/9/11"),
    12: ("Bercy, Nation", "Métro 1/6/14/RER A"),
    13: ("Butte-aux-Cailles", "Métro 5/6/7/RER C"),
    14: ("Montparnasse", "Métro 4/6/12/13"),
    15: ("Convention, Beaugrenelle", "Métro 8/10/12"),
    16: ("Trocadéro, Auteuil", "Métro 6/9/10"),
    17: ("Batignolles", "Métro 2/13/RER C"),
    18: ("Montmartre, La Chapelle", "Métro 2/4/12"),
    19: ("La Villette", "Métro 5/7"),
    20: ("Belleville, Ménilmontant", "Métro 2/3/11"),
}

CITY_COORDS = {
    "PARIS": (48.8566, 2.3522),
    "LYON": (45.7640, 4.8357),
    "MARSEILLE": (43.2965, 5.3698),
    "AIX EN PROVENCE": (43.5297, 5.4474),
    "TOULOUSE": (43.6045, 1.4442),
    "BORDEAUX": (44.8378, -0.5792),
    "LILLE": (50.6292, 3.0573),
    "NANTES": (47.2184, -1.5536),
    "STRASBOURG": (48.5734, 7.7521),
    "RENNES": (48.1173, -1.6778),
    "MONTPELLIER": (43.6108, 3.8767),
    "NICE": (43.7102, 7.2620),
    "GRENOBLE": (45.1885, 5.7245),
    "TOULON": (43.1242, 5.9280),
    "DIJON": (47.3220, 5.0415),
    "ANGERS": (47.4784, -0.5632),
    "AVIGNON": (43.9493, 4.8055),
    "METZ": (49.1193, 6.1757),
    "NANCY": (48.6921, 6.1844),
    "REIMS": (49.2583, 4.0317),
    "LE HAVRE": (49.4944, 0.1079),
    "BREST": (48.3904, -4.4861),
    "ORLEANS": (47.9029, 1.9093),
    "TOURS": (47.3941, 0.6848),
    "CLERMONT FERRAND": (45.7772, 3.0870),
    "CLERMONT-FERRAND": (45.7772, 3.0870),
    "PERPIGNAN": (42.6887, 2.8948),
    "PAU": (43.2951, -0.3708),
    "BAYONNE": (43.4927, -1.4748),
    "POITIERS": (46.5802, 0.3404),
    "LA ROCHELLE": (46.1603, -1.1511),
    "AMIENS": (49.8941, 2.2957),
    "CAEN": (49.1829, -0.3700),
    "ROUEN": (49.4432, 1.0993),
    "LIMOGES": (45.8336, 1.2611),
    "BESANCON": (47.2378, 6.0241),
    "BESANÇON": (47.2378, 6.0241),
    "ANNECY": (45.8992, 6.1294),
    "VALENCIENNES": (50.3570, 3.5230),
    "NIMES": (43.8367, 4.3601),
    "NÎMES": (43.8367, 4.3601),
    "NIORT": (46.3230, -0.4588),
    "QUIMPER": (47.9961, -4.0970),
    "COLMAR": (48.0798, 7.3585),
    "MULHOUSE": (47.7508, 7.3359),
    "SAINT DENIS": (48.9362, 2.3574),
    "SAINT-DENIS": (48.9362, 2.3574),
    "VERSAILLES": (48.8049, 2.1204),
    "NANTERRE": (48.8924, 2.2067),
    "BOULOGNE BILLANCOURT": (48.8397, 2.2399),
    "COURBEVOIE": (48.8978, 2.2566),
    "MONTREUIL": (48.8638, 2.4485),
    "SAINT MAUR DES FOSSES": (48.7939, 2.4945),
    "AULNAY SOUS BOIS": (48.9326, 2.4938),
    "CLICHY": (48.9047, 2.3070),
    "ISSY LES MOULINEAUX": (48.8210, 2.2770),
    "NEUILLY SUR SEINE": (48.8846, 2.2686),
    "CRETEIL": (48.7904, 2.4556),
    "VINCENNES": (48.8470, 2.4370),
    "MONTPELLIER": (43.6108, 3.8767),
    "SAINT HERBLAIN": (47.2187, -1.6496),
    "MERIGNAC": (44.8439, -0.6458),
    "PESSAC": (44.8100, -0.6410),
    "BAGNEUX": (48.7995, 2.3133),
}


class Record:
    __slots__ = (
        "ville",
        "ville_key",
        "postal_code",
        "department",
        "region_code",
        "actions",
        "nb_stagiaires",
        "effectif",
        "specialite",
    )

    def __init__(
        self,
        ville: str,
        ville_key: str,
        postal_code: Optional[str],
        department: Optional[str],
        region_code: Optional[str],
        actions: Optional[float],
        nb_stagiaires: Optional[float],
        effectif: Optional[float],
        specialite: str,
    ) -> None:
        self.ville = ville
        self.ville_key = ville_key
        self.postal_code = postal_code
        self.department = department
        self.region_code = region_code
        self.actions = actions
        self.nb_stagiaires = nb_stagiaires
        self.effectif = effectif
        self.specialite = specialite


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_city_key(name: Optional[str]) -> str:
    if not name:
        return ""
    text = name.strip().upper()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text


def normalize_postal_code(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    text = text.replace(" ", "")
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    if len(digits) >= 5:
        return digits[:5]
    if len(digits) == 4:
        return "0" + digits
    if len(digits) == 3:
        if digits.startswith(("97", "98")):
            return digits + "00"
        return digits
    return digits.zfill(5)


def department_from_postal_code(cp: Optional[str]) -> Optional[str]:
    if not cp:
        return None
    if cp.startswith("97") or cp.startswith("98"):
        return cp[:3]
    if cp.startswith("20"):
        third = cp[2] if len(cp) > 2 else "0"
        return "2A" if third in {"0", "1"} else "2B"
    return cp[:2]


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
                    idx = column_ref_to_index(ref)
                    values[idx] = get_cell_value(cell, shared_strings) or ""
                postal_code = normalize_postal_code(values.get(COL_CODE_POSTAL))
                department = department_from_postal_code(postal_code)
                ville = (values.get(COL_VILLE) or "").strip()
                ville_key = normalize_city_key(ville)
                record = Record(
                    ville=ville,
                    ville_key=ville_key,
                    postal_code=postal_code,
                    department=department,
                    region_code=(values.get(COL_CODE_REGION) or "").strip(),
                    actions=parse_float(values.get(COL_ACTIONS_FORMATION)),
                    nb_stagiaires=parse_float(values.get(COL_NB_STAGIAIRES)),
                    effectif=parse_float(values.get(COL_EFFECTIF_FORMATEURS)),
                    specialite=(values.get(COL_SPEC1) or "").strip(),
                )
                records.append(record)
                elem.clear()
    return records


def filter_tam(records: Iterable[Record]) -> List[Record]:
    result: List[Record] = []
    for rec in records:
        if rec.effectif is None:
            continue
        if not (TARGET_MIN_EFFECTIF <= rec.effectif <= TARGET_MAX_EFFECTIF):
            continue
        if rec.actions is None or rec.actions <= 0:
            continue
        if rec.nb_stagiaires is None or rec.nb_stagiaires <= 0:
            continue
        result.append(rec)
    return result


def format_int(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def format_float(value: float, decimals: int = 1) -> str:
    return f"{value:,.{decimals}f}".replace(",", " ")


def summarize_postal_codes(codes: List[str]) -> str:
    unique = sorted({c for c in codes if c})
    if not unique:
        return "-"
    if len(unique) == 1:
        return unique[0]
    prefixes = {c[:2] for c in unique if len(c) >= 2}
    if len(prefixes) == 1:
        prefix = prefixes.pop()
        suffix = "xxx"
        if prefix in {"97", "98"}:
            return prefix + "xxx"
        return prefix + suffix
    if len(unique) <= 3:
        return ", ".join(unique)
    return ", ".join(unique[:3]) + "…"


def event_type(count: int) -> str:
    if count >= 100:
        return "Conférence"
    if count >= 50:
        return "Meetup"
    if count >= 30:
        return "Atelier"
    return "Non viable"


def metro_population_label(city_key: str) -> str:
    pop = CITY_METRO_POP.get(city_key)
    if pop is None:
        return "-"
    if pop >= 1_000_000:
        return f"~{pop/1_000_000:.1f}M"
    return f"~{int(pop/1_000)}k"


def build_city_stats(records: List[Record]):
    city_stats: Dict[Tuple[str, Optional[str]], Dict[str, object]] = {}
    dept_totals: Counter = Counter()
    total_tam = 0
    for rec in records:
        if not rec.department:
            continue
        dept_totals[rec.department] += 1
        total_tam += 1
        key = (rec.ville_key, rec.department)
        stats = city_stats.setdefault(
            key,
            {
                "ville": rec.ville,
                "department": rec.department,
                "count": 0,
                "total_stagiaires": 0.0,
                "postaux": [],
                "specialites": Counter(),
            },
        )
        stats["count"] += 1
        if rec.nb_stagiaires:
            stats["total_stagiaires"] += rec.nb_stagiaires
        if rec.postal_code:
            stats["postaux"].append(rec.postal_code)
        if rec.specialite:
            stats["specialites"][rec.specialite] += 1
    return city_stats, dept_totals, total_tam


def format_department(dept: Optional[str]) -> str:
    if not dept:
        return "-"
    name = DEPARTMENT_NAMES.get(dept, "Inconnu")
    return f"{dept} ({name})"


def compute_table1(city_stats, dept_totals):
    rows = []
    for (ville_key, dept), stats in city_stats.items():
        count = int(stats["count"])
        dept_total = dept_totals.get(dept, 0)
        pct = (count / dept_total * 100) if dept_total else 0
        avg_stag = (stats["total_stagiaires"] / count) if count else 0
        cp_summary = summarize_postal_codes(stats["postaux"])
        ville_label = stats["ville"].title() if stats["ville"] else ville_key
        rows.append(
            {
                "ville_key": ville_key,
                "dept": dept,
                "ville_label": ville_label,
                "cp": cp_summary,
                "count": count,
                "pct": pct,
                "avg_stag": avg_stag,
                "pop": metro_population_label(ville_key),
                "event": event_type(count),
                "specialites": stats.get("specialites", Counter()),
            }
        )
    rows.sort(key=lambda r: (-r["count"], r["ville_key"]))
    table_rows = []
    for idx, row in enumerate(rows[:50], start=1):
        table_rows.append(
            [
                str(idx),
                row["ville_label"],
                row["cp"],
                format_department(row["dept"]),
                format_int(row["count"]),
                f"{row['pct']:.1f}%",
                format_float(row["avg_stag"], 0),
                row["pop"],
                row["event"],
            ]
        )
    return table_rows, rows


def compute_cluster_table(city_rows, dept_totals, total_tam):
    # Map dept to list of city entries
    dept_to_cities: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in city_rows:
        dept_to_cities[row["dept"]].append(row)

    cluster_rows = []
    for name, dept_list, radius in CLUSTERS:
        total = sum(dept_totals.get(dept, 0) for dept in dept_list)
        if total == 0:
            continue
        cities = []
        for dept in dept_list:
            cities.extend(dept_to_cities.get(dept, []))
        cities.sort(key=lambda c: -c["count"])
        top_cities = []
        for city in cities:
            label = city["ville_label"]
            cp = city["cp"]
            top_cities.append(f"{label} ({city['count']})")
            if len(top_cities) == 3:
                break
        part = (total / total_tam * 100) if total_tam else 0
        cluster_rows.append(
            [
                name,
                ", ".join(dept_list),
                format_int(total),
                f"{part:.1f}%",
                ", ".join(top_cities),
                f"{radius} km",
                event_type(total),
            ]
        )
    cluster_rows.sort(key=lambda r: -int(r[2].replace(" ", "")))
    return cluster_rows


def compute_paris_table(records: List[Record]):
    paris_records = [r for r in records if r.ville_key.startswith("PARIS") and r.postal_code]
    if not paris_records:
        return []
    arr_counts: Counter = Counter()
    arr_stag: Dict[int, float] = defaultdict(float)
    for rec in paris_records:
        arr = None
        city_suffix = rec.ville_key.replace("PARIS", "").strip()
        if city_suffix:
            digits = "".join(ch for ch in city_suffix if ch.isdigit())
            if digits:
                try:
                    arr_candidate = int(digits[-2:])
                    if 1 <= arr_candidate <= 20:
                        arr = arr_candidate
                except ValueError:
                    arr = None
        if arr is None:
            cp = rec.postal_code
            if len(cp) >= 5 and cp.startswith("75"):
                try:
                    arr_candidate = int(cp[-2:])
                    if 1 <= arr_candidate <= 20:
                        arr = arr_candidate
                except ValueError:
                    arr = None
        if 1 <= arr <= 20:
            arr_counts[arr] += 1
            if rec.nb_stagiaires:
                arr_stag[arr] += rec.nb_stagiaires
    total_paris = sum(arr_counts.values())
    table = []
    for arr in range(1, 21):
        count = arr_counts.get(arr, 0)
        if total_paris == 0:
            pct = 0
        else:
            pct = count / total_paris * 100
        zone, access = PARIS_ARR_INFO.get(arr, ("-", "-"))
        avg_stag = (arr_stag.get(arr, 0) / count) if count else 0
        table.append(
            [
                str(arr),
                format_int(count) if count else "0",
                f"{pct:.1f}%",
                zone,
                access,
            ]
        )
    table.sort(key=lambda r: -int(r[1].replace(" ", "")))
    return table, total_paris


def compute_mid_cities(city_rows):
    mid_rows = []
    for row in city_rows:
        count = row["count"]
        if 20 <= count <= 49:
            mid_rows.append(row)
    mid_rows.sort(key=lambda r: (-r["count"], r["ville_key"]))
    result = []
    for row in mid_rows:
        specialites = row.get("specialites") if isinstance(row.get("specialites"), Counter) else None
        if specialites:
            top_specialite = specialites.most_common(1)[0][0]
        else:
            top_specialite = "Divers"
        result.append(
            [
                row["ville_label"],
                format_department(row["dept"]),
                format_int(row["count"]),
                metro_population_label(row["ville_key"]),
                top_specialite or "Divers",
                "Atelier ciblé",
            ]
        )
    return result[:20]


def compute_deserts(dept_totals: Counter):
    rows = []
    for dept, count in dept_totals.items():
        if count < 10:
            name = DEPARTMENT_NAMES.get(dept, "Inconnu")
            interpretation = "Tissu local limité ou données incomplètes"
            strategie = "Webinaires nationaux"
            rows.append(
                [
                    dept,
                    name,
                    format_int(count),
                    interpretation,
                    strategie,
                ]
            )
    rows.sort(key=lambda r: int(r[2].replace(" ", "")))
    return rows


def compute_city_specialites(records: List[Record]):
    mapping: Dict[Tuple[str, Optional[str]], Counter] = defaultdict(Counter)
    for rec in records:
        if not rec.department:
            continue
        key = (rec.ville_key, rec.department)
        if rec.specialite:
            mapping[key][rec.specialite] += 1
    return mapping


def attach_specialites(city_stats, specialite_mapping):
    for key, counter in specialite_mapping.items():
        if key in city_stats:
            city_stats[key]["specialites"] = counter


def write_markdown(tables: Dict[str, List[List[str]]], synthesis: List[str]) -> None:
    ensure_output_dir()
    path = os.path.join(OUTPUT_DIR, "prompt11_clusters_denses.md")
    with open(path, "w", encoding="utf-8") as f:
        for title, table in tables.items():
            f.write(f"## {title}\n")
            if not table:
                f.write("*(Aucune donnée)*\n\n")
                continue
            header = table[0]
            rows = table[1:]
            f.write("| " + " | ".join(header) + " |\n")
            f.write("|" + "|".join([" --- " for _ in header]) + "|\n")
            for row in rows:
                f.write("| " + " | ".join(row) + " |\n")
            f.write("\n")
        f.write("## Synthèse\n")
        for line in synthesis:
            f.write(f"- {line}\n")
        f.write("\n")


def build_coord_csv(selected_cities: List[Tuple[str, str, str, str]]) -> None:
    ensure_output_dir()
    path = os.path.join(OUTPUT_DIR, "prompt11_villes_coordonnees.csv")
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ville", "departement", "event", "latitude", "longitude"])
        for ville_label, dept, event, ville_key in selected_cities:
            coords = CITY_COORDS.get(ville_key)
            if coords is None and ville_key.startswith("PARIS"):
                coords = CITY_COORDS.get("PARIS")
            if coords is None and " " in ville_key:
                base = ville_key.split(" ")[0]
                coords = CITY_COORDS.get(base)
            lat = f"{coords[0]:.4f}" if coords else ""
            lon = f"{coords[1]:.4f}" if coords else ""
            writer.writerow([ville_label, dept, event, lat, lon])


def build_tables():
    records = load_records()
    tam_records = filter_tam(records)
    city_stats, dept_totals, total_tam = build_city_stats(tam_records)
    specialites = compute_city_specialites(tam_records)
    attach_specialites(city_stats, specialites)

    table1_rows, city_rows = compute_table1(city_stats, dept_totals)
    table1 = [[
        "Rang",
        "Ville",
        "CP",
        "Dept",
        "OF TAM",
        "% dept",
        "Stag. moy",
        "Pop. metro",
        "Potentiel event",
    ]] + table1_rows

    cluster_rows = compute_cluster_table(city_rows, dept_totals, total_tam)
    table2 = [[
        "Cluster",
        "Départements",
        "OF TAM total",
        "% TAM",
        "Villes principales",
        "Rayon km",
        "Type event",
    ]] + cluster_rows

    paris_table, paris_total = compute_paris_table(tam_records)
    table3 = [[
        "Arrond.",
        "OF TAM",
        "% Paris",
        "Zones proches",
        "Accessibilité",
    ]] + paris_table

    mid_cities = compute_mid_cities(city_rows)
    table4 = [[
        "Ville",
        "Dept",
        "OF TAM",
        "Pop. metro",
        "Spé dominante",
        "Format event",
    ]] + mid_cities

    deserts = compute_deserts(dept_totals)
    table5 = [[
        "Dept",
        "Nom",
        "OF TAM",
        "Interprétation",
        "Stratégie",
    ]] + deserts

    planning_entries = build_planning(cluster_rows, city_rows)
    table6 = [[
        "Mois",
        "Ville/Cluster",
        "OF attendus",
        "Format",
        "Budget est.",
        "Priorité",
    ]] + [
        [
            entry["month"],
            entry["name"],
            format_int(entry["count"]),
            entry["event"],
            entry["budget"],
            entry["priority"],
        ]
        for entry in planning_entries
    ]

    tables = {
        "Tableau 1 - Villes à forte densité": table1,
        "Tableau 2 - Clusters multi-départements": table2,
        "Tableau 3 - Paris intra-muros": table3,
        "Tableau 4 - Villes moyennes (ateliers)": table4,
        "Tableau 5 - Zones à faible densité": table5,
        "Tableau 6 - Planning événements (6 mois)": table6,
    }

    synthesis = build_synthesis(cluster_rows, city_rows, mid_cities, deserts, total_tam, paris_total)
    write_markdown(tables, synthesis)

    selected_for_csv = []
    for entry in planning_entries:
        selected_for_csv.append(
            (
                entry["name"],
                entry.get("dept", ""),
                entry["event"],
                entry["ville_key"],
            )
        )
    build_coord_csv(selected_for_csv)


def build_planning(cluster_rows, city_rows):
    month_labels = ["M1", "M2", "M3", "M4", "M5", "M6"]
    priorities = ["🔴", "🔴", "🟠", "🟠", "🟢", "🟢"]

    cluster_events = []
    for row in cluster_rows[:3]:
        name = row[0]
        count = int(row[2].replace(" ", ""))
        evt = row[6]
        top_cities = row[4]
        primary_city = top_cities.split(",")[0] if top_cities else name
        primary_city_name = primary_city.split(" (")[0].strip()
        primary_key = normalize_city_key(primary_city_name)
        dept = row[1]
        cluster_events.append((name, count, evt, dept, primary_key, primary_city_name))

    city_events = []
    for row in city_rows:
        count = row["count"]
        if count < 20:
            continue
        city_events.append(
            (
                row["ville_label"],
                count,
                event_type(count),
                format_department(row["dept"]),
                row["ville_key"],
                row["ville_label"],
            )
        )

    combined = cluster_events + city_events

    planning_entries = []
    for idx, label in enumerate(month_labels):
        if idx >= len(combined):
            break
        name, count, evt, dept, ville_key, base_label = combined[idx]
        budget = "5-10K€" if evt == "Conférence" else "2-3K€" if evt == "Meetup" else "1-2K€"
        planning_entries.append(
            {
                "month": label,
                "name": name,
                "count": count,
                "event": evt,
                "budget": budget,
                "priority": priorities[idx],
                "dept": dept,
                "ville_key": ville_key,
            }
        )
    return planning_entries


def build_synthesis(cluster_rows, city_rows, mid_cities, deserts, total_tam, paris_total):
    synthesis = []
    num_clusters = len(cluster_rows)
    synthesis.append(f"CLUSTERS IDENTIFIÉS : {num_clusters} zones denses")
    top_clusters = cluster_rows[:5]
    lines = [f"{idx+1}. {row[0]} : {row[2]} OF TAM" for idx, row in enumerate(top_clusters)]
    synthesis.extend(lines)

    conference_cities = [row for row in city_rows if row["event"] == "Conférence"]
    meetup_cities = [row for row in city_rows if row["event"] == "Meetup"]
    atelier_cities = [row for row in city_rows if row["event"] == "Atelier"]
    synthesis.append(
        "Villes prioritaires : Conférences : "
        + ", ".join(row["ville_label"] for row in conference_cities[:5])
    )
    synthesis.append(
        "Villes prioritaires : Meetups : "
        + ", ".join(row["ville_label"] for row in meetup_cities[:5])
    )
    synthesis.append(
        "Villes prioritaires : Ateliers : "
        + ", ".join(row["ville_label"] for row in atelier_cities[:5])
    )

    synthesis.append(f"Déserts géographiques : {len(deserts)} départements <10 OF TAM")
    synthesis.append("Stratégie dominante : Webinaires nationaux")

    total_portee = sum(row["count"] for row in city_rows[:6])
    budget_min = 5 + 2 + 2 + 1 + 1 + 1
    budget_max = 10 + 3 + 3 + 2 + 2 + 2
    synthesis.append(
        f"Planning suggéré : 6 événements physiques, budget total ~{budget_min}-{budget_max}K€, portée directe ≈ {format_int(total_portee)} OF (Paris intra-muros : {format_int(paris_total)})"
    )
    return synthesis


if __name__ == "__main__":
    build_tables()

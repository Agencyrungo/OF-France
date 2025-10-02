import csv
import os
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass
from statistics import mean
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from analyze_specialites import MACRO_THEMES, classify_specialite


XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
OUTPUT_MARKDOWN = os.path.join(OUTPUT_DIR, "reseaux_nationaux.md")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "reseaux_top50.csv")

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

PRIORITY_THEMES = {"Soft Skills", "Tech/Digital", "Commerce/Gestion", "Santé"}


@dataclass
class Record:
    siren: str
    siret: str
    denomination: str
    region_code: Optional[int]
    effectif: Optional[int]
    nb_stagiaires: Optional[float]
    actions_form: Optional[float]
    specialite_label: Optional[str]

    @property
    def macro_theme(self) -> str:
        return classify_specialite(self.specialite_label)


@dataclass
class Network:
    siren: str
    denomination: str
    etablissements: List[Record]

    @property
    def siret_count(self) -> int:
        return len({rec.siret for rec in self.etablissements if rec.siret})

    @property
    def effectif_total(self) -> int:
        return sum(rec.effectif or 0 for rec in self.etablissements)

    @property
    def tam_records(self) -> List[Record]:
        return [rec for rec in self.etablissements if is_tam(rec)]

    @property
    def tam_count(self) -> int:
        return len(self.tam_records)

    @property
    def regions(self) -> List[str]:
        codes = {rec.region_code for rec in self.etablissements if rec.region_code is not None}
        names = [REGION_NAMES.get(code, "Autres territoires") for code in codes]
        return sorted(names)

    @property
    def coverage_type(self) -> str:
        nb_regions = len(self.regions)
        if nb_regions >= 5:
            return "Nationale"
        if nb_regions >= 2:
            return "Régionale"
        return "Locale"

    @property
    def main_theme(self) -> str:
        records = self.tam_records or self.etablissements
        counter: Counter[str] = Counter(rec.macro_theme for rec in records)
        if not counter:
            return "Autre"
        most_common = counter.most_common()
        best_count = most_common[0][1]
        candidates = [theme for theme, count in most_common if count == best_count]
        for theme in MACRO_THEMES:
            if theme in candidates:
                return theme
        return candidates[0]


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


def format_int(value: Optional[int]) -> str:
    return f"{value:,}".replace(",", " ") if value is not None else "-"


def format_float(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:,.{decimals}f}".replace(",", " ")


def safe_mean(values: Iterable[float]) -> Optional[float]:
    data = [v for v in values if v is not None]
    if not data:
        return None
    return mean(data)


def parse_identifier(value: Optional[str], length: Optional[int] = None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if digits:
        result = digits
    else:
        try:
            number = int(float(text))
            result = str(number)
        except ValueError:
            result = text
    if length and result.isdigit() and len(result) < length:
        result = result.zfill(length)
    return result


def normalize_name(name: str) -> str:
    import re

    text = name.upper()
    text = re.sub(r"[^A-Z0-9]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def classify_network_type(network: Network) -> Tuple[str, str]:
    names = [normalize_name(rec.denomination) for rec in network.etablissements if rec.denomination]
    if not names:
        return "Groupe intégré", "Données incomplètes"
    counter = Counter(names)
    top_name, top_count = counter.most_common(1)[0]
    share = top_count / len(names)
    keywords = {"FEDERATION", "UNION", "RESEAU", "COOPERATIVE", "GROUPEMENT", "SYNDICAT"}
    if share >= 0.7:
        return "Franchise", "Marque commune dominante"
    if any(any(keyword in name for keyword in keywords) for name in names):
        return "Coopérative", "Gouvernance partagée"
    return "Groupe intégré", "Portefeuille multi-marques"


def is_tam(record: Record) -> bool:
    if record.effectif is None or not (TARGET_MIN <= record.effectif <= TARGET_MAX):
        return False
    if record.nb_stagiaires is None or record.nb_stagiaires <= 0:
        return False
    if record.actions_form is None:
        return False
    return True


def load_records() -> List[Record]:
    records: List[Record] = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            header_map: Dict[int, str] = {}
            target_indices: Dict[str, int] = {}
            for event, elem in ET.iterparse(f, events=("end",)):
                if elem.tag != NS + "row":
                    continue
                row_index = int(elem.attrib.get("r"))
                values: Dict[int, str] = {}
                if row_index == 1:
                    for cell in elem.findall(NS + "c"):
                        ref = cell.attrib.get("r")
                        if not ref:
                            continue
                        idx = column_ref_to_index(ref)
                        val = get_cell_value(cell, shared_strings)
                        if val is not None:
                            header_map[idx] = val
                    target_indices = {
                        "denomination": next(
                            (idx for idx, name in header_map.items() if name == "denomination"),
                            None,
                        ),
                        "siren": next((idx for idx, name in header_map.items() if name == "siren"), None),
                        "siret": next(
                            (
                                idx
                                for idx, name in header_map.items()
                                if name == "siretEtablissementDeclarant"
                            ),
                            None,
                        ),
                        "region": next(
                            (
                                idx
                                for idx, name in header_map.items()
                                if name == "adressePhysiqueOrganismeFormation.codeRegion"
                            ),
                            None,
                        ),
                        "actions": next(
                            (
                                idx
                                for idx, name in header_map.items()
                                if name == "certifications.actionsDeFormation"
                            ),
                            None,
                        ),
                        "stagiaires": next(
                            (
                                idx
                                for idx, name in header_map.items()
                                if name == "informationsDeclarees.nbStagiaires"
                            ),
                            None,
                        ),
                        "effectif": next(
                            (
                                idx
                                for idx, name in header_map.items()
                                if name == "informationsDeclarees.effectifFormateurs"
                            ),
                            None,
                        ),
                        "specialite": next(
                            (
                                idx
                                for idx, name in header_map.items()
                                if name
                                == "informationsDeclarees.specialitesDeFormation.libelleSpecialite1"
                            ),
                            None,
                        ),
                    }
                    elem.clear()
                    continue

                indices = {idx for idx in target_indices.values() if idx is not None}
                if not indices:
                    continue
                for cell in elem.findall(NS + "c"):
                    ref = cell.attrib.get("r")
                    if not ref:
                        continue
                    idx = column_ref_to_index(ref)
                    if idx not in indices:
                        continue
                    val = get_cell_value(cell, shared_strings)
                    if val is not None:
                        values[idx] = val

                raw_siren = values.get(target_indices["siren"]) if target_indices["siren"] is not None else None
                raw_siret = values.get(target_indices["siret"]) if target_indices["siret"] is not None else None
                siren = parse_identifier(raw_siren, length=9)
                siret = parse_identifier(raw_siret, length=14)
                denomination = (
                    str(values.get(target_indices["denomination"], ""))
                    if target_indices["denomination"] is not None
                    else ""
                )
                if not siren:
                    elem.clear()
                    continue

                record = Record(
                    siren=siren.strip(),
                    siret=siret.strip(),
                    denomination=denomination.strip(),
                    region_code=parse_int(values.get(target_indices["region"])),
                    effectif=parse_int(values.get(target_indices["effectif"])),
                    nb_stagiaires=parse_float(values.get(target_indices["stagiaires"])),
                    actions_form=parse_float(values.get(target_indices["actions"])),
                    specialite_label=values.get(target_indices["specialite"]),
                )
                records.append(record)
                elem.clear()
    return records


def build_networks(records: Sequence[Record]) -> Tuple[List[Network], Dict[str, int]]:
    grouped: Dict[str, List[Record]] = defaultdict(list)
    for rec in records:
        if rec.siren:
            grouped[rec.siren].append(rec)

    networks: List[Network] = []
    diagnostics: Dict[str, int] = {
        "total_sirens": len(grouped),
        "multi_site": 0,
        "effectif_ok": 0,
        "tam_ok": 0,
    }
    for siren, recs in grouped.items():
        network = Network(siren=siren, denomination=select_network_name(recs), etablissements=recs)
        if network.siret_count < 3:
            continue
        diagnostics["multi_site"] += 1
        if network.effectif_total < 10:
            continue
        diagnostics["effectif_ok"] += 1
        if network.tam_count == 0:
            continue
        diagnostics["tam_ok"] += 1
        networks.append(network)
    diagnostics["effectif_missing"] = diagnostics["multi_site"] - diagnostics["effectif_ok"]
    diagnostics["tam_missing"] = diagnostics["effectif_ok"] - diagnostics["tam_ok"]
    return networks, diagnostics


def select_network_name(records: Sequence[Record]) -> str:
    counter = Counter(
        rec.denomination.strip() for rec in records if rec.denomination and rec.denomination.strip()
    )
    if not counter:
        return "Non renseigné"
    return counter.most_common(1)[0][0]


def compute_table1(networks: List[Network]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for rank, network in enumerate(sorted(networks, key=lambda n: n.effectif_total, reverse=True)[:50], 1):
        rows.append(
            {
                "rank": rank,
                "siren": network.siren,
                "name": network.denomination,
                "etab": network.siret_count,
                "effectif": network.effectif_total,
                "tam": network.tam_count,
                "regions": ", ".join(network.regions),
                "theme": network.main_theme,
            }
        )
    return rows


def categorize_size(nb: int) -> str:
    if nb <= 5:
        return "3-5 étab"
    if nb <= 10:
        return "6-10 étab"
    if nb <= 20:
        return "11-20 étab"
    return "21+ étab"


def compute_table2(networks: List[Network]) -> List[Dict[str, object]]:
    buckets: Dict[str, Dict[str, float]] = defaultdict(lambda: {"networks": 0, "etab": 0, "effectif": 0, "tam": 0})
    for network in networks:
        key = categorize_size(network.siret_count)
        buckets[key]["networks"] += 1
        buckets[key]["etab"] += network.siret_count
        buckets[key]["effectif"] += network.effectif_total
        buckets[key]["tam"] += network.tam_count

    ordered = ["3-5 étab", "6-10 étab", "11-20 étab", "21+ étab"]
    rows: List[Dict[str, object]] = []
    totals = {"networks": 0, "etab": 0, "effectif": 0, "tam": 0}
    for key in ordered:
        data = buckets.get(key, {"networks": 0, "etab": 0, "effectif": 0, "tam": 0})
        totals["networks"] += data["networks"]
        totals["etab"] += data["etab"]
        totals["effectif"] += data["effectif"]
        totals["tam"] += data["tam"]
        rows.append({"taille": key, **data})
    rows.append({"taille": "TOTAL", **totals})
    return rows


def compute_table3(networks: List[Network]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    theme_to_networks: Dict[str, List[Network]] = defaultdict(list)
    for network in networks:
        theme_to_networks[network.main_theme].append(network)

    themes = ["Soft Skills", "Tech/Digital", "Commerce/Gestion", "Santé"]
    rows.extend(_build_theme_rows(theme_to_networks, themes))
    other_networks = []
    for theme, nets in theme_to_networks.items():
        if theme not in themes:
            other_networks.extend(nets)
    if other_networks:
        best = max(other_networks, key=lambda n: (n.tam_count, n.effectif_total))
        rows.append(
            {
                "theme": "Autres",
                "name": best.denomination,
                "etab": best.siret_count,
                "tam": best.tam_count,
                "opportunity": format_opportunity(best),
            }
        )
    else:
        rows.append(
            {
                "theme": "Autres",
                "name": "Aucun réseau éligible",
                "etab": 0,
                "tam": 0,
                "opportunity": "Données à qualifier",
            }
        )
    return rows


def _build_theme_rows(theme_to_networks: Dict[str, List[Network]], themes: Sequence[str]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for theme in themes:
        nets = theme_to_networks.get(theme)
        if not nets:
            rows.append(
                {
                    "theme": theme,
                    "name": "Aucun réseau éligible",
                    "etab": 0,
                    "tam": 0,
                    "opportunity": "Données à qualifier",
                }
            )
            continue
        best = max(nets, key=lambda n: (n.tam_count, n.effectif_total))
        rows.append(
            {
                "theme": theme,
                "name": best.denomination,
                "etab": best.siret_count,
                "tam": best.tam_count,
                "opportunity": format_opportunity(best),
            }
        )
    return rows


def format_opportunity(network: Network) -> str:
    return f"{network.tam_count} OF TAM, {network.coverage_type.lower()}"


def compute_table4(top_networks: List[Network]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for network in top_networks[:20]:
        regions = network.regions
        rows.append(
            {
                "name": network.denomination,
                "etab": network.siret_count,
                "regions": len(regions),
                "regions_list": ", ".join(regions),
                "coverage": network.coverage_type,
            }
        )
    if not rows:
        rows.append(
            {
                "name": "Aucun réseau éligible",
                "etab": 0,
                "regions": 0,
                "regions_list": "-",
                "coverage": "-",
            }
        )
    return rows


def compute_table5(records: List[Record], networks: List[Network]) -> List[Dict[str, object]]:
    tam_records = [rec for rec in records if is_tam(rec)]
    siren_to_sirets: Dict[str, set[str]] = defaultdict(set)
    for rec in records:
        if rec.siren and rec.siret:
            siren_to_sirets[rec.siren].add(rec.siret)

    network_sirens = {network.siren for network in networks}
    network_recs = [rec for rec in tam_records if rec.siren in network_sirens]
    independent_recs = [rec for rec in tam_records if len(siren_to_sirets.get(rec.siren, set())) <= 2]

    total_tam = len(tam_records)
    return build_profile_rows(network_recs, independent_recs, total_tam)


def build_profile_rows(
    network_recs: List[Record], independent_recs: List[Record], total_tam: int
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []

    net_count = len(network_recs)
    indep_count = len(independent_recs)
    rows.append(
        {
            "metric": "Nombre OF",
            "networks": net_count,
            "independents": indep_count,
            "diff": diff_ratio(net_count, indep_count),
        }
    )

    rows.append(
        {
            "metric": "% TAM",
            "networks": percent(net_count, total_tam),
            "independents": percent(indep_count, total_tam),
            "diff": diff_points(percent(net_count, total_tam), percent(indep_count, total_tam)),
        }
    )

    rows.append(
        {
            "metric": "Stag. moyen",
            "networks": safe_mean([rec.nb_stagiaires for rec in network_recs]),
            "independents": safe_mean([rec.nb_stagiaires for rec in independent_recs]),
            "diff": diff_ratio(
                safe_mean([rec.nb_stagiaires for rec in network_recs]),
                safe_mean([rec.nb_stagiaires for rec in independent_recs]),
            ),
        }
    )

    rows.append(
        {
            "metric": "Effectif moyen",
            "networks": safe_mean([rec.effectif for rec in network_recs]),
            "independents": safe_mean([rec.effectif for rec in independent_recs]),
            "diff": diff_ratio(
                safe_mean([rec.effectif for rec in network_recs]),
                safe_mean([rec.effectif for rec in independent_recs]),
            ),
        }
    )

    rows.append(
        {
            "metric": "Production est.",
            "networks": safe_mean(production_ratio(rec) for rec in network_recs),
            "independents": safe_mean(production_ratio(rec) for rec in independent_recs),
            "diff": diff_ratio(
                safe_mean(production_ratio(rec) for rec in network_recs),
                safe_mean(production_ratio(rec) for rec in independent_recs),
            ),
        }
    )
    return rows


def production_ratio(record: Record) -> Optional[float]:
    if record.nb_stagiaires is None or record.effectif in (None, 0):
        return None
    return record.nb_stagiaires / record.effectif


def percent(count: Optional[float], total: int) -> Optional[float]:
    if count is None or total == 0:
        return None
    return (count / total) * 100


def diff_ratio(value_a: Optional[float], value_b: Optional[float]) -> Optional[str]:
    if value_a is None or value_b in (None, 0):
        return None
    diff = (value_a / value_b) - 1
    return format_percentage(diff)


def diff_points(value_a: Optional[float], value_b: Optional[float]) -> Optional[str]:
    if value_a is None or value_b is None:
        return None
    diff = value_a - value_b
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.1f} pts"


def format_percentage(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value * 100:.1f}%"


def safe_sum(values: Iterable[Optional[float]]) -> float:
    return sum(v or 0 for v in values)


def compute_table6(networks: List[Network]) -> List[Dict[str, object]]:
    if not networks:
        return [
            {
                "rank": "-",
                "name": "Aucun réseau éligible",
                "score": 0.0,
                "tam": 0,
                "coverage": "-",
                "theme": "-",
                "action": "Qualifier données",
            }
        ]
    max_tam = max(network.tam_count for network in networks)
    max_effectif = max(network.effectif_total for network in networks)

    rows: List[Dict[str, object]] = []
    for network in networks:
        score = 0.0
        score += 0.4 * (network.tam_count / max_tam if max_tam else 0)
        score += 0.3 * coverage_weight(network.coverage_type)
        score += 0.2 * (1.0 if network.main_theme in PRIORITY_THEMES else 0.0)
        score += 0.1 * (network.effectif_total / max_effectif if max_effectif else 0)
        rows.append(
            {
                "name": network.denomination,
                "score": score,
                "tam": network.tam_count,
                "coverage": network.coverage_type,
                "theme": network.main_theme,
                "action": recommended_action(network),
            }
        )

    rows.sort(key=lambda item: item["score"], reverse=True)
    for idx, row in enumerate(rows[:20], 1):
        row["rank"] = idx
    return rows[:20]


def coverage_weight(coverage: str) -> float:
    if coverage == "Nationale":
        return 1.0
    if coverage == "Régionale":
        return 0.6
    return 0.3


def recommended_action(network: Network) -> str:
    if network.coverage_type == "Nationale":
        return "Planifier RDV siège"
    if network.coverage_type == "Régionale":
        return "Identifier relais régionaux"
    return "Approche locale ciblée"


def compute_table7(networks: List[Network]) -> List[Dict[str, object]]:
    type_groups: Dict[str, List[Network]] = defaultdict(list)
    type_notes: Dict[str, str] = {}
    for network in networks:
        network_type, note = classify_network_type(network)
        type_groups[network_type].append(network)
        type_notes[network_type] = note

    rows: List[Dict[str, object]] = []
    for network_type in ["Franchise", "Groupe intégré", "Coopérative"]:
        nets = type_groups.get(network_type, [])
        if nets:
            avg_etab = sum(network.siret_count for network in nets) / len(nets)
            note = type_notes.get(network_type, "")
        else:
            avg_etab = 0.0
            note = "Aucun réseau éligible"
        rows.append(
            {
                "type": network_type,
                "count": len(nets),
                "avg_etab": avg_etab,
                "note": note,
            }
        )
    return rows


def build_summary(
    networks: List[Network],
    table1: List[Dict[str, object]],
    table5: List[Dict[str, object]],
    diagnostics: Dict[str, int],
) -> List[str]:
    total_networks = len(networks)
    top10 = table1[:10]
    summary_lines = [f"RÉSEAUX IDENTIFIÉS : {total_networks} réseaux (≥3 établissements)"]
    summary_lines.append("")
    summary_lines.append("Top 10 réseaux :")
    if top10:
        for row in top10:
            summary_lines.append(
                "{rank}. {name} : {etab} étab, {tam} OF TAM, {regions}".format(
                    rank=row["rank"],
                    name=row["name"],
                    etab=row["etab"],
                    tam=row["tam"],
                    regions=row["regions"],
                )
            )
    else:
        summary_lines.append("Aucun réseau éligible aux critères 2025.")

    national_networks = [n for n in networks if n.coverage_type == "Nationale"]
    total_access = sum(n.tam_count for n in national_networks)
    opportunity_lines = [
        "Opportunité partenariats :",
    ]
    if national_networks:
        opportunity_lines.append(
            f"- {len(national_networks)} réseaux nationaux = accès {total_access} OF TAM"
        )
    else:
        opportunity_lines.append("- Aucun réseau national éligible (données à qualifier)")
    if top10:
        head = top10[0]
        opportunity_lines.append(
            f"- {head['name']} = {head['tam']} OF dans TAM d'un coup"
        )
    else:
        opportunity_lines.append("- Priorité : enrichir les données effectif pour capter des réseaux TAM")

    tam_row = next((row for row in table5 if row["metric"] == "% TAM"), None)
    prod_row = next((row for row in table5 if row["metric"] == "Production est."), None)
    reseaux_vs = ["Réseaux vs indépendants :"]
    if tam_row and tam_row["networks"] is not None and tam_row["independents"] is not None:
        reseaux_vs.append(
            f"- Réseaux : {tam_row['networks']:.1f}% du TAM"
        )
        reseaux_vs.append(
            f"- Indépendants : {tam_row['independents']:.1f}% du TAM"
        )
    if prod_row and prod_row["diff"]:
        reseaux_vs.append(f"- Activité : {activity_label(prod_row['diff'])}")

    quality_lines: List[str] = []
    if diagnostics.get("effectif_missing", 0) or diagnostics.get("tam_missing", 0):
        quality_lines.append("Qualité des données à surveiller :")
        if diagnostics.get("effectif_missing", 0):
            quality_lines.append(
                f"- {diagnostics['effectif_missing']} réseaux multi-sites < 10 formateurs déclarés"
            )
        if diagnostics.get("tam_missing", 0):
            quality_lines.append(
                f"- {diagnostics['tam_missing']} réseaux multi-sites sans OF dans le TAM 3-10"
            )

    actions = ["Actions recommandées :"]
    if table1:
        lead = table1[0]
        actions.append(
            f"1. Engager {lead['name']} comme pilote partenariat"
        )
        if diagnostics.get("tam_missing", 0):
            actions.append(
                f"2. Qualifier les effectifs des {diagnostics['tam_missing']} réseaux hors TAM"
            )
        else:
            actions.append("2. Cartographier les relais locaux pour déploiement")
        actions.append("3. Préparer offre multi-sites adaptée (tarification volume)")
    else:
        actions.extend(
            [
                "1. Relancer les réseaux multi-sites pour compléter les effectifs",
                "2. Croiser avec bases Qualiopi/branches pour identifier des groupes",
                "3. Ajuster le ciblage TAM (3-10) selon disponibilité des données",
            ]
        )

    sections: List[str] = summary_lines
    sections.append("")
    sections.extend(opportunity_lines)
    sections.append("")
    sections.extend(reseaux_vs)
    if quality_lines:
        sections.append("")
        sections.extend(quality_lines)
    sections.append("")
    sections.extend(actions)
    return sections


def activity_label(diff_text: str) -> str:
    if diff_text.startswith("+"):
        return f"Supérieure ({diff_text})"
    if diff_text.startswith("-"):
        return f"Inférieure ({diff_text})"
    return f"Alignée ({diff_text})"


def write_markdown(
    table1: List[Dict[str, object]],
    table2: List[Dict[str, object]],
    table3: List[Dict[str, object]],
    table4: List[Dict[str, object]],
    table5: List[Dict[str, object]],
    table6: List[Dict[str, object]],
    table7: List[Dict[str, object]],
    summary: List[str],
) -> None:
    lines: List[str] = []
    lines.append("# Analyse réseaux nationaux OF France 2025")
    lines.append("")

    lines.append("## Tableau 1 : Top 50 réseaux multi-établissements")
    lines.append("| Rang | SIREN | Nom réseau | Nb étab | Effectif total | OF TAM 3-10 | Régions | Spé principale |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    if table1:
        for row in table1:
            lines.append(
                "| {rank} | {siren} | {name} | {etab} | {effectif} | {tam} | {regions} | {theme} |".format(
                    rank=row["rank"],
                    siren=row["siren"],
                    name=row["name"],
                    etab=row["etab"],
                    effectif=format_int(row["effectif"]),
                    tam=row["tam"],
                    regions=row["regions"],
                    theme=row["theme"],
                )
            )
    else:
        lines.append("| - | - | Aucun réseau éligible | - | - | - | - | - |")
    lines.append("")

    lines.append("## Tableau 2 : Segmentation taille des réseaux")
    lines.append("| Taille réseau | Nb réseaux | Nb étab total | Effectif total | OF TAM 3-10 |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in table2:
        effectif_val = int(round(row["effectif"])) if isinstance(row["effectif"], (int, float)) else 0
        lines.append(
            "| {taille} | {networks} | {etab} | {effectif} | {tam} |".format(
                taille=row["taille"],
                networks=int(round(row["networks"])),
                etab=int(round(row["etab"])),
                effectif=format_int(effectif_val),
                tam=int(round(row["tam"])),
            )
        )
    lines.append("")

    lines.append("## Tableau 3 : Réseaux dominants par domaine")
    lines.append("| Macro-thème | Top réseau | Nb étab | OF TAM | Opportunité |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in table3:
        lines.append(
            "| {theme} | {name} | {etab} | {tam} | {opp} |".format(
                theme=row["theme"],
                name=row["name"],
                etab=row["etab"],
                tam=row["tam"],
                opp=row["opportunity"],
            )
        )
    lines.append("")

    lines.append("## Tableau 4 : Implantation territoriale (Top 20)")
    lines.append("| Réseau | Nb étab | Nb régions | Régions présentes | Type couverture |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in table4:
        lines.append(
            "| {name} | {etab} | {regions} | {regions_list} | {coverage} |".format(
                name=row["name"],
                etab=row["etab"],
                regions=row["regions"],
                regions_list=row["regions_list"],
                coverage=row["coverage"],
            )
        )
    lines.append("")

    lines.append("## Tableau 5 : Réseaux vs indépendants (TAM)")
    lines.append("| Métrique | OF réseaux | OF indépendants | Différence |")
    lines.append("| --- | --- | --- | --- |")
    for row in table5:
        lines.append(
            "| {metric} | {net} | {indep} | {diff} |".format(
                metric=row["metric"],
                net=format_metric_value(row["networks"]),
                indep=format_metric_value(row["independents"]),
                diff=row["diff"] or "-",
            )
        )
    lines.append("")

    lines.append("## Tableau 6 : Top 20 réseaux prioritaires")
    lines.append("| Rang | Réseau | Score | OF TAM | Couverture | Spé | Action |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for row in table6:
        lines.append(
            "| {rank} | {name} | {score:.2f} | {tam} | {coverage} | {theme} | {action} |".format(
                rank=row["rank"],
                name=row["name"],
                score=row["score"],
                tam=row["tam"],
                coverage=row["coverage"],
                theme=row["theme"],
                action=row["action"],
            )
        )
    lines.append("")

    lines.append("## Tableau 7 : Typologie des réseaux")
    lines.append("| Type | Nb réseaux | Nb étab moy | Caractéristiques |")
    lines.append("| --- | --- | --- | --- |")
    for row in table7:
        lines.append(
            "| {type} | {count} | {avg:.1f} | {note} |".format(
                type=row["type"],
                count=row["count"],
                avg=row["avg_etab"],
                note=row["note"],
            )
        )
    lines.append("")

    lines.append("## Synthèse et actions")
    lines.extend(summary)

    ensure_output_dir()
    with open(OUTPUT_MARKDOWN, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def format_metric_value(value: Optional[float]) -> str:
    if value is None:
        return "-"
    if isinstance(value, float) and not value.is_integer():
        return format_float(value, 1)
    return format_int(int(value))


def write_csv_export(table1: List[Dict[str, object]], networks: List[Network]) -> None:
    ensure_output_dir()
    coverage_map = {network.denomination: network.coverage_type for network in networks}
    theme_map = {network.denomination: network.main_theme for network in networks}
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "rang",
                "siren",
                "nom",
                "nb_etablissements",
                "effectif_total",
                "of_tam",
                "couverture",
                "specialite",
                "action",
            ]
        )
        for row in table1:
            coverage = coverage_map.get(row["name"], "-")
            action = recommended_action_by_coverage(coverage)
            writer.writerow(
                [
                    row["rank"],
                    row["siren"],
                    row["name"],
                    row["etab"],
                    row["effectif"],
                    row["tam"],
                    coverage,
                    theme_map.get(row["name"], "Autre"),
                    action,
                ]
            )


def recommended_action_by_coverage(coverage: str) -> str:
    if coverage == "Nationale":
        return "Contacter direction nationale"
    if coverage == "Régionale":
        return "Structurer offre multi-régions"
    if coverage == "Locale":
        return "Proposer accompagnement de proximité"
    return "Qualifier contact"


def main() -> None:
    records = load_records()
    networks, diagnostics = build_networks(records)
    table1 = compute_table1(networks)
    table2 = compute_table2(networks)
    table3 = compute_table3(networks)
    top_networks = sorted(networks, key=lambda n: n.effectif_total, reverse=True)
    table4 = compute_table4(top_networks)
    table5 = compute_table5(records, networks)
    table6 = compute_table6(networks)
    table7 = compute_table7(networks)
    summary = build_summary(networks, table1, table5, diagnostics)
    write_markdown(table1, table2, table3, table4, table5, table6, table7, summary)
    write_csv_export(table1, networks)


if __name__ == "__main__":
    main()

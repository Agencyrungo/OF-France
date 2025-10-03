import csv
import os
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import re

from compute_tam import NS, column_ref_to_index, get_cell_value, load_shared_strings

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"

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
}

REGION_ORDER = [11, 84, 76, 93, 75, 44, 52, 32, 53, 28, 27, 24, 94]

SPECIALITE_PONCTUELLE_KEYWORDS = [
    "CONDUITE",
    "TRANSPORT",
    "SECUR",
    "PREVENT",
    "EVENEMENT",
    "SPECTACLE",
]


@dataclass
class OFRecord:
    denomination: str
    code_postal: Optional[str]
    region_code: Optional[int]
    effectif: Optional[int]
    actions: Optional[float]
    nb_stagiaires: Optional[float]
    nb_confies: Optional[float]
    date_declaration: Optional[str]
    specialites: List[str]

    @property
    def declaration_year(self) -> Optional[int]:
        if not self.date_declaration:
            return None
        text = self.date_declaration.strip()
        if not text:
            return None
        match = re.search(r"(19|20)\d{2}", text)
        if not match:
            return None
        year = int(match.group(0))
        if year < 1900 or year > 2100:
            return None
        return year

    @property
    def main_specialite(self) -> Optional[str]:
        return self.specialites[0] if self.specialites else None

    @property
    def specialite_count(self) -> int:
        return sum(1 for s in self.specialites if s)


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return float(text.replace(" ", ""))
    except ValueError:
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return int(float(text.replace(" ", "")))
    except ValueError:
        return None


def load_records() -> List[OFRecord]:
    records: List[OFRecord] = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            header_map: Dict[int, str] = {}
            target_indices: Dict[str, Optional[int]] = {
                "denomination": None,
                "code_postal": None,
                "region": None,
                "actions": None,
                "nb_stagiaires": None,
                "nb_confies": None,
                "effectif": None,
                "date_declaration": None,
                "specialite1": None,
                "specialite2": None,
                "specialite3": None,
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
                    target_indices["code_postal"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "adressePhysiqueOrganismeFormation.codePostal"
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
                    target_indices["actions"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "certifications.actionsDeFormation"
                        ),
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
                    target_indices["nb_confies"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "informationsDeclarees.nbStagiairesConfiesParUnAutreOF"
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
                    target_indices["date_declaration"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name == "informationsDeclarees.dateDerniereDeclaration"
                        ),
                        None,
                    )
                    target_indices["specialite1"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name
                            == "informationsDeclarees.specialitesDeFormation.libelleSpecialite1"
                        ),
                        None,
                    )
                    target_indices["specialite2"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name
                            == "informationsDeclarees.specialitesDeFormation.libelleSpecialite2"
                        ),
                        None,
                    )
                    target_indices["specialite3"] = next(
                        (
                            idx
                            for idx, name in header_map.items()
                            if name
                            == "informationsDeclarees.specialitesDeFormation.libelleSpecialite3"
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

                denomination = (values.get(target_indices["denomination"], "") or "").strip()
                code_postal = values.get(target_indices["code_postal"])
                if code_postal is not None:
                    code_postal = code_postal.strip()
                    if not code_postal:
                        code_postal = None
                region_code = parse_int(values.get(target_indices["region"]))
                actions = parse_float(values.get(target_indices["actions"]))
                nb_stagiaires = parse_float(values.get(target_indices["nb_stagiaires"]))
                nb_confies = parse_float(values.get(target_indices["nb_confies"]))
                effectif = parse_int(values.get(target_indices["effectif"]))
                raw_date = values.get(target_indices["date_declaration"])
                date_decl: Optional[str] = None
                if raw_date is not None:
                    text_date = raw_date.strip()
                    if text_date:
                        try:
                            serial = float(text_date)
                        except ValueError:
                            date_decl = text_date
                        else:
                            if serial > 0:
                                base = datetime(1899, 12, 30)
                                dt = base + timedelta(days=int(serial))
                                date_decl = dt.strftime("%Y-%m-%d")
                            else:
                                date_decl = None
                specialites: List[str] = []
                for key in ("specialite1", "specialite2", "specialite3"):
                    idx = target_indices[key]
                    if idx is None:
                        continue
                    val = values.get(idx)
                    if val is None:
                        continue
                    label = val.strip()
                    if label:
                        specialites.append(label)
                records.append(
                    OFRecord(
                        denomination=denomination,
                        code_postal=code_postal,
                        region_code=region_code,
                        effectif=effectif,
                        actions=actions,
                        nb_stagiaires=nb_stagiaires,
                        nb_confies=nb_confies,
                        date_declaration=date_decl,
                        specialites=specialites,
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


def region_name(code: Optional[int]) -> str:
    if code is None:
        return "Autres DOM-TOM"
    return REGION_NAMES.get(code, "Autres DOM-TOM")


def safe_mean(values: List[float]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def department_from_cp(cp: Optional[str]) -> str:
    if not cp:
        return "-"
    cp = cp.strip()
    if len(cp) >= 3 and cp.startswith(("97", "98")):
        return cp[:3]
    return cp[:2]


def classify_dormant(rec: OFRecord) -> str:
    year = rec.declaration_year
    if year is not None and year <= 2022:
        return "Cessation activité"
    if rec.specialite_count >= 2:
        return "Sous-déclaration"
    spe = (rec.main_specialite or "").upper()
    region = region_name(rec.region_code)
    if any(keyword in spe for keyword in SPECIALITE_PONCTUELLE_KEYWORDS) or region in {
        "Autres DOM-TOM",
        "Corse",
    }:
        return "Activité ponctuelle"
    if year is not None and year >= 2024:
        return "Reconversion récente"
    return "Activité ponctuelle"


def main() -> None:
    ensure_output_dir()
    records = load_records()

    certified = [
        r
        for r in records
        if r.effectif is not None
        and TARGET_MIN <= r.effectif <= TARGET_MAX
        and r.actions is not None
    ]

    dormants = [r for r in certified if (r.nb_stagiaires is None or r.nb_stagiaires == 0)]
    actives = [r for r in certified if r.nb_stagiaires is not None and r.nb_stagiaires > 0]

    total_certified = len(certified)
    total_dormants = len(dormants)
    total_actives = len(actives)

    # Table 1 metrics
    pct_dormants = (total_dormants / total_certified * 100) if total_certified else 0
    pct_actives = (total_actives / total_certified * 100) if total_certified else 0

    mean_eff_dormants = safe_mean([float(r.effectif) for r in dormants])
    mean_eff_actives = safe_mean([float(r.effectif) for r in actives])

    region_counter_dormants: Counter[str] = Counter(region_name(r.region_code) for r in dormants)
    region_counter_actives: Counter[str] = Counter(region_name(r.region_code) for r in actives)

    top_region_dormants, top_region_dormants_share = ("-", 0.0)
    if region_counter_dormants:
        region_code_counts: Dict[str, int] = dict(region_counter_dormants)
        top_region_dormants = max(region_code_counts.items(), key=lambda item: item[1])[0]
        top_region_dormants_share = region_code_counts[top_region_dormants] / total_dormants * 100

    top_region_actives, top_region_actives_share = ("-", 0.0)
    if region_counter_actives:
        region_code_counts_actives: Dict[str, int] = dict(region_counter_actives)
        top_region_actives = max(region_code_counts_actives.items(), key=lambda item: item[1])[0]
        top_region_actives_share = region_code_counts_actives[top_region_actives] / total_actives * 100

    specialite_counter_dormants: Counter[str] = Counter(
        r.main_specialite or "Non renseigné" for r in dormants
    )
    specialite_counter_actives: Counter[str] = Counter(
        r.main_specialite or "Non renseigné" for r in actives
    )

    top_spe_dormants, top_spe_dormants_share = ("-", 0.0)
    if specialite_counter_dormants:
        spe_counts_dormants = dict(specialite_counter_dormants)
        top_spe_dormants = max(spe_counts_dormants.items(), key=lambda item: item[1])[0]
        top_spe_dormants_share = spe_counts_dormants[top_spe_dormants] / total_dormants * 100

    top_spe_actives, top_spe_actives_share = ("-", 0.0)
    if specialite_counter_actives:
        spe_counts_actives = dict(specialite_counter_actives)
        top_spe_actives = max(spe_counts_actives.items(), key=lambda item: item[1])[0]
        top_spe_actives_share = spe_counts_actives[top_spe_actives] / total_actives * 100

    table1_rows = [
        [
            "Nombre OF",
            format_int(total_dormants),
            format_int(total_actives),
            format_int(total_dormants - total_actives),
        ],
        [
            "% de certifiés",
            format_percent(pct_dormants, 1),
            format_percent(pct_actives, 1),
            format_percent(pct_dormants - pct_actives, 1),
        ],
        [
            "Effectif moyen",
            format_float(mean_eff_dormants, 1),
            format_float(mean_eff_actives, 1),
            format_float(
                (mean_eff_dormants - mean_eff_actives)
                if mean_eff_dormants is not None and mean_eff_actives is not None
                else None,
                1,
            ),
        ],
        [
            "Région dominante",
            f"{top_region_dormants} ({top_region_dormants_share:.1f}%)" if total_dormants else "-",
            f"{top_region_actives} ({top_region_actives_share:.1f}%)" if total_actives else "-",
            (
                f"{top_region_dormants_share - top_region_actives_share:+.1f} pts"
                if total_dormants and total_actives and top_region_dormants == top_region_actives
                else "Différent"
            ),
        ],
        [
            "Spé dominante",
            f"{top_spe_dormants} ({top_spe_dormants_share:.1f}%)" if total_dormants else "-",
            f"{top_spe_actives} ({top_spe_actives_share:.1f}%)" if total_actives else "-",
            (
                f"{top_spe_dormants_share - top_spe_actives_share:+.1f} pts"
                if total_dormants and total_actives and top_spe_dormants == top_spe_actives
                else "Différent"
            ),
        ],
    ]

    # Table 2 - hypotheses
    hypotheses_order = [
        ("Reconversion récente", "Pas encore clients"),
        ("Cessation activité", "Certif maintenue"),
        ("Sous-déclaration", "Erreur données"),
        ("Activité ponctuelle", "Pas d'exercice déclaré"),
    ]

    hypothesis_counts: Counter[str] = Counter()
    for rec in dormants:
        hypothesis = classify_dormant(rec)
        hypothesis_counts[hypothesis] += 1

    table2_rows: List[List[str]] = []
    for label, hypothesis_text in hypotheses_order:
        count = hypothesis_counts.get(label, 0)
        pct = (count / total_dormants * 100) if total_dormants else 0
        table2_rows.append(
            [
                label,
                format_int(count),
                format_percent(pct, 1),
                hypothesis_text,
            ]
        )

    # Table 3 - geographic distribution
    region_certified_counts: Dict[str, int] = Counter(region_name(r.region_code) for r in certified)
    table3_rows: List[List[str]] = []
    for code in REGION_ORDER:
        name = region_name(code)
        dormants_count = sum(1 for r in dormants if region_name(r.region_code) == name)
        certified_count = region_certified_counts.get(name, 0)
        pct_region = (dormants_count / certified_count * 100) if certified_count else 0
        pct_national = (dormants_count / total_dormants * 100) if total_dormants else 0
        table3_rows.append(
            [
                name,
                format_int(dormants_count),
                format_percent(pct_region, 1),
                format_percent(pct_national, 1),
            ]
        )

    # Add other regions not in order
    listed_names = {region_name(code) for code in REGION_ORDER}
    for name, certified_count in sorted(region_certified_counts.items()):
        if name in listed_names:
            continue
        dormants_count = sum(1 for r in dormants if region_name(r.region_code) == name)
        pct_region = (dormants_count / certified_count * 100) if certified_count else 0
        pct_national = (dormants_count / total_dormants * 100) if total_dormants else 0
        table3_rows.append(
            [
                name,
                format_int(dormants_count),
                format_percent(pct_region, 1),
                format_percent(pct_national, 1),
            ]
        )

    total_row_table3 = [
        "TOTAL",
        format_int(total_dormants),
        format_percent((total_dormants / total_certified * 100) if total_certified else 0, 1),
        "100%",
    ]
    table3_rows.append(total_row_table3)

    # Table 4 - targeting segments
    recent_count = 0
    anciens_count = 0
    for r in dormants:
        year = r.declaration_year
        if year is None:
            anciens_count += 1
        elif year >= 2024:
            recent_count += 1
        else:
            anciens_count += 1
    multi_count = sum(1 for r in dormants if r.specialite_count >= 2)

    table4_rows = [
        [
            "Récents (2024-2025)",
            format_int(recent_count),
            "Nouveaux certifiés",
            "Accompagnement lancement",
        ],
        [
            "Anciens (≤2023)",
            format_int(anciens_count),
            "Certifiés depuis >1 an",
            "Réactivation",
        ],
        [
            "Multi-spécialités",
            format_int(multi_count),
            "Polyvalents",
            "Opportunité diversification",
        ],
    ]

    # Table 5 - sous-traitants
    sous_traitants = [
        r
        for r in certified
        if r.nb_confies is not None and r.nb_confies > 0
    ]
    total_sous_traitants = len(sous_traitants)
    total_confies = sum(r.nb_confies or 0 for r in sous_traitants)

    tranches: List[Tuple[str, int, Optional[int]]] = [
        ("1-50", 1, 50),
        ("51-200", 51, 200),
        ("201+", 201, None),
    ]

    table5_rows: List[List[str]] = []
    for label, lower, upper in tranches:
        subset = []
        for r in sous_traitants:
            value = r.nb_confies or 0
            if value < lower:
                continue
            if upper is not None and value > upper:
                continue
            subset.append(r)
        count = len(subset)
        pct = (count / total_sous_traitants * 100) if total_sous_traitants else 0
        avg_confies = safe_mean([r.nb_confies for r in subset])
        avg_total = safe_mean([r.nb_stagiaires for r in subset])
        ratios = [
            (r.nb_confies / r.nb_stagiaires)
            for r in subset
            if r.nb_stagiaires and r.nb_stagiaires > 0
        ]
        avg_ratio = safe_mean(ratios)
        table5_rows.append(
            [
                label,
                format_int(count),
                format_percent(pct, 1),
                format_float(avg_confies, 1),
                format_float(avg_total, 1),
                format_percent(avg_ratio * 100 if avg_ratio is not None else None, 1),
            ]
        )

    overall_avg_confies = safe_mean([r.nb_confies for r in sous_traitants])
    overall_avg_total = safe_mean([r.nb_stagiaires for r in sous_traitants])
    overall_ratio = safe_mean(
        [
            r.nb_confies / r.nb_stagiaires
            for r in sous_traitants
            if r.nb_stagiaires and r.nb_stagiaires > 0
        ]
    )
    table5_rows.append(
        [
            "TOTAL",
            format_int(total_sous_traitants),
            "100%",
            format_float(overall_avg_confies, 1),
            format_float(overall_avg_total, 1),
            format_percent(overall_ratio * 100 if overall_ratio is not None else None, 1),
        ]
    )

    # Table 6 - profile comparison
    non_sous_traitants = [r for r in actives if not (r.nb_confies and r.nb_confies > 0)]
    pct_sous_tam = (total_sous_traitants / total_actives * 100) if total_actives else 0
    pct_non_tam = (len(non_sous_traitants) / total_actives * 100) if total_actives else 0
    mean_eff_sous = safe_mean([float(r.effectif) for r in sous_traitants])
    mean_eff_non = safe_mean([float(r.effectif) for r in non_sous_traitants])
    mean_stag_sous = safe_mean([r.nb_stagiaires for r in sous_traitants])
    mean_stag_non = safe_mean([r.nb_stagiaires for r in non_sous_traitants])

    region_counter_sous: Counter[str] = Counter(region_name(r.region_code) for r in sous_traitants)
    region_counter_non: Counter[str] = Counter(region_name(r.region_code) for r in non_sous_traitants)

    top_region_sous, share_region_sous = ("-", 0.0)
    if region_counter_sous:
        region_counts = dict(region_counter_sous)
        top_region_sous = max(region_counts.items(), key=lambda item: item[1])[0]
        share_region_sous = region_counts[top_region_sous] / total_sous_traitants * 100

    top_region_non, share_region_non = ("-", 0.0)
    if region_counter_non:
        region_counts_non = dict(region_counter_non)
        top_region_non = max(region_counts_non.items(), key=lambda item: item[1])[0]
        share_region_non = region_counts_non[top_region_non] / len(non_sous_traitants) * 100

    spe_counter_sous: Counter[str] = Counter(r.main_specialite or "Non renseigné" for r in sous_traitants)
    spe_counter_non: Counter[str] = Counter(r.main_specialite or "Non renseigné" for r in non_sous_traitants)

    top_spe_sous, share_spe_sous = ("-", 0.0)
    if spe_counter_sous:
        spe_counts = dict(spe_counter_sous)
        top_spe_sous = max(spe_counts.items(), key=lambda item: item[1])[0]
        share_spe_sous = spe_counts[top_spe_sous] / total_sous_traitants * 100

    top_spe_non, share_spe_non = ("-", 0.0)
    if spe_counter_non:
        spe_counts_non = dict(spe_counter_non)
        top_spe_non = max(spe_counts_non.items(), key=lambda item: item[1])[0]
        share_spe_non = spe_counts_non[top_spe_non] / len(non_sous_traitants) * 100

    def diff_percent(value1: float, value2: float) -> str:
        return f"{value1 - value2:+.1f} pts"

    table6_rows = [
        [
            "Nombre OF",
            format_int(total_sous_traitants),
            format_int(len(non_sous_traitants)),
            format_int(total_sous_traitants - len(non_sous_traitants)),
        ],
        [
            "% TAM",
            format_percent(pct_sous_tam, 1),
            format_percent(pct_non_tam, 1),
            format_percent(pct_sous_tam - pct_non_tam, 1),
        ],
        [
            "Effectif moyen",
            format_float(mean_eff_sous, 1),
            format_float(mean_eff_non, 1),
            format_float(
                (mean_eff_sous - mean_eff_non)
                if mean_eff_sous is not None and mean_eff_non is not None
                else None,
                1,
            ),
        ],
        [
            "Stag total moy",
            format_float(mean_stag_sous, 1),
            format_float(mean_stag_non, 1),
            format_float(
                (mean_stag_sous - mean_stag_non)
                if mean_stag_sous is not None and mean_stag_non is not None
                else None,
                1,
            ),
        ],
        [
            "Région dominante",
            f"{top_region_sous} ({share_region_sous:.1f}%)" if total_sous_traitants else "-",
            f"{top_region_non} ({share_region_non:.1f}%)" if non_sous_traitants else "-",
            (
                diff_percent(share_region_sous, share_region_non)
                if total_sous_traitants
                and non_sous_traitants
                and top_region_sous == top_region_non
                else "Différent"
            ),
        ],
        [
            "Spé dominante",
            f"{top_spe_sous} ({share_spe_sous:.1f}%)" if total_sous_traitants else "-",
            f"{top_spe_non} ({share_spe_non:.1f}%)" if non_sous_traitants else "-",
            (
                diff_percent(share_spe_sous, share_spe_non)
                if total_sous_traitants and non_sous_traitants and top_spe_sous == top_spe_non
                else "Différent"
            ),
        ],
    ]

    # Table 7 - top 20 sous-traitants
    top20 = sorted(sous_traitants, key=lambda r: r.nb_confies or 0, reverse=True)[:20]
    table7_rows: List[List[str]] = []
    for rank, rec in enumerate(top20, start=1):
        ratio = None
        if rec.nb_stagiaires and rec.nb_stagiaires > 0:
            ratio = rec.nb_confies / rec.nb_stagiaires
        table7_rows.append(
            [
                str(rank),
                rec.denomination or "-",
                department_from_cp(rec.code_postal),
                format_int(rec.nb_confies),
                format_int(rec.nb_stagiaires),
                format_percent(ratio * 100 if ratio is not None else None, 1),
                rec.main_specialite or "Non renseigné",
            ]
        )

    # CSV exports
    top20_csv_path = os.path.join(OUTPUT_DIR, "prompt16_top20_sous_traitants.csv")
    with open(top20_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "rang",
            "denomination",
            "departement",
            "stagiaires_confies",
            "stagiaires_total",
            "ratio_confies",
            "specialite_principale",
        ])
        for row in table7_rows:
            ratio_value = row[5]
            writer.writerow([
                row[0],
                row[1],
                row[2],
                row[3].replace(" ", ""),
                row[4].replace(" ", ""),
                row[5],
                row[6],
            ])

    dormants_reactivables = [
        r
        for r in dormants
        if r.declaration_year is not None and r.declaration_year <= 2023
    ]
    dormants_csv_path = os.path.join(OUTPUT_DIR, "prompt16_dormants_reactivables.csv")
    with open(dormants_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "denomination",
            "region",
            "annee_derniere_declaration",
            "effectif",
            "specialites",
        ])
        for rec in dormants_reactivables:
            writer.writerow([
                rec.denomination,
                region_name(rec.region_code),
                rec.declaration_year or "-",
                rec.effectif or "",
                " | ".join(rec.specialites) if rec.specialites else "",
            ])

    # Markdown output
    markdown_lines: List[str] = []
    markdown_lines.append("## Analyse dormants et sous-traitance (effectif 3-10)")
    markdown_lines.append("")

    def write_table(title: str, headers: List[str], rows: List[List[str]]) -> None:
        markdown_lines.append(title)
        markdown_lines.append("| " + " | ".join(headers) + " |")
        markdown_lines.append("|" + "|".join([" --- " for _ in headers]) + "|")
        for row in rows:
            markdown_lines.append("| " + " | ".join(row) + " |")
        markdown_lines.append("")

    write_table(
        "### Tableau 1 : Profil OF dormants",
        ["Caractéristique", "OF dormants", "TAM actifs", "Écart"],
        table1_rows,
    )

    write_table(
        "### Tableau 2 : Répartition dormants par profil",
        ["Profil hypothétique", "OF estimés", "% dormants", "Hypothèse"],
        table2_rows,
    )

    write_table(
        "### Tableau 3 : Dormants par région",
        ["Région", "OF dormants", "% région", "% dormants_national"],
        table3_rows,
    )

    write_table(
        "### Tableau 4 : Ciblage dormants",
        ["Segment", "OF", "Caractéristiques", "Action"],
        table4_rows,
    )

    write_table(
        "### Tableau 5 : OF sous-traitants (dans TAM)",
        ["Tranche confiés", "OF", "%", "Stag confiés moy", "Stag total moy", "Ratio"],
        table5_rows,
    )

    write_table(
        "### Tableau 6 : Caractéristiques sous-traitants",
        ["Métrique", "OF sous-traitants", "Non sous-traitants", "Différence"],
        table6_rows,
    )

    write_table(
        "### Tableau 7 : Top 20 sous-traitants (volume confiés)",
        ["Rang", "Dénomination", "Dept", "Stag confiés", "Stag total", "Ratio", "Spé"],
        table7_rows,
    )

    # Synthesis section
    top_regions = region_counter_dormants.most_common(2)
    regions_text = ", ".join(
        f"{name} ({count / total_dormants * 100:.1f}%)" for name, count in top_regions
    ) if total_dormants else "-"

    top_hypothesis = hypothesis_counts.most_common(1)
    hypothesis_text = (
        f"{top_hypothesis[0][0]} ({top_hypothesis[0][1] / total_dormants * 100:.1f}%)"
        if top_hypothesis and total_dormants
        else "-"
    )

    top_ratio_entries = table7_rows[:3]
    markdown_lines.append("### Synthèse")
    markdown_lines.append("")
    markdown_lines.append(
        f"**OF dormants** : {format_int(total_dormants)} ({pct_dormants:.1f}% des certifiés 3-10)."
    )
    markdown_lines.append(
        f"- Effectif moyen : {format_float(mean_eff_dormants, 1)} formateurs"
    )
    markdown_lines.append(f"- Régions dominantes : {regions_text}")
    markdown_lines.append(f"- Hypothèse principale : {hypothesis_text}")
    markdown_lines.append("")
    markdown_lines.append("**Opportunité** :")
    markdown_lines.append(
        f"- Récents (2024-2025) : {format_int(recent_count)} OF → Accompagnement"
    )
    markdown_lines.append(
        f"- Anciens : {format_int(anciens_count)} OF → Réactivation"
    )
    markdown_lines.append("- Messaging : « Relancez votre activité avec Qalia »")
    markdown_lines.append("")
    markdown_lines.append("**Sous-traitance** :")
    markdown_lines.append(
        f"- OF confiant des stagiaires : {format_int(total_sous_traitants)} ({pct_sous_tam:.1f}% du TAM actif)"
    )
    markdown_lines.append(
        f"- Stagiaires confiés total : {format_int(total_confies)}"
    )
    markdown_lines.append(
        f"- Effectif moyen : {format_float(mean_eff_sous, 1)} formateurs ; ratio moyen : {format_percent(overall_ratio * 100 if overall_ratio is not None else None, 1)}"
    )
    if top_ratio_entries:
        markdown_lines.append("- Top sous-traitants :")
        for row in top_ratio_entries:
            markdown_lines.append(
                f"  - {row[1]} ({row[2]}) : {row[3]} stagiaires confiés"
            )
    markdown_lines.append(
        "- Opportunité : Contacter le top 20 pour cooptation et animer l'offre « Réseau »"
    )

    output_path = os.path.join(OUTPUT_DIR, "prompt16_dormants_sous_traitance.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(markdown_lines))


if __name__ == "__main__":
    main()

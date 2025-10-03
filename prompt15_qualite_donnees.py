import csv
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterable, List, Optional, Tuple

from compute_tam import (
    NS,
    OUTPUT_DIR,
    REGION_NAMES,
    TARGET_MAX,
    TARGET_MIN,
    column_ref_to_index,
    get_cell_value,
    load_shared_strings,
)

XLSX_PATH = "OF 3-10.xlsx"

TARGET_HEADERS = {
    "nda": "numeroDeclarationActivite",
    "denomination": "denomination",
    "effectif": "informationsDeclarees.effectifFormateurs",
    "stagiaires": "informationsDeclarees.nbStagiaires",
    "region": "adressePhysiqueOrganismeFormation.codeRegion",
    "cp": "adressePhysiqueOrganismeFormation.codePostal",
    "ville": "adressePhysiqueOrganismeFormation.ville",
    "voie": "adressePhysiqueOrganismeFormation.voie",
    "actions": "certifications.actionsDeFormation",
    "spe1": "informationsDeclarees.specialitesDeFormation.libelleSpecialite1",
    "spe2": "informationsDeclarees.specialitesDeFormation.libelleSpecialite2",
    "spe3": "informationsDeclarees.specialitesDeFormation.libelleSpecialite3",
}


@dataclass
class OFRecord:
    nda: Optional[str]
    denomination: Optional[str]
    effectif: Optional[float]
    stagiaires: Optional[float]
    region_code: Optional[int]
    code_postal: Optional[str]
    ville: Optional[str]
    voie: Optional[str]
    actions: Optional[str]
    spe1: Optional[str]
    spe2: Optional[str]
    spe3: Optional[str]


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def parse_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def parse_float(value: Optional[str]) -> Optional[float]:
    text = parse_text(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    text = parse_text(value)
    if text is None:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def normalize_numeric_text(value: Optional[str], pad_to: Optional[int] = None) -> Optional[str]:
    text = parse_text(value)
    if text is None:
        return None
    normalized = text
    try:
        decimal_value = Decimal(text)
    except InvalidOperation:
        return normalized
    if decimal_value == decimal_value.to_integral():
        normalized = str(int(decimal_value))
    else:
        normalized = format(decimal_value, "f")
    if pad_to is not None and normalized.isdigit():
        if len(normalized) < pad_to:
            normalized = normalized.zfill(pad_to)
    return normalized


def load_records() -> List[OFRecord]:
    records: List[OFRecord] = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            header_map: Dict[int, str] = {}
            target_indices: Dict[str, Optional[int]] = {key: None for key in TARGET_HEADERS}
            for event, elem in ET.iterparse(f, events=("end",)):
                if elem.tag != NS + "row":
                    continue
                row_index = int(elem.attrib.get("r"))
                if row_index == 1:
                    for cell in elem.findall(NS + "c"):
                        ref = cell.attrib.get("r")
                        if not ref:
                            continue
                        col_idx = column_ref_to_index(ref)
                        header_map[col_idx] = get_cell_value(cell, shared_strings)
                    for key, header_name in TARGET_HEADERS.items():
                        target_indices[key] = next(
                            (idx for idx, name in header_map.items() if name == header_name),
                            None,
                        )
                    elem.clear()
                    continue

                values: Dict[str, Optional[str]] = {key: None for key in TARGET_HEADERS}
                for cell in elem.findall(NS + "c"):
                    ref = cell.attrib.get("r")
                    if not ref:
                        continue
                    col_idx = column_ref_to_index(ref)
                    for key, idx in target_indices.items():
                        if idx is not None and col_idx == idx:
                            values[key] = get_cell_value(cell, shared_strings)
                record = OFRecord(
                    nda=normalize_numeric_text(values["nda"]),
                    denomination=parse_text(values["denomination"]),
                    effectif=parse_float(values["effectif"]),
                    stagiaires=parse_float(values["stagiaires"]),
                    region_code=parse_int(values["region"]),
                    code_postal=normalize_numeric_text(values["cp"], pad_to=5),
                    ville=parse_text(values["ville"]),
                    voie=parse_text(values["voie"]),
                    actions=parse_text(values["actions"]),
                    spe1=parse_text(values["spe1"]),
                    spe2=parse_text(values["spe2"]),
                    spe3=parse_text(values["spe3"]),
                )
                records.append(record)
                elem.clear()
    return records


def presence(value: Optional[str]) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def format_int(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def format_percent(value: float, decimals: int = 1) -> str:
    return f"{value:.{decimals}f}%"


def classify_field(pct: float) -> str:
    if pct > 90:
        return "✅"
    if pct >= 50:
        return "⚠️"
    return "❌"


def classify_quality(cp_pct: float) -> str:
    if cp_pct > 80:
        return "Excellente"
    if cp_pct >= 60:
        return "Bonne"
    if cp_pct >= 40:
        return "Moyenne"
    return "Faible"


def acquisition_strategy(cp_pct: float) -> str:
    if cp_pct > 70:
        return "Ads géo Facebook/LinkedIn"
    if cp_pct >= 50:
        return "LinkedIn organique + Ads large"
    return "Contenu national uniquement"


def safe_mean(values: Iterable[float]) -> Optional[float]:
    data = [v for v in values if v is not None]
    if not data:
        return None
    return sum(data) / len(data)


def format_optional(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:.{decimals}f}"


def format_difference(base: Optional[float], compare: Optional[float]) -> str:
    if base is None or compare is None or base == 0:
        return "-"
    diff = (compare - base) / base * 100
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.1f}%"


def format_pp_difference(base: Optional[float], compare: Optional[float]) -> str:
    if base is None or compare is None:
        return "-"
    diff = (compare - base) * 100
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.1f} pp"


def dominant_region(records: List[OFRecord]) -> str:
    counts: Counter[int] = Counter()
    for rec in records:
        if rec.region_code is None:
            continue
        counts[rec.region_code] += 1
    if not counts:
        return "-"
    code, _ = counts.most_common(1)[0]
    return REGION_NAMES.get(code, "Autres DOM-TOM")


def write_markdown_table(lines: List[str], title: str, headers: List[str], rows: List[List[str]]) -> None:
    lines.append(title)
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join([" --- " for _ in headers]) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")


try:
    import zipfile
    import xml.etree.ElementTree as ET
except ImportError as exc:  # pragma: no cover
    raise SystemExit(f"Missing standard library dependency: {exc}")


def main() -> None:
    ensure_output_dir()
    records = load_records()
    total_records = len(records)

    completeness_fields = [
        ("numeroDeclarationActivite", "nda"),
        ("denomination", "denomination"),
        ("effectifFormateurs", "effectif"),
        ("nbStagiaires", "stagiaires"),
        ("codeRegion", "region_code"),
        ("codePostal", "code_postal"),
        ("ville", "ville"),
        ("voie", "voie"),
        ("actionsDeFormation", "actions"),
        ("libelleSpecialite1", "spe1"),
        ("libelleSpecialite2", "spe2"),
        ("libelleSpecialite3", "spe3"),
    ]

    field_counts: Dict[str, int] = {}
    for field_key, attr in completeness_fields:
        count = sum(1 for rec in records if presence(getattr(rec, attr)))
        field_counts[field_key] = count

    table1_rows: List[List[str]] = []
    for field_key, attr in completeness_fields:
        count = field_counts[field_key]
        pct = (count / total_records * 100) if total_records else 0
        status = classify_field(pct)
        table1_rows.append(
            [
                field_key,
                format_int(count),
                format_percent(pct),
                status,
            ]
        )

    tam_records = [
        rec
        for rec in records
        if rec.effectif is not None
        and TARGET_MIN <= rec.effectif <= TARGET_MAX
        and presence(rec.actions)
        and rec.stagiaires is not None
        and rec.stagiaires > 0
    ]

    region_stats: Dict[int, Dict[str, float]] = {}
    region_records: Dict[int, List[OFRecord]] = defaultdict(list)
    for rec in tam_records:
        code = rec.region_code if rec.region_code is not None else -1
        region_records[code].append(rec)

    table2_rows: List[List[str]] = []
    for code, recs in sorted(region_records.items(), key=lambda item: REGION_NAMES.get(item[0], "zzz")):
        total = len(recs)
        cp_pct = sum(1 for r in recs if presence(r.code_postal)) / total * 100 if total else 0
        ville_pct = sum(1 for r in recs if presence(r.ville)) / total * 100 if total else 0
        voie_pct = sum(1 for r in recs if presence(r.voie)) / total * 100 if total else 0
        quality = classify_quality(cp_pct)
        name = REGION_NAMES.get(code, "Autres DOM-TOM")
        table2_rows.append(
            [
                name,
                format_int(total),
                format_percent(cp_pct),
                format_percent(ville_pct),
                format_percent(voie_pct),
                quality,
            ]
        )
        region_stats[code] = {
            "total": total,
            "cp_pct": cp_pct,
            "ville_pct": ville_pct,
            "voie_pct": voie_pct,
            "name": name,
        }

    total_tam = len(tam_records)
    if total_tam:
        cp_total_pct = sum(1 for r in tam_records if presence(r.code_postal)) / total_tam * 100
        ville_total_pct = sum(1 for r in tam_records if presence(r.ville)) / total_tam * 100
        voie_total_pct = sum(1 for r in tam_records if presence(r.voie)) / total_tam * 100
    else:
        cp_total_pct = ville_total_pct = voie_total_pct = 0
    table2_rows.append(
        [
            "France",
            format_int(total_tam),
            format_percent(cp_total_pct),
            format_percent(ville_total_pct),
            format_percent(voie_total_pct),
            classify_quality(cp_total_pct),
        ]
    )

    table3_rows: List[List[str]] = []
    for code, stats in sorted(region_stats.items(), key=lambda item: item[1]["cp_pct"], reverse=True):
        table3_rows.append(
            [
                stats["name"],
                format_percent(stats["cp_pct"]),
                acquisition_strategy(stats["cp_pct"]),
            ]
        )

    with_cp = [rec for rec in records if presence(rec.code_postal)]
    without_cp = [rec for rec in records if not presence(rec.code_postal)]

    effectif_with = safe_mean(r.effectif for r in with_cp if r.effectif is not None)
    effectif_without = safe_mean(r.effectif for r in without_cp if r.effectif is not None)
    stag_with = safe_mean(r.stagiaires for r in with_cp if r.stagiaires is not None)
    stag_without = safe_mean(r.stagiaires for r in without_cp if r.stagiaires is not None)
    cert_with = sum(1 for r in with_cp if presence(r.actions)) / len(with_cp) if with_cp else None
    cert_without = sum(1 for r in without_cp if presence(r.actions)) / len(without_cp) if without_cp else None

    table4_rows = [
        [
            "Effectif moyen",
            format_optional(effectif_with),
            format_optional(effectif_without),
            format_difference(effectif_with, effectif_without),
        ],
        [
            "Stagiaires moyens",
            format_optional(stag_with),
            format_optional(stag_without),
            format_difference(stag_with, stag_without),
        ],
        [
            "Taux certif",
            format_optional(cert_with * 100 if cert_with is not None else None),
            format_optional(cert_without * 100 if cert_without is not None else None),
            format_pp_difference(cert_with, cert_without),
        ],
        [
            "Région dominante",
            dominant_region(with_cp),
            dominant_region(without_cp),
            "-",
        ],
    ]

    speciality_counter = Counter()
    for rec in records:
        count = sum(1 for field in (rec.spe1, rec.spe2, rec.spe3) if presence(field))
        speciality_counter[count] += 1

    labels = {
        0: "Aucune",
        1: "Minimale",
        2: "Bonne",
        3: "Complète",
    }
    table5_rows: List[List[str]] = []
    for count in range(0, 4):
        total = speciality_counter.get(count, 0)
        pct = total / total_records * 100 if total_records else 0
        table5_rows.append(
            [
                str(count),
                format_int(total),
                format_percent(pct),
                labels.get(count, "-"),
            ]
        )

    cp_missing_pct = 100 - (field_counts["codePostal"] / total_records * 100 if total_records else 0)
    spe2_missing_pct = 100 - (field_counts["libelleSpecialite2"] / total_records * 100 if total_records else 0)
    spe3_missing_pct = 100 - (field_counts["libelleSpecialite3"] / total_records * 100 if total_records else 0)

    table6_rows = [
        [
            f"{cp_missing_pct:.0f}% CP manquants",
            "Limite ads géo",
            "Enrichissement externe (LinkedIn)",
            "🔴 HAUTE",
        ],
        [
            f"{spe2_missing_pct:.0f}% Spé2 manquants",
            "Limite segmentation",
            "Acceptable (Spé1 suffit)",
            "🟢 BASSE",
        ],
        [
            f"{spe3_missing_pct:.0f}% Spé3 manquants",
            "Limite analyse",
            "Acceptable",
            "🟢 BASSE",
        ],
    ]

    markdown_lines: List[str] = []
    write_markdown_table(
        markdown_lines,
        "Tableau 1 : Taux de complétude des champs",
        ["Champ", "Valeurs renseignées", "% complétude", "Utilisable ?"],
        table1_rows,
    )
    write_markdown_table(
        markdown_lines,
        "Tableau 2 : Qualité des données régionales (TAM)",
        ["Région", "OF TAM", "% codePostal", "% ville", "% voie", "Qualité globale"],
        table2_rows,
    )
    write_markdown_table(
        markdown_lines,
        "Tableau 3 : Faisabilité du ciblage par région",
        ["Région", "Complétude CP", "Stratégie acquisition"],
        table3_rows,
    )
    write_markdown_table(
        markdown_lines,
        "Tableau 4 : Profil des données manquantes",
        ["Caractéristique", "OF avec CP", "OF sans CP", "Différence"],
        table4_rows,
    )
    write_markdown_table(
        markdown_lines,
        "Tableau 5 : Niveau de spécialités renseignées",
        ["Nb spés renseignées", "OF", "%", "Complétude"],
        table5_rows,
    )
    write_markdown_table(
        markdown_lines,
        "Tableau 6 : Plan d'amélioration de la qualité",
        ["Problème", "Impact", "Action recommandée", "Priorité"],
        table6_rows,
    )

    excellent_fields = [
        name
        for name, attr in completeness_fields
        if (field_counts[name] / total_records * 100 if total_records else 0) > 90
    ]
    weak_fields = [
        name
        for name, attr in completeness_fields
        if (field_counts[name] / total_records * 100 if total_records else 0) < 60
    ]

    regions_above_70 = sum(1 for stats in region_stats.values() if stats["cp_pct"] > 70)
    regions_between_50_70 = sum(1 for stats in region_stats.values() if 50 <= stats["cp_pct"] <= 70)
    regions_below_50 = sum(1 for stats in region_stats.values() if stats["cp_pct"] < 50)

    bias_conclusion = "Systématique" if abs((stag_without or 0) - (stag_with or 0)) > 10 else "Aléatoire"

    markdown_lines.append("## Synthèse")
    markdown_lines.append("QUALITÉ DONNÉES :")
    markdown_lines.append("Champs excellents (>90%) :")
    if excellent_fields:
        for field in excellent_fields:
            markdown_lines.append(f"- {field}")
    else:
        markdown_lines.append("- Aucun")
    markdown_lines.append("")
    markdown_lines.append("Champs problématiques (<60%) :")
    if weak_fields:
        for field in weak_fields:
            markdown_lines.append(f"- {field}")
    else:
        markdown_lines.append("- Aucun")
    markdown_lines.append("")
    markdown_lines.append("Impact ciblage :")
    markdown_lines.append(
        f"- Régions >70% CP : {regions_above_70} → Ads géo OK"
    )
    markdown_lines.append(
        f"- Régions 50-70% CP : {regions_between_50_70} → LinkedIn organique / Ads large"
    )
    markdown_lines.append(
        f"- Régions <50% CP : {regions_below_50} → Contenu organique"
    )
    markdown_lines.append("")
    markdown_lines.append("Biais données manquantes :")
    markdown_lines.append(f"- {bias_conclusion}")
    markdown_lines.append(
        f"- Impact : Stagiaires moyens sans CP {format_optional(stag_without)} vs {format_optional(stag_with)}"
    )
    markdown_lines.append("")
    markdown_lines.append("Recommandations :")
    markdown_lines.append("1. Enrichissement CP via LinkedIn (priorité HAUTE)")
    markdown_lines.append("2. Utiliser la région comme fallback de ciblage")
    markdown_lines.append("3. Spécialité 1 suffisante pour segmentation actuelle")

    output_path = os.path.join(OUTPUT_DIR, "prompt15_qualite_donnees.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(markdown_lines))

    csv_path = os.path.join(OUTPUT_DIR, "prompt15_of_sans_cp.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["codeRegion", "region", "of_tam_total", "of_sans_cp", "pct_sans_cp"])
        for code, stats in sorted(region_stats.items(), key=lambda item: item[1]["name"]):
            total = stats["total"]
            sans_cp = sum(1 for r in region_records[code] if not presence(r.code_postal))
            pct = sans_cp / total * 100 if total else 0
            writer.writerow([
                code,
                stats["name"],
                total,
                sans_cp,
                f"{pct:.1f}",
            ])


if __name__ == "__main__":
    main()

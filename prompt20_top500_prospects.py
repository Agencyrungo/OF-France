import csv
import math
import os
import statistics
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from analyze_specialites import REGION_NAMES, classify_specialite

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
OUTPUT_MD = os.path.join(OUTPUT_DIR, "prompt20_top500_prospects.md")
OUTPUT_CSV_TOP500 = os.path.join(OUTPUT_DIR, "prompt20_top500.csv")
OUTPUT_CSV_TOP100 = os.path.join(OUTPUT_DIR, "prompt20_top100.csv")

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

PRIMARY_REGIONS = {"Île-de-France", "Auvergne-Rhône-Alpes", "Provence-Alpes-Côte d'Azur"}
SECONDARY_REGIONS = {"Occitanie", "Nouvelle-Aquitaine", "Grand Est"}

PRIORITY_LABELS = [
    (90, 100, "🔴", "Très haute priorité"),
    (75, 89, "🟠", "Haute priorité"),
    (60, 74, "🟡", "Priorité moyenne"),
    (45, 59, "🟢", "Priorité basse"),
    (0, 44, "⚪", "Hors cible"),
]


@dataclass
class ProspectRecord:
    numero: str
    denomination: str
    siren: str
    siret: str
    ville: Optional[str]
    code_postal: Optional[str]
    region_code: Optional[int]
    effectif: Optional[int]
    nb_stagiaires: float
    actions_cert: Optional[int]
    specialites: Tuple[Optional[str], Optional[str], Optional[str]]

    @property
    def region_name(self) -> str:
        if self.region_code is None:
            return "Autres DOM-TOM"
        return REGION_NAMES.get(self.region_code, "Autres DOM-TOM")

    @property
    def production_estimee(self) -> Optional[float]:
        if self.effectif is None or self.nb_stagiaires is None:
            return None
        stag_mois = self.nb_stagiaires / 12.0
        return (stag_mois / 20.0) + (self.effectif * 2.0)

    @property
    def specialite_principale(self) -> Optional[str]:
        for label in self.specialites:
            if label:
                return label
        return None

    @property
    def specialite_count(self) -> int:
        return sum(1 for label in self.specialites if label)


@dataclass
class ProspectScore:
    record: ProspectRecord
    score_effectif: int
    score_soft: int
    score_activite: int
    score_region: int
    score_multi: int

    @property
    def score_total(self) -> float:
        # Apply explicit weighting formula to keep traceability with requirements
        components = [
            (self.score_effectif, 25, 0.25),
            (self.score_soft, 25, 0.25),
            (self.score_activite, 20, 0.20),
            (self.score_region, 15, 0.15),
            (self.score_multi, 15, 0.15),
        ]
        weighted = sum((score / maximum) * weight for score, maximum, weight in components)
        return weighted * 100.0

    @property
    def priority_label(self) -> Tuple[str, str]:
        score = self.score_total
        for minimum, maximum, emoji, label in PRIORITY_LABELS:
            if minimum <= score <= maximum:
                return emoji, label
        # Fallback (should not occur)
        return "⚪", "Hors cible"


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


def get_cell_value(cell: ET.Element, shared_strings: Sequence[str]) -> Optional[str]:
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
        return int(float(text))
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
    if length and result.isdigit():
        if len(result) > length:
            result = result[:length]
        elif len(result) < length:
            result = result.zfill(length)
    return result


def normalize_postal_code(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if digits:
        if len(digits) >= 5:
            digits = digits[:5]
        return digits
    return text


def load_records() -> List[ProspectRecord]:
    records: List[ProspectRecord] = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            header_map: Dict[int, str] = {}
            target_indices: Dict[str, Optional[int]] = {}
            for event, elem in ET.iterparse(f, events=("end",)):
                if elem.tag != NS + "row":
                    continue
                row_idx = int(elem.attrib.get("r"))
                if row_idx == 1:
                    for cell in elem.findall(NS + "c"):
                        ref = cell.attrib.get("r")
                        if not ref:
                            continue
                        idx = column_ref_to_index(ref)
                        val = get_cell_value(cell, shared_strings)
                        if val is not None:
                            header_map[idx] = val
                    lookup = {
                        "numero": "numeroDeclarationActivite",
                        "denomination": "denomination",
                        "siren": "siren",
                        "siret": "siretEtablissementDeclarant",
                        "voie": "adressePhysiqueOrganismeFormation.voie",
                        "cp": "adressePhysiqueOrganismeFormation.codePostal",
                        "ville": "adressePhysiqueOrganismeFormation.ville",
                        "region": "adressePhysiqueOrganismeFormation.codeRegion",
                        "actions": "certifications.actionsDeFormation",
                        "stagiaires": "informationsDeclarees.nbStagiaires",
                        "effectif": "informationsDeclarees.effectifFormateurs",
                        "spe1": "informationsDeclarees.specialitesDeFormation.libelleSpecialite1",
                        "spe2": "informationsDeclarees.specialitesDeFormation.libelleSpecialite2",
                        "spe3": "informationsDeclarees.specialitesDeFormation.libelleSpecialite3",
                    }
                    for key, header_name in lookup.items():
                        target_indices[key] = next(
                            (idx for idx, name in header_map.items() if name == header_name),
                            None,
                        )
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
                    idx = column_ref_to_index(ref)
                    if idx not in indices:
                        continue
                    val = get_cell_value(cell, shared_strings)
                    if val is not None:
                        values[idx] = val

                numero = values.get(target_indices["numero"], "") if target_indices["numero"] is not None else ""
                denomination = values.get(target_indices["denomination"], "") if target_indices["denomination"] is not None else ""
                siren = parse_identifier(values.get(target_indices["siren"]) if target_indices["siren"] is not None else None, length=9)
                siret = parse_identifier(values.get(target_indices["siret"]) if target_indices["siret"] is not None else None, length=14)
                ville = values.get(target_indices["ville"]) if target_indices["ville"] is not None else None
                if ville:
                    ville = ville.strip() or None
                code_postal_raw = values.get(target_indices["cp"]) if target_indices["cp"] is not None else None
                code_postal = normalize_postal_code(code_postal_raw)
                region_code = parse_int(values.get(target_indices["region"])) if target_indices["region"] is not None else None
                actions = parse_int(values.get(target_indices["actions"])) if target_indices["actions"] is not None else None
                nb_stagiaires = parse_float(values.get(target_indices["stagiaires"])) if target_indices["stagiaires"] is not None else None
                effectif = parse_int(values.get(target_indices["effectif"])) if target_indices["effectif"] is not None else None

                spe_values: List[Optional[str]] = []
                for key in ("spe1", "spe2", "spe3"):
                    idx = target_indices.get(key)
                    if idx is None:
                        spe_values.append(None)
                        continue
                    raw = values.get(idx)
                    if raw is None:
                        spe_values.append(None)
                        continue
                    text = raw.strip()
                    spe_values.append(text if text else None)

                record = ProspectRecord(
                    numero=numero.strip(),
                    denomination=denomination.strip(),
                    siren=siren,
                    siret=siret,
                    ville=ville,
                    code_postal=code_postal,
                    region_code=region_code,
                    effectif=effectif,
                    nb_stagiaires=nb_stagiaires or 0.0,
                    actions_cert=actions,
                    specialites=tuple(spe_values),
                )
                records.append(record)
                elem.clear()
    return records


def is_tam(record: ProspectRecord) -> bool:
    if record.effectif is None or record.effectif < 3 or record.effectif > 10:
        return False
    if record.actions_cert != 1:
        return False
    if record.nb_stagiaires is None or record.nb_stagiaires <= 0:
        return False
    return True


def score_effectif(record: ProspectRecord) -> int:
    effectif = record.effectif
    if effectif is None:
        return 0
    if effectif in (4, 5):
        return 25
    if effectif in (3, 6, 7):
        return 15
    if 8 <= effectif <= 10:
        return 10
    return 0


def score_soft_skills(record: ProspectRecord) -> int:
    themes = [classify_specialite(label) for label in record.specialites if label]
    if not themes:
        return 0
    if themes[0] == "Soft Skills":
        return 25
    if "Soft Skills" in themes[1:]:
        return 15
    return 0


def score_activite(record: ProspectRecord) -> int:
    stagiaires = record.nb_stagiaires or 0.0
    if stagiaires >= 500:
        return 20
    if stagiaires >= 200:
        return 15
    if stagiaires >= 100:
        return 10
    if stagiaires >= 50:
        return 5
    return 2


def score_region(record: ProspectRecord) -> int:
    region = record.region_name
    if region in PRIMARY_REGIONS:
        return 15
    if region in SECONDARY_REGIONS:
        return 10
    return 5


def score_multi_specialites(record: ProspectRecord) -> int:
    count = record.specialite_count
    if count >= 3:
        return 15
    if count == 2:
        return 10
    if count == 1:
        return 5
    return 0


def compute_scores(records: Iterable[ProspectRecord]) -> List[ProspectScore]:
    scores: List[ProspectScore] = []
    for rec in records:
        scores.append(
            ProspectScore(
                record=rec,
                score_effectif=score_effectif(rec),
                score_soft=score_soft_skills(rec),
                score_activite=score_activite(rec),
                score_region=score_region(rec),
                score_multi=score_multi_specialites(rec),
            )
        )
    return scores


def format_int(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def format_float(value: float, decimals: int = 1) -> str:
    return f"{value:,.{decimals}f}".replace(",", " ")


def format_percent(value: float, decimals: int = 1) -> str:
    return f"{value:.{decimals}f}%"


def safe_mean(values: Iterable[float]) -> float:
    data = [v for v in values if v is not None]
    if not data:
        return 0.0
    return sum(data) / len(data)


def determine_priority(score: float) -> Tuple[str, str]:
    for minimum, maximum, emoji, label in PRIORITY_LABELS:
        if minimum <= score <= maximum:
            return emoji, label
    return "⚪", "Hors cible"


def distribution_table(scores: Sequence[ProspectScore]) -> List[Tuple[str, str, str, str]]:
    total = len(scores)
    rows: List[Tuple[str, str, str, str]] = []
    for minimum, maximum, emoji, label in PRIORITY_LABELS:
        count = sum(1 for sc in scores if minimum <= sc.score_total <= maximum)
        pct = (count / total * 100) if total else 0.0
        interpretation = {
            "Très haute priorité": "Pipeline immédiat",
            "Haute priorité": "Ciblage M1-M2",
            "Priorité moyenne": "Ciblage M3-M6",
            "Priorité basse": "Opportuniste",
            "Hors cible": "Ignorer",
        }[label]
        rows.append(
            (
                f"{emoji} {label}",
                f"{minimum}-{maximum}",
                format_int(count),
                f"{pct:.1f}%",
                interpretation,
            )
        )
    return rows


def priority_pipeline(scores: Sequence[ProspectScore]) -> Dict[str, Dict[str, object]]:
    mapping = {
        "🔴": {"label": "Très haute", "actions": "Contact direct + Démo", "timeline": "M1", "range": "90-100"},
        "🟠": {"label": "Haute", "actions": "Outreach LinkedIn", "timeline": "M1-M2", "range": "75-89"},
        "🟡": {"label": "Moyenne", "actions": "Nurturing contenu", "timeline": "M3-M6", "range": "60-74"},
        "🟢": {"label": "Basse", "actions": "Base données", "timeline": "M6+", "range": "45-59"},
    }
    result: Dict[str, Dict[str, object]] = {}
    for emoji, info in mapping.items():
        count = sum(1 for sc in scores if determine_priority(sc.score_total)[0] == emoji)
        result[emoji] = {"count": count, **info}
    return result


def segmentation_metrics(scores: Sequence[ProspectScore]) -> Dict[str, float]:
    score_values = [sc.score_total for sc in scores]
    effectifs = [sc.record.effectif or 0 for sc in scores]
    stagiaires = [sc.record.nb_stagiaires or 0.0 for sc in scores]
    soft_share = (
        sum(1 for sc in scores if score_soft_skills(sc.record) >= 15) / len(scores) * 100
        if scores
        else 0.0
    )
    region_share = (
        sum(1 for sc in scores if sc.record.region_name in PRIMARY_REGIONS) / len(scores) * 100
        if scores
        else 0.0
    )
    production = [sc.record.production_estimee or 0.0 for sc in scores]
    return {
        "score_mean": statistics.mean(score_values) if score_values else 0.0,
        "effectif_mean": statistics.mean(effectifs) if effectifs else 0.0,
        "stagiaires_mean": statistics.mean(stagiaires) if stagiaires else 0.0,
        "soft_pct": soft_share,
        "region_pct": region_share,
        "production_mean": statistics.mean(production) if production else 0.0,
    }


def compare_segments(top_metrics: Dict[str, float], tam_metrics: Dict[str, float]) -> List[Tuple[str, str, str, str]]:
    rows: List[Tuple[str, str, str, str]] = []
    entries = [
        ("Score moyen", "score_mean", "pts"),
        ("Effectif moyen", "effectif_mean", " form"),
        ("Stagiaires moyen", "stagiaires_mean", " /an"),
        ("% Soft skills", "soft_pct", " pp"),
        ("% IDF/AURA/PACA", "region_pct", " pp"),
        ("Production est.", "production_mean", " livr/mois"),
    ]
    for label, key, suffix in entries:
        top_val = top_metrics.get(key, 0.0)
        tam_val = tam_metrics.get(key, 0.0)
        diff = top_val - tam_val
        if key in {"soft_pct", "region_pct"}:
            top_display = f"{top_val:.1f}%"
            tam_display = f"{tam_val:.1f}%"
            diff_display = f"{diff:+.1f} pp"
        elif key == "score_mean":
            top_display = f"{top_val:.1f}"
            tam_display = f"{tam_val:.1f}"
            diff_display = f"{diff:+.1f} pts"
        elif key == "effectif_mean":
            top_display = f"{top_val:.2f}"
            tam_display = f"{tam_val:.2f}"
            diff_display = f"{diff:+.2f} form"
        elif key == "production_mean":
            top_display = f"{top_val:.1f}"
            tam_display = f"{tam_val:.1f}"
            diff_display = f"{diff:+.1f} livr/mois"
        else:
            top_display = f"{top_val:.0f}"
            tam_display = f"{tam_val:.0f}"
            diff_display = f"{diff:+.0f}{suffix.strip()}"
        rows.append((label, top_display, tam_display, diff_display))
    return rows


def region_distribution(scores: Sequence[ProspectScore], tam_scores: Sequence[ProspectScore]) -> List[Tuple[str, str, str, str]]:
    region_counts: Dict[str, int] = {}
    tam_counts: Dict[str, int] = {}
    for sc in scores:
        region_counts[sc.record.region_name] = region_counts.get(sc.record.region_name, 0) + 1
    for sc in tam_scores:
        tam_counts[sc.record.region_name] = tam_counts.get(sc.record.region_name, 0) + 1

    total_top = sum(region_counts.values())
    total_tam = sum(tam_counts.values())
    all_regions = sorted({*region_counts.keys(), *tam_counts.keys()})
    rows: List[Tuple[str, str, str, str]] = []
    for region in all_regions:
        top_pct = (region_counts.get(region, 0) / total_top * 100) if total_top else 0.0
        tam_pct = (tam_counts.get(region, 0) / total_tam * 100) if total_tam else 0.0
        diff = top_pct - tam_pct
        if diff >= 3:
            opportunity = "Élevée"
        elif diff <= -3:
            opportunity = "Faible"
        else:
            opportunity = "Moy"
        rows.append(
            (
                region,
                f"{top_pct:.1f}%",
                f"{tam_pct:.1f}%",
                opportunity,
            )
        )
    rows.sort(key=lambda row: float(row[1].rstrip('%')), reverse=True)
    return rows


def enrichment_tasks() -> List[Tuple[str, str, str, str]]:
    tasks = [
        ("1. Recherche LinkedIn entreprise", "LinkedIn", "2 min", "⏳"),
        ("2. Identification décideur", "LinkedIn Sales Nav", "3 min", "⏳"),
        ("3. Email professionnel", "Hunter.io / LinkedIn", "2 min", "⏳"),
        ("4. Veille activité (posts récents)", "LinkedIn", "2 min", "⏳"),
        ("5. Événement déclencheur", "Google News / LinkedIn", "2 min", "⏳"),
    ]
    return tasks


def markdown_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    if not headers:
        return ""
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def build_markdown(
    tam_scores: Sequence[ProspectScore],
    top_scores: Sequence[ProspectScore],
    distribution_rows: Sequence[Tuple[str, str, str, str]],
    comparison_rows: Sequence[Tuple[str, str, str, str]],
    region_rows: Sequence[Tuple[str, str, str, str]],
    pipeline_info: Dict[str, Dict[str, object]],
) -> str:
    total_tam = len(tam_scores)
    total_top = len(top_scores)
    red_count = sum(1 for sc in tam_scores if sc.score_total >= 90)
    orange_count = sum(1 for sc in tam_scores if 75 <= sc.score_total <= 89)
    yellow_count = sum(1 for sc in tam_scores if 60 <= sc.score_total <= 74)

    summary_lines = [
        "# PROMPT 20 — Top 500 prospects scorés",
        "",
        "## Synthèse",
        f"- Base TAM qualifiée : **{format_int(total_tam)}** organismes",
        f"- Prospects priorisés (Top 500) : **{format_int(total_top)}**",
        f"- Distribution scoring TAM : 🔴 {format_int(red_count)} | 🟠 {format_int(orange_count)} | 🟡 {format_int(yellow_count)}",
        "",
        "## Analyse 1 — Distribution des scores TAM",
        markdown_table(["Priorité", "Score", "OF", "% TAM", "Interprétation"], distribution_rows),
        "",
        "## Analyse 2 — Profil Top 500 vs TAM",
        markdown_table(["Métrique", "Top 500", "TAM général", "Enrichissement"], comparison_rows),
        "",
        "## Analyse 3 — Top 500 prospects prioritaires",
        "Consulter `analysis_outputs/prompt20_top500.csv` pour l'export complet (500 lignes).",
        "",
        "## Analyse 4 — Répartition géographique (Top 500)",
        markdown_table(["Région", "% Top 500", "% TAM", "Opportunité"], region_rows),
        "",
        "## Analyse 5 — Pipeline par priorité",
    ]

    pipeline_rows: List[List[str]] = []
    for emoji in ["🔴", "🟠", "🟡", "🟢"]:
        info = pipeline_info.get(emoji, {})
        pipeline_rows.append(
            [
                f"{emoji} {info.get('label', '')}",
                info.get("range", ""),
                format_int(info.get("count", 0)),
                info.get("actions", ""),
                info.get("timeline", ""),
            ]
        )
    summary_lines.append(markdown_table(["Priorité", "Score", "OF", "Actions", "Timeline"], pipeline_rows))
    summary_lines.extend(
        [
            "",
            "## Analyse 6 — Checklist enrichissement",
            markdown_table(["Tâche", "Outil", "Temps", "Statut"], enrichment_tasks()),
            "",
            "## Recommandations opérationnelles",
            "1. Export CSV Top 500 (livrable présent).",
            "2. Enrichir en priorité le Top 100 (coordonnées commerciales).",
            "3. Segmenter par région pour planifier les tournées.",
            "4. Lancer des séquences d'outreach personnalisées par priorité.",
        ]
    )
    return "\n".join(summary_lines)


def export_csv(scores: Sequence[ProspectScore], path: str, limit: Optional[int] = None) -> None:
    ensure_output_dir()
    rows = []
    for rank, sc in enumerate(scores, start=1):
        if limit is not None and rank > limit:
            break
        emoji, label = sc.priority_label
        record = sc.record
        production = sc.record.production_estimee
        rows.append(
            {
                "Rang": rank,
                "Dénomination": record.denomination,
                "SIREN": record.siren,
                "Ville": record.ville or "",
                "CP": record.code_postal or "",
                "Région": record.region_name,
                "Effectif": record.effectif or 0,
                "Stagiaires": int(round(record.nb_stagiaires)),
                "Spécialité": record.specialite_principale or "Non renseigné",
                "Score": round(sc.score_total, 1),
                "Priorité": emoji,
                "Production_est": round(production, 1) if production is not None else "",
                "Email": "",
                "LinkedIn": "",
                "Statut": "",
                "Notes": "",
            }
        )
    fieldnames = [
        "Rang",
        "Dénomination",
        "SIREN",
        "Ville",
        "CP",
        "Région",
        "Effectif",
        "Stagiaires",
        "Spécialité",
        "Score",
        "Priorité",
        "Production_est",
        "Email",
        "LinkedIn",
        "Statut",
        "Notes",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    ensure_output_dir()
    records = load_records()
    tam_records = [rec for rec in records if is_tam(rec)]
    tam_scores = compute_scores(tam_records)
    tam_scores.sort(key=lambda sc: (-sc.score_total, -(sc.record.nb_stagiaires or 0), -(sc.record.production_estimee or 0)))

    top_scores = tam_scores[:500]

    distribution_rows = distribution_table(tam_scores)
    tam_metrics = segmentation_metrics(tam_scores)
    top_metrics = segmentation_metrics(top_scores)
    comparison_rows = compare_segments(top_metrics, tam_metrics)
    region_rows = region_distribution(top_scores, tam_scores)
    pipeline_info = priority_pipeline(tam_scores)

    export_csv(top_scores, OUTPUT_CSV_TOP500)
    export_csv(top_scores, OUTPUT_CSV_TOP100, limit=100)

    markdown_content = build_markdown(
        tam_scores=tam_scores,
        top_scores=top_scores,
        distribution_rows=distribution_rows,
        comparison_rows=comparison_rows,
        region_rows=region_rows,
        pipeline_info=pipeline_info,
    )
    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write(markdown_content)


if __name__ == "__main__":
    main()

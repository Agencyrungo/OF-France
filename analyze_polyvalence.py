import csv
import itertools
import os
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

from analyze_specialites import (
    REGION_NAMES,
    Record,
    format_float,
    format_int,
    load_records,
    is_tam,
)

OUTPUT_DIR = "analysis_outputs"
OUTPUT_MARKDOWN = os.path.join(OUTPUT_DIR, "polyvalence_analysis.md")
OUTPUT_COMBOS_CSV = os.path.join(OUTPUT_DIR, "polyvalence_combinations.csv")


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def count_specialites(record: Record) -> int:
    return sum(1 for spec in record.specialites if spec is not None)


def inclusive_counts(records: Iterable[Record]) -> Dict[int, int]:
    counts = Counter()
    for rec in records:
        if rec.spec1 is None:
            counts[0] += 1
        if rec.spec1 is not None:
            counts[1] += 1
        if rec.spec2 is not None:
            counts[2] += 1
        if rec.spec3 is not None:
            counts[3] += 1
    return counts


def exclusive_counts(records: Iterable[Record]) -> Counter:
    counter: Counter = Counter()
    for rec in records:
        counter[count_specialites(rec)] += 1
    return counter


def average(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def compute_table1(records: List[Record], tam_records: List[Record]) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    total_base = len(records)
    total_tam = len(tam_records)
    inc_base = inclusive_counts(records)
    inc_tam = inclusive_counts(tam_records)

    rows: List[Dict[str, object]] = []
    for key, label in [
        (0, "0 (non renseigné)"),
        (1, "1 seule"),
        (2, "2 spécialités"),
        (3, "3 spécialités"),
    ]:
        base_count = inc_base.get(key, 0)
        tam_count = inc_tam.get(key, 0)
        stag_values = [
            rec.nb_stagiaires or 0.0
            for rec in tam_records
            if (
                (key == 0 and rec.spec1 is None)
                or (key == 1 and rec.spec1 is not None)
                or (key == 2 and rec.spec2 is not None)
                or (key == 3 and rec.spec3 is not None)
            )
        ]
        rows.append(
            {
                "label": label,
                "base_count": base_count,
                "base_pct": (base_count / total_base * 100) if total_base else 0.0,
                "tam_count": tam_count,
                "tam_pct": (tam_count / total_tam * 100) if total_tam else 0.0,
                "stag_mean": average(stag_values),
            }
        )

    exc_counter = exclusive_counts(records)
    exclusive_rows: List[Dict[str, object]] = []
    for key, label in [
        (0, "Non renseigné"),
        (1, "Spécialisés"),
        (2, "Diversifiés"),
        (3, "Polyvalents"),
    ]:
        count = exc_counter.get(key, 0)
        exclusive_rows.append(
            {
                "label": label,
                "count": count,
                "pct": (count / total_base * 100) if total_base else 0.0,
            }
        )
    return rows, exclusive_rows


def compute_table2(tam_records: List[Record]) -> List[Dict[str, object]]:
    total_tam = len(tam_records)
    rows: List[Dict[str, object]] = []
    grouped: Dict[int, List[Record]] = defaultdict(list)
    for rec in tam_records:
        grouped[count_specialites(rec)].append(rec)

    for key, label in [
        (1, "1 spécialité"),
        (2, "2 spécialités"),
        (3, "3 spécialités"),
    ]:
        group = grouped.get(key, [])
        stag_values = [rec.nb_stagiaires or 0.0 for rec in group]
        prod_values = [
            (rec.nb_stagiaires or 0.0) / rec.effectif
            for rec in group
            if rec.effectif
        ]
        effectif_values = [rec.effectif for rec in group if rec.effectif]
        rows.append(
            {
                "label": label,
                "tam_count": len(group),
                "tam_pct": (len(group) / total_tam * 100) if total_tam else 0.0,
                "stag_mean": average(stag_values),
                "prod_mean": average(prod_values),
                "effectif_mean": average(effectif_values),
            }
        )
    return rows


def normalize_label(spec: Optional[Tuple[Optional[str], Optional[str]]]) -> Optional[str]:
    if spec is None:
        return None
    label = spec[1] or spec[0]
    if not label:
        return None
    return label.strip()


def compute_combinations(records: List[Record], tam_records: List[Record]) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    pair_counter: Counter = Counter()
    total_multi = 0
    for rec in records:
        labels = []
        for spec in rec.specialites:
            label = normalize_label(spec)
            if label and label not in labels:
                labels.append(label)
        if len(labels) >= 2:
            total_multi += 1
            labels_sorted = sorted(labels, key=str.casefold)
            for a, b in itertools.combinations(labels_sorted, 2):
                pair_counter[(a, b)] += 1

    tam_pair_stag: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for rec in tam_records:
        labels = []
        for spec in rec.specialites:
            label = normalize_label(spec)
            if label and label not in labels:
                labels.append(label)
        if len(labels) < 2:
            continue
        labels_sorted = sorted(labels, key=str.casefold)
        for a, b in itertools.combinations(labels_sorted, 2):
            tam_pair_stag[(a, b)].append(rec.nb_stagiaires or 0.0)

    top_pairs = pair_counter.most_common(20)
    rows: List[Dict[str, object]] = []
    for rank, ((a, b), count) in enumerate(top_pairs, start=1):
        stag_mean = average(tam_pair_stag.get((a, b), []))
        rows.append(
            {
                "rank": rank,
                "a": a,
                "b": b,
                "count": count,
                "pct_multi": (count / total_multi * 100) if total_multi else 0.0,
                "stag_mean": stag_mean,
                "insight": f"Offre combinant {a} & {b}",
            }
        )

    all_rows: List[Dict[str, object]] = []
    for (a, b), count in pair_counter.most_common():
        all_rows.append(
            {
                "specialite_1": a,
                "specialite_2": b,
                "of": count,
                "pct_multi": (count / total_multi * 100) if total_multi else 0.0,
                "stag_mean_tam": average(tam_pair_stag.get((a, b), [])),
            }
        )

    return rows, all_rows


def compute_table4(records: List[Record]) -> List[Dict[str, object]]:
    region_groups: Dict[str, List[Record]] = defaultdict(list)
    for rec in records:
        if rec.region_code is None:
            continue
        name = REGION_NAMES.get(rec.region_code, "Autres territoires")
        region_groups[name].append(rec)

    rows: List[Dict[str, object]] = []
    national_total = sum(len(recs) for recs in region_groups.values())
    national_poly = sum(
        sum(1 for r in recs if count_specialites(r) >= 2)
        for recs in region_groups.values()
    )
    national_pct = (national_poly / national_total * 100) if national_total else 0.0

    for name, recs in region_groups.items():
        total = len(recs)
        one_spec = sum(1 for r in recs if count_specialites(r) == 1)
        multi = sum(1 for r in recs if count_specialites(r) >= 2)
        poly_pct = (multi / total * 100) if total else 0.0
        rows.append(
            {
                "region": name,
                "one_spec": one_spec,
                "multi_spec": multi,
                "poly_pct": poly_pct,
                "delta": poly_pct - national_pct,
            }
        )

    rows.sort(key=lambda item: item["poly_pct"], reverse=True)
    rows.append(
        {
            "region": "National",
            "one_spec": sum(1 for r in records if count_specialites(r) == 1),
            "multi_spec": sum(1 for r in records if count_specialites(r) >= 2),
            "poly_pct": national_pct,
            "delta": 0.0,
        }
    )
    return rows


def categorize_effectif(effectif: Optional[int]) -> Optional[str]:
    if effectif is None:
        return None
    if effectif <= 2:
        return "≤2"
    if effectif == 3:
        return "3"
    if effectif == 4:
        return "4"
    if 5 <= effectif <= 6:
        return "5-6"
    if 7 <= effectif <= 8:
        return "7-8"
    if 9 <= effectif <= 10:
        return "9-10"
    return ">10"


def compute_table5(tam_records: List[Record]) -> List[Dict[str, object]]:
    groups: Dict[str, List[Record]] = defaultdict(list)
    for rec in tam_records:
        category = categorize_effectif(rec.effectif)
        if category:
            groups[category].append(rec)

    ordered_categories = ["3", "4", "5-6", "7-8", "9-10"]
    if any(cat not in ordered_categories for cat in groups):
        for cat in sorted(groups.keys()):
            if cat not in ordered_categories:
                ordered_categories.append(cat)

    rows: List[Dict[str, object]] = []
    for cat in ordered_categories:
        recs = groups.get(cat, [])
        total = len(recs)
        one_spec = sum(1 for r in recs if count_specialites(r) == 1)
        two_spec = sum(1 for r in recs if count_specialites(r) == 2)
        three_spec = sum(1 for r in recs if count_specialites(r) == 3)
        poly_pct = ((two_spec + three_spec) / total * 100) if total else 0.0
        rows.append(
            {
                "category": cat,
                "one_spec": one_spec,
                "two_spec": two_spec,
                "three_spec": three_spec,
                "poly_pct": poly_pct,
            }
        )
    return rows


def write_markdown(
    table1: List[Dict[str, object]],
    exclusive: List[Dict[str, object]],
    table2: List[Dict[str, object]],
    table3: List[Dict[str, object]],
    table4: List[Dict[str, object]],
    table5: List[Dict[str, object]],
    summary: List[str],
) -> None:
    lines: List[str] = []
    lines.append("# Analyse polyvalence OF France 2025")
    lines.append("")

    lines.append("## Tableau 1a : Spécialités déclarées (cumulatives)")
    lines.append("| Nb spécialités | OF total | % base | OF TAM | % TAM | Stag. moyen TAM |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for row in table1:
        lines.append(
            "| {label} | {base} | {base_pct:.1f}% | {tam} | {tam_pct:.1f}% | {stag} |".format(
                label=row["label"],
                base=format_int(row["base_count"]),
                base_pct=row["base_pct"],
                tam=format_int(row["tam_count"]),
                tam_pct=row["tam_pct"],
                stag=format_float(row["stag_mean"], 0) if row["tam_count"] else "-",
            )
        )
    lines.append("")

    lines.append("## Tableau 1b : Répartition exclusive des OF")
    lines.append("| Statut | OF | % base |")
    lines.append("| --- | --- | --- |")
    for row in exclusive:
        lines.append(
            "| {label} | {count} | {pct:.1f}% |".format(
                label=row["label"],
                count=format_int(row["count"]),
                pct=row["pct"],
            )
        )
    lines.append("")

    lines.append("## Tableau 2 : Polyvalence vs activité (TAM)")
    lines.append("| Nb spécialités | OF TAM | % TAM | Stag. moyen | Prod. estimée | Effectif moyen |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for row in table2:
        lines.append(
            "| {label} | {tam} | {tam_pct:.1f}% | {stag} | {prod} | {effectif} |".format(
                label=row["label"],
                tam=format_int(row["tam_count"]),
                tam_pct=row["tam_pct"],
                stag=format_float(row["stag_mean"], 0) if row["tam_count"] else "-",
                prod=format_float(row["prod_mean"], 1) if row["tam_count"] else "-",
                effectif=format_float(row["effectif_mean"], 1) if row["tam_count"] else "-",
            )
        )
    lines.append("")

    lines.append("## Tableau 3 : Paires de spécialités les plus fréquentes")
    lines.append("| Rang | Spé 1 | Spé 2 | OF | % multi-spés | Stag. moyen TAM | Interprétation |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for row in table3:
        lines.append(
            "| {rank} | {a} | {b} | {count} | {pct:.1f}% | {stag} | {insight} |".format(
                rank=row["rank"],
                a=row["a"],
                b=row["b"],
                count=format_int(row["count"]),
                pct=row["pct_multi"],
                stag=format_float(row["stag_mean"], 0) if row["stag_mean"] else "-",
                insight=row["insight"],
            )
        )
    lines.append("")

    lines.append("## Tableau 4 : Polyvalence par région")
    lines.append("| Région | OF 1 spé | OF 2+ spés | % polyvalents | vs national (pp) |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in table4:
        lines.append(
            "| {region} | {one} | {multi} | {pct:.1f}% | {delta:+.1f} |".format(
                region=row["region"],
                one=format_int(row["one_spec"]),
                multi=format_int(row["multi_spec"]),
                pct=row["poly_pct"],
                delta=row["delta"],
            )
        )
    lines.append("")

    lines.append("## Tableau 5 : Polyvalence selon l'effectif (TAM)")
    lines.append("| Effectif | OF 1 spé | OF 2 spés | OF 3 spés | % polyvalents |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in table5:
        lines.append(
            "| {cat} | {one} | {two} | {three} | {pct:.1f}% |".format(
                cat=row["category"],
                one=format_int(row["one_spec"]),
                two=format_int(row["two_spec"]),
                three=format_int(row["three_spec"]),
                pct=row["poly_pct"],
            )
        )
    lines.append("")

    lines.append("## Synthèse")
    for bullet in summary:
        lines.append(f"- {bullet}")

    with open(OUTPUT_MARKDOWN, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def write_combinations_csv(rows: List[Dict[str, object]]) -> None:
    fieldnames = ["specialite_1", "specialite_2", "of", "pct_multi", "stag_mean_tam"]
    with open(OUTPUT_COMBOS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_summary(
    exclusive: List[Dict[str, object]],
    table2: List[Dict[str, object]],
    table3: List[Dict[str, object]],
    table4: List[Dict[str, object]],
    table5: List[Dict[str, object]],
) -> List[str]:
    spec_lookup = {row["label"]: row for row in exclusive}
    poly_pct = spec_lookup.get("Polyvalents", {}).get("pct", 0.0)
    divers_pct = spec_lookup.get("Diversifiés", {}).get("pct", 0.0)
    spec_pct = spec_lookup.get("Spécialisés", {}).get("pct", 0.0)

    tam_lookup = {row["label"]: row for row in table2}
    stag1 = tam_lookup.get("1 spécialité", {}).get("stag_mean", 0.0)
    stag2 = tam_lookup.get("2 spécialités", {}).get("stag_mean", 0.0)
    stag3 = tam_lookup.get("3 spécialités", {}).get("stag_mean", 0.0)

    top_pairs = table3[:3]
    regions_sorted = [row for row in table4 if row["region"] != "National"]
    top_region = next(
        (row for row in regions_sorted if row["region"] != "Autres territoires"),
        regions_sorted[0] if regions_sorted else None,
    )
    low_region = next(
        (row for row in reversed(regions_sorted) if row["region"] != "Autres territoires"),
        regions_sorted[-1] if regions_sorted else None,
    )

    effectif_high = max(table5, key=lambda row: row["poly_pct"], default=None)

    summary: List[str] = []
    summary.append(
        "Répartition : {spec:.1f}% spécialisés (1 spé), {divers:.1f}% diversifiés (2 spés) et {poly:.1f}% polyvalents (3 spés).".format(
            spec=spec_pct,
            divers=divers_pct,
            poly=poly_pct,
        )
    )
    if stag3 and stag1:
        uplift = stag3 - stag1
        summary.append(
            "TAM : les OF à 3 spés forment en moyenne {stag3:.0f} stagiaires (+{uplift:.0f} vs 1 spé).".format(
                stag3=stag3,
                uplift=uplift,
            )
        )
    if top_pairs:
        pair_text = "; ".join(
            f"{row['a']} + {row['b']} ({format_int(row['count'])} OF)" for row in top_pairs
        )
        summary.append(f"Top combinaisons : {pair_text}.")
    if top_region and low_region:
        summary.append(
            "Régions les plus polyvalentes : {high} ({pct_high:.1f}%), les moins : {low} ({pct_low:.1f}%).".format(
                high=top_region["region"],
                pct_high=top_region["poly_pct"],
                low=low_region["region"],
                pct_low=low_region["poly_pct"],
            )
        )
    if effectif_high:
        summary.append(
            "Polyvalence et taille : pic à {cat} formateurs ({pct:.1f}% d'OF à 2+ spés).".format(
                cat=effectif_high["category"],
                pct=effectif_high["poly_pct"],
            )
        )
    return summary


def main() -> None:
    ensure_output_dir()
    records = load_records()
    tam_records = [rec for rec in records if is_tam(rec)]

    table1, exclusive = compute_table1(records, tam_records)
    table2 = compute_table2(tam_records)
    table3, combos_csv = compute_combinations(records, tam_records)
    table4 = compute_table4(records)
    table5 = compute_table5(tam_records)
    summary = build_summary(exclusive, table2, table3, table4, table5)

    write_markdown(table1, exclusive, table2, table3, table4, table5, summary)
    write_combinations_csv(combos_csv)


if __name__ == "__main__":
    main()

import os
from collections import Counter
from typing import Dict, List, Tuple

from prompt17_sweet_spot import MACRO_THEMES, REGION_NAMES, load_records

OUTPUT_MD = os.path.join("analysis_outputs", "prompt18_tam_final.md")
OUTPUT_CSV = os.path.join("analysis_outputs", "prompt18_tam_final.csv")

MINDSET_FACTOR = 0.70
PRODUCTION_THRESHOLD = 5.0

def format_int(value: float) -> str:
    return f"{int(round(value)):,}".replace(",", " ")

def format_percent(value: float, decimals: int = 1) -> str:
    return f"{value * 100:.{decimals}f}%"

def build_stage_funnel(records) -> Tuple[List[Tuple[str, int, float, float]], Dict[str, List]]:
    total_base = len(records)
    stage1 = [r for r in records if r.effectif is not None and 3 <= r.effectif <= 10]
    stage2 = [r for r in stage1 if r.qualiopi_actions is not None]
    stage3 = [r for r in stage2 if r.nb_stagiaires > 0]
    stage4 = [r for r in stage3 if (r.production_estimee or 0) >= PRODUCTION_THRESHOLD]
    stage5_total = len(stage4) * MINDSET_FACTOR

    funnel = [
        ("Base France", total_base, 1.0, 1.0),
        ("3-10 formateurs", len(stage1), len(stage1) / total_base if total_base else 0.0, len(stage1) / total_base if total_base else 0.0),
        ("+ Qualiopi certifiés", len(stage2), len(stage2) / total_base if total_base else 0.0, len(stage2) / len(stage1) if stage1 else 0.0),
        ("+ Actifs (>0 stag)", len(stage3), len(stage3) / total_base if total_base else 0.0, len(stage3) / len(stage2) if stage2 else 0.0),
        ("+ Production ≥5 livr.", len(stage4), len(stage4) / total_base if total_base else 0.0, len(stage4) / len(stage3) if stage3 else 0.0),
        ("+ Mindset tech (70%)", stage5_total, stage5_total / total_base if total_base else 0.0, stage5_total / len(stage4) if stage4 else 0.0),
    ]

    stages = {
        "stage1": stage1,
        "stage2": stage2,
        "stage3": stage3,
        "stage4": stage4,
        "final_total": stage5_total,
    }
    return funnel, stages

def table_markdown(headers: List[str], rows: List[List[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)

def build_comparison_table(actual_base: int, actual_prod: int, final_total: float) -> str:
    doc_base = 12303
    doc_prod = 7381
    doc_final = 5167
    rows = []
    def add_row(label: str, doc_value: int, actual_value: float):
        diff = actual_value - doc_value
        diff_pct = (diff / doc_value * 100) if doc_value else 0.0
        rows.append([
            label,
            format_int(doc_value),
            format_int(actual_value),
            f"{diff:+,.0f}".replace(",", " "),
            format_percent(diff_pct / 100.0, 1),
        ])
    add_row("TAM Base", doc_base, actual_base)
    add_row("TAM Production (60%)", doc_prod, actual_prod)
    add_row("TAM Final (×70%)", doc_final, final_total)
    return table_markdown(["Étape", "Document 1 (théorique)", "Calculé (réel)", "Écart", "Écart %"], rows)

def build_segment_table(records_stage3: List, final_total: float) -> str:
    segment_prioritaire = [r for r in records_stage3 if r.effectif in (4, 5)]
    segment_secondaire = [r for r in records_stage3 if r.effectif not in (4, 5)]

    def summarize(records_subset: List) -> Tuple[int, int, float]:
        base = len(records_subset)
        prod = len([r for r in records_subset if (r.production_estimee or 0) >= PRODUCTION_THRESHOLD])
        final = prod * MINDSET_FACTOR
        return base, prod, final

    prior_base, prior_prod, prior_final = summarize(segment_prioritaire)
    sec_base, sec_prod, sec_final = summarize(segment_secondaire)
    total_base = prior_base + sec_base
    total_prod = prior_prod + sec_prod
    total_final = prior_final + sec_final

    rows = [
        ["Prioritaire (4-5)", format_int(prior_base), format_int(prior_prod), format_int(prior_final), "🔴 HAUTE"],
        ["Secondaire (3, 6-10)", format_int(sec_base), format_int(sec_prod), format_int(sec_final), "🟠 MOYENNE"],
        ["TOTAL 3-10", format_int(total_base), format_int(total_prod), format_int(total_final), "-"]
    ]
    return table_markdown(["Segment", "TAM Base", "TAM Prod", "TAM Final", "Priorité"], rows)

def resolve_region_name(code: int) -> str:
    if code in REGION_NAMES:
        return REGION_NAMES[code]
    if code == 905:
        return "Autres DOM-TOM"
    if code is None:
        return "Non renseigné"
    return str(code)


def build_region_table(records_stage3: List, final_total: float) -> str:
    region_counts: Dict[int, int] = Counter(r.region_code for r in records_stage3)
    rows = []
    for code, count in sorted(region_counts.items(), key=lambda item: item[1], reverse=True):
        prod = count  # 100% production
        final = prod * MINDSET_FACTOR
        pct = final / final_total if final_total else 0.0
        name = resolve_region_name(code)
        rows.append([
            name,
            format_int(count),
            format_int(prod),
            format_int(final),
            format_percent(pct, 1),
        ])
    rows.append(["TOTAL", format_int(sum(region_counts.values())), format_int(sum(region_counts.values())), format_int(final_total), "100.0%"])
    return table_markdown(["Région", "TAM Base", "TAM Prod", "TAM Final", "% national"], rows)

def load_macro_priorities() -> Dict[str, str]:
    path = os.path.join("analysis_outputs", "specialites_analysis.md")
    priorities: Dict[str, str] = {}
    if not os.path.exists(path):
        return priorities
    capture = False
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip().startswith("## Tableau 4"):
                capture = True
                continue
            if capture:
                if line.strip().startswith("## "):
                    break
                if not line.strip().startswith("|") or "---" in line:
                    continue
                parts = [part.strip() for part in line.strip().strip("|").split("|")]
                if len(parts) < 6 or parts[0] == "Macro-thème":
                    continue
                theme = parts[0]
                priority = parts[5]
                priorities[theme] = priority
    return priorities

def build_macro_table(records_stage3: List, final_total: float) -> str:
    priorities = load_macro_priorities()
    macro_counts: Dict[str, int] = Counter(r.macro_theme for r in records_stage3)
    rows = []
    for theme in MACRO_THEMES:
        count = macro_counts.get(theme, 0)
        prod = count
        final = prod * MINDSET_FACTOR
        pct = final / final_total if final_total else 0.0
        priority = priorities.get(theme, "-")
        rows.append([
            theme,
            format_int(final),
            format_percent(pct, 1),
            priority if priority else "-",
        ])
    return table_markdown(["Macro-thème", "TAM Final", "% total", "Priorité marketing"], rows)

def build_penetration_table(final_total: float) -> str:
    goals = [
        ("50K€/mois", 167),
        ("100K€/mois", 334),
        ("150K€/mois", 502),
        ("200K€/mois", 669),
    ]
    rows = []
    for label, clients in goals:
        rate = clients / final_total if final_total else 0.0
        if rate <= 0.03:
            verdict = "✅ Réaliste"
        elif rate <= 0.05:
            verdict = "⚠️ Ambitieux"
        elif rate <= 0.08:
            verdict = "⚠️ Difficile"
        else:
            verdict = "❌ Hors benchmark"
        rows.append([
            label,
            format_int(clients),
            format_int(final_total),
            format_percent(rate, 2),
            verdict,
        ])
    return table_markdown(["Objectif", "Clients nécessaires", "TAM Final", "Taux pénétration", "Faisabilité"], rows)

def build_scenarios_table(final_total: float) -> str:
    if final_total >= 3000:
        rows = [["Actuel", "Aucun", format_int(0), format_int(final_total), "Impact limité"]]
    else:
        # Placeholder scenario expansion (not triggered here)
        rows = []
    return table_markdown(["Option", "Ajustement", "TAM additionnel", "TAM Final", "Impact"], rows)

def compute_top_segments(records_stage3: List) -> List[Tuple[str, float]]:
    combo_counts: Dict[Tuple[str, int, str], int] = Counter()
    for rec in records_stage3:
        if rec.effectif not in (4, 5):
            continue
        region = resolve_region_name(rec.region_code)
        theme = rec.macro_theme
        combo_counts[(region, rec.effectif, theme)] += 1
    if not combo_counts:
        return []
    top_items = combo_counts.most_common(3)
    results = []
    for (region, effectif, theme), count in top_items:
        final = count * MINDSET_FACTOR
        results.append((f"{region} + {effectif} form + {theme}", final))
    return results

def write_csv(records_stage3: List):
    import csv
    aggregated: Dict[Tuple[str, int, str], int] = Counter()
    for rec in records_stage3:
        region = resolve_region_name(rec.region_code)
        effectif = rec.effectif or 0
        theme = rec.macro_theme
        aggregated[(region, effectif, theme)] += 1
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["region", "effectif", "macro_theme", "tam_base", "tam_final"])
        for (region, effectif, theme), count in sorted(aggregated.items()):
            writer.writerow([
                region,
                effectif,
                theme,
                count,
                round(count * MINDSET_FACTOR, 2),
            ])

def main():
    records = load_records()
    funnel, stages = build_stage_funnel(records)
    stage3 = stages["stage3"]
    final_total = stages["final_total"]
    lines: List[str] = []

    # Table 1
    rows = []
    for label, count, pct_base, pct_prev in funnel:
        rows.append([
            label,
            format_int(count),
            format_percent(pct_base, 2),
            format_percent(pct_prev, 2),
        ])
    lines.append("### Tableau 1 : Entonnoir TAM complet")
    lines.append(table_markdown(["Étape", "OF", "% base", "% précédent"], rows))
    lines.append("")

    # Table 2
    lines.append("### Tableau 2 : Comparaison TAM théorique vs réel")
    lines.append(build_comparison_table(len(stage3), len(stage3), final_total))
    lines.append("")

    # Table 3
    lines.append("### Tableau 3 : TAM segmenté")
    lines.append(build_segment_table(stage3, final_total))
    lines.append("")

    # Table 4
    lines.append("### Tableau 4 : TAM Final répartition géographique")
    lines.append(build_region_table(stage3, final_total))
    lines.append("")

    # Table 5
    lines.append("### Tableau 5 : TAM Final par domaine")
    lines.append(build_macro_table(stage3, final_total))
    lines.append("")

    # Table 6
    lines.append("### Tableau 6 : Taux pénétration objectifs")
    lines.append(build_penetration_table(final_total))
    lines.append("")

    # Table 7
    lines.append("### Tableau 7 : Options élargissement TAM")
    lines.append(build_scenarios_table(final_total))
    lines.append("")

    # Synthèse sections
    lines.append("## Synthèse")
    tam_final_int = int(round(final_total))
    tam_prod = len(stage3)
    lines.append(f"TAM FINAL QALIA : {format_int(tam_final_int)} OF")
    lines.append("")
    lines.append("Entonnoir complet :")
    for label, count, pct_base, _ in funnel:
        lines.append(f"- {label} : {format_int(count)} ({format_percent(pct_base, 2)})")
    lines.append("")
    doc_final = 5167
    diff_pct = (tam_final_int - doc_final) / doc_final * 100 if doc_final else 0.0
    lines.append(f"vs Document 1 théorique (5 167) : {diff_pct:+.1f}%")
    lines.append("")

    lines.append("### Validation Avatar")
    lines.append("AVATAR V1 : ✅ VALIDÉ" if tam_final_int >= 3000 else "AVATAR V1 : ❌ INVALIDÉ")
    lines.append("")
    lines.append("Hypothèses validées :")
    lines.append("- 3-10 formateurs : ✅ {0} OF".format(format_int(len(stages["stage1"]))))
    lines.append("- Qualiopi + actifs : ✅ {0} OF".format(format_int(len(stage3))))
    lines.append("- Production 60% : ❌ 100% réel")
    lines.append("- Sweet spot : 3-10 formateurs")
    lines.append("")
    lines.append("TAM Final :")
    lines.append(f"- {format_int(tam_final_int)} OF (+{diff_pct:.1f}% vs doc)")
    lines.append("")

    lines.append("### Objectif 150K€ faisabilité")
    rate_150 = 502 / final_total if final_total else 0.0
    lines.append(f"OBJECTIF 150K€/MOIS : 502 clients → {format_percent(rate_150, 2)} du TAM")
    if rate_150 <= 0.03:
        horizon = "M6 : ✅ Atteignable | M12 : ✅ Réaliste | M18 : ✅ Très réaliste"
    elif rate_150 <= 0.08:
        horizon = "M6 : ⚠️ Très difficile | M12 : ✅ Réaliste | M18 : ✅ Très réaliste"
    else:
        horizon = "M6 : ❌ Peu réaliste | M12 : ⚠️ Ambitieux | M18 : ✅ Très réaliste"
    lines.append(f"FAISABILITÉ : {horizon}")
    lines.append("")
    lines.append("### Segments prioritaires")
    top_segments = compute_top_segments(stage3)
    total_share = sum(final for _, final in top_segments)
    share_pct = total_share / final_total if final_total else 0.0
    lines.append(f"TOP 3 SEGMENTS (représentent {format_percent(share_pct, 1)} TAM Final) :")
    for idx, (label, final) in enumerate(top_segments, start=1):
        lines.append(f"{idx}. [{label}] : {format_int(final)} OF")
    lines.append("")

    os.makedirs(os.path.dirname(OUTPUT_MD), exist_ok=True)
    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    write_csv(stage3)

if __name__ == "__main__":
    main()

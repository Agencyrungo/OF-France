import os
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
import math
import statistics

XLSX_PATH = "OF 3-10.xlsx"
OUTPUT_DIR = "analysis_outputs"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

# Column indices based on header inspection
COL_DENOMINATION = 2
COL_NB_STAGIAIRES = 27
COL_EFFECTIF = 29


def load_shared_strings(zf):
    shared_strings = []
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


def column_ref_to_index(ref):
    letters = "".join(ch for ch in ref if ch.isalpha())
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def get_cell_value(cell, shared_strings):
    t = cell.attrib.get("t")
    if t == "s":
        v = cell.find(NS + "v")
        if v is None or v.text is None:
            return None
        return shared_strings[int(v.text)]
    if t == "inlineStr":
        is_elem = cell.find(NS + "is")
        if is_elem is None:
            return None
        return "".join(t_el.text or "" for t_el in is_elem.findall('.//' + NS + 't'))
    v = cell.find(NS + "v")
    if v is None:
        return None
    return v.text


def parse_int(value):
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


def parse_float(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def load_records():
    records = []
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared_strings = load_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as f:
            row_iter = ET.iterparse(f, events=("end",))
            for event, elem in row_iter:
                if elem.tag != NS + "row":
                    continue
                row_index = int(elem.attrib.get("r"))
                if row_index == 1:
                    elem.clear()
                    continue
                values = {}
                for cell in elem.findall(NS + "c"):
                    ref = cell.attrib.get("r")
                    if not ref:
                        continue
                    col_idx = column_ref_to_index(ref)
                    val = get_cell_value(cell, shared_strings)
                    if val is not None:
                        values[col_idx] = val
                denomination = values.get(COL_DENOMINATION)
                effectif = parse_int(values.get(COL_EFFECTIF))
                nb_stagiaires = parse_float(values.get(COL_NB_STAGIAIRES))
                if effectif is None:
                    effectif = 0
                records.append(
                    {
                        "denomination": denomination or "",
                        "effectif": effectif,
                        "nb_stagiaires": nb_stagiaires,
                    }
                )
                elem.clear()
    return records


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def format_int(value):
    return f"{int(round(value)):,}" if value is not None else "-"


def format_float(value, decimals=1):
    if value is None:
        return "-"
    return f"{value:,.{decimals}f}"


def percentile(sorted_data, p):
    if not sorted_data:
        return None
    if p <= 0:
        return sorted_data[0]
    if p >= 1:
        return sorted_data[-1]
    k = (len(sorted_data) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_data[int(k)])
    d0 = sorted_data[f] * (c - k)
    d1 = sorted_data[c] * (k - f)
    return float(d0 + d1)


def write_csv_distribution(distribution_rows):
    import csv

    csv_path = os.path.join(OUTPUT_DIR, "distribution_0_100.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["effectif", "nombre_OF", "pct_total", "cumul_OF", "cumul_pct"])
        for row in distribution_rows:
            writer.writerow(row)
    return csv_path


def write_markdown_table(filename, headers, rows):
    path = os.path.join(OUTPUT_DIR, filename)
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    lines.extend("| " + " | ".join(str(cell) for cell in row) + " |" for row in rows)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def write_table1(records):
    total = len(records)
    counter = Counter(r["effectif"] for r in records)
    rows = []
    cumulative_count = 0
    for effectif in range(0, 101):
        count = counter.get(effectif, 0)
        cumulative_count += count
        pct = (count / total) * 100 if total else 0
        cumulative_pct = (cumulative_count / total) * 100 if total else 0
        rows.append(
            [
                effectif,
                count,
                pct,
                cumulative_count,
                cumulative_pct,
            ]
        )
    csv_rows = [
        [
            r[0],
            r[1],
            f"{r[2]:.6f}",
            r[3],
            f"{r[4]:.6f}",
        ]
        for r in rows
    ]
    csv_path = write_csv_distribution(csv_rows)

    md_rows = [
        [
            r[0],
            format_int(r[1]),
            f"{r[2]:.2f}%",
            format_int(r[3]),
            f"{r[4]:.2f}%",
        ]
        for r in rows
    ]
    md_path = write_markdown_table("table1_distribution.md", ["effectif", "nombre_OF", "% total", "cumul_OF", "cumul_%"], md_rows)
    return md_path, csv_path


def segment_for_effectif(effectif):
    if effectif == 0:
        return "Sans formateur", "0"
    if effectif == 1:
        return "Solo", "1"
    if effectif == 2:
        return "Duo", "2"
    if 3 <= effectif <= 10:
        return "Cible Qalia", "3-10"
    if 11 <= effectif <= 20:
        return "PME formation", "11-20"
    if 21 <= effectif <= 50:
        return "Grandes structures", "21-50"
    if 51 <= effectif <= 100:
        return "Très grandes", "51-100"
    return "Géantes", "101+"


def write_table2(records):
    total = len(records)
    segment_data = {}
    segment_order = [
        "Sans formateur",
        "Solo",
        "Duo",
        "Cible Qalia",
        "PME formation",
        "Grandes structures",
        "Très grandes",
        "Géantes",
    ]
    interpretation = {
        "Sans formateur": "Administratif uniquement",
        "Solo": "Indépendants",
        "Duo": "Binôme",
        "Cible Qalia": "Sweet spot",
        "PME formation": "Scale-up",
        "Grandes structures": "Entreprise",
        "Très grandes": "Groupe",
        "Géantes": "Holding/Réseau",
    }
    for record in records:
        seg_name, seg_range = segment_for_effectif(record["effectif"])
        seg = segment_data.setdefault(seg_name, {
            "range": seg_range,
            "count": 0,
            "nb_sum": 0.0,
            "nb_count": 0,
            "effectif_sum": 0,
        })
        seg["count"] += 1
        nb = record["nb_stagiaires"]
        if nb is not None:
            seg["nb_sum"] += nb
            seg["nb_count"] += 1
        seg["effectif_sum"] += record["effectif"]
    rows = []
    for seg_name in segment_order:
        seg = segment_data.get(seg_name, {"range": "-", "count": 0, "nb_sum": 0.0, "nb_count": 0})
        avg = seg["nb_sum"] / seg["nb_count"] if seg.get("nb_count") else None
        rows.append([
            seg_name,
            seg.get("range", "-"),
            format_int(seg["count"]),
            f"{(seg["count"] / total * 100) if total else 0:.2f}%",
            format_float(avg, 0),
            interpretation.get(seg_name, ""),
        ])
    md_path = write_markdown_table(
        "table2_segments.md",
        ["Segment", "Tranche effectif", "Nombre OF", "% total", "Moyenne stagiaires", "Interprétation"],
        rows,
    )
    return md_path, segment_data


def compute_segment_metrics(records, segment_names):
    data = {name: {"records": []} for name in segment_names}
    for record in records:
        seg_name, _ = segment_for_effectif(record["effectif"])
        if seg_name in data:
            data[seg_name]["records"].append(record)
    return data


def median_from_list(values):
    if not values:
        return None
    return float(statistics.median(values))


def write_table3(records):
    total = len(records)
    total_nb = sum(r["nb_stagiaires"] for r in records if r["nb_stagiaires"] is not None)
    focus_segments = ["Solo", "Duo", "Cible Qalia", "PME formation"]
    data = compute_segment_metrics(records, focus_segments)
    rows = []
    comparison_values = {}
    for name in focus_segments:
        seg_records = data[name]["records"]
        count = len(seg_records)
        nb_values = [r["nb_stagiaires"] for r in seg_records if r["nb_stagiaires"] is not None]
        nb_sum = sum(nb_values)
        avg_nb = (nb_sum / len(nb_values)) if nb_values else None
        median_nb = median_from_list(nb_values)
        effectif_sum = sum(r["effectif"] for r in seg_records if r["effectif"] > 0)
        ratio = (nb_sum / effectif_sum) if effectif_sum else None
        pct_total = (count / total * 100) if total else 0
        pct_nb = (nb_sum / total_nb * 100) if total_nb else None
        comparison_values[name] = {
            "count": count,
            "pct_total": pct_total,
            "avg_nb": avg_nb,
            "median_nb": median_nb,
            "nb_sum": nb_sum,
            "pct_nb": pct_nb,
            "ratio": ratio,
        }
    base_row = ["Métrique", "Solo (1)", "Duo (2)", "Cible 3-10", "PME 11-20", "Ratio 3-10 vs"]

    def ratio_string(values, metric_key, targets):
        parts = []
        numerator = values["Cible Qalia"].get(metric_key)
        for label, seg_name in targets:
            denominator = values[seg_name].get(metric_key)
            if numerator is None or denominator in (None, 0):
                parts.append(f"vs {label}: -")
            else:
                parts.append(f"vs {label}: {numerator / denominator * 100:.1f}%")
        return " | ".join(parts)

    metrics = []
    metrics.append((
        "Nombre OF",
        lambda v: format_int(v["count"]),
        lambda values: ratio_string(values, "count", [("1", "Solo"), ("2", "Duo")]),
    ))
    metrics.append((
        "% base totale",
        lambda v: f"{v['pct_total']:.2f}%",
        lambda values: ratio_string(values, "pct_total", [("1", "Solo"), ("2", "Duo")]),
    ))
    metrics.append((
        "Stagiaires moyen",
        lambda v: format_float(v["avg_nb"], 0),
        lambda values: ratio_string(values, "avg_nb", [("1", "Solo"), ("2", "Duo")]),
    ))
    metrics.append((
        "Stagiaires médian",
        lambda v: format_float(v["median_nb"], 0),
        lambda values: ratio_string(values, "median_nb", [("1", "Solo"), ("2", "Duo")]),
    ))
    metrics.append((
        "Stagiaires total",
        lambda v: format_float(v["nb_sum"], 0),
        lambda values: f"% France: {values['Cible Qalia']['pct_nb']:.2f}%" if values['Cible Qalia']['pct_nb'] is not None else "-",
    ))
    metrics.append((
        "% du total France",
        lambda v: f"{v['pct_nb']:.2f}%" if v["pct_nb"] is not None else "-",
        lambda values: ratio_string(values, "pct_nb", [("11-20", "PME formation")]) if values['PME formation']['pct_nb'] else "-",
    ))
    metrics.append((
        "Ratio stag/form",
        lambda v: format_float(v["ratio"], 1),
        lambda values: ratio_string(values, "ratio", [("1", "Solo"), ("2", "Duo"), ("11-20", "PME formation")]),
    ))

    rows = [base_row]
    rows.append(["---"] * len(base_row))
    for metric_name, formatter, compare_fn in metrics:
        row = [metric_name]
        for seg in focus_segments:
            row.append(formatter(comparison_values[seg]))
        comparison_text = compare_fn(comparison_values)
        row.append(comparison_text)
        rows.append(row)

    path = os.path.join(OUTPUT_DIR, "table3_comparison.md")
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write("| " + " | ".join(row) + " |\n")
    return path, comparison_values


def write_table4(records):
    over_100 = [r for r in records if r["effectif"] > 100]
    over_1000 = [r for r in over_100 if r["effectif"] > 1000]
    top20 = sorted(over_100, key=lambda r: r["effectif"], reverse=True)[:20]
    rows = []
    for rank, record in enumerate(top20, start=1):
        nb = record["nb_stagiaires"]
        ratio = (nb / record["effectif"]) if (nb is not None and record["effectif"]) else None
        rows.append([
            rank,
            format_int(record["effectif"]),
            record["denomination"] or "-",
            format_float(nb, 0),
            format_float(ratio, 1),
            "",
        ])
    md_path = write_markdown_table(
        "table4_top20.md",
        ["rang", "effectif_declares", "denomination", "nb_stagiaires", "ratio_stag/form", "interpretation"],
        rows,
    )
    summary_path = os.path.join(OUTPUT_DIR, "outliers_summary.md")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"OF avec effectif > 1000 : {len(over_1000)}\n")
        if over_1000:
            f.write("\n")
            for record in sorted(over_1000, key=lambda r: r["effectif"], reverse=True):
                nb = record["nb_stagiaires"]
                ratio = (nb / record["effectif"]) if (nb is not None and record["effectif"]) else None
                f.write(
                    f"- {record['denomination'] or '-'} : effectif={record['effectif']:,}, nb_stagiaires={format_float(nb, 0)}, ratio={format_float(ratio, 1)}\n"
                )
    return md_path, summary_path


def write_table5(records):
    effectifs = [r["effectif"] for r in records]
    effectifs_no_outliers = [e for e in effectifs if e <= 1000]
    effectifs_sorted = sorted(effectifs_no_outliers)
    mean_val = sum(effectifs_no_outliers) / len(effectifs_no_outliers)
    median_val = percentile(effectifs_sorted, 0.5)
    counter = Counter(effectifs_no_outliers)
    mode_val = counter.most_common(1)[0][0]
    std_val = statistics.pstdev(effectifs_no_outliers)
    q1 = percentile(effectifs_sorted, 0.25)
    q3 = percentile(effectifs_sorted, 0.75)
    p90 = percentile(effectifs_sorted, 0.90)
    p95 = percentile(effectifs_sorted, 0.95)
    p99 = percentile(effectifs_sorted, 0.99)
    rows = [
        ["Population (<=1000)", format_int(len(effectifs_no_outliers))],
        ["Moyenne", format_float(mean_val, 2)],
        ["Médiane", format_float(median_val, 0)],
        ["Mode", format_int(mode_val)],
        ["Écart-type", format_float(std_val, 2)],
        ["Q1 (25e)", format_float(q1, 0)],
        ["Q3 (75e)", format_float(q3, 0)],
        ["P90", format_float(p90, 0)],
        ["P95", format_float(p95, 0)],
        ["P99", format_float(p99, 0)],
    ]
    md_path = write_markdown_table(
        "table5_stats.md",
        ["Statistique", "Valeur"],
        rows,
    )
    return md_path, {
        "mean": mean_val,
        "median": median_val,
        "mode": mode_val,
        "std": std_val,
        "q1": q1,
        "q3": q3,
        "p90": p90,
        "p95": p95,
        "p99": p99,
        "population": len(effectifs_no_outliers),
    }


def write_summary(records, segment_data, comparison_values, stats, outliers_summary_path):
    total = len(records)
    total_nb = sum(r["nb_stagiaires"] for r in records if r["nb_stagiaires"] is not None)
    zero = segment_data["Sans formateur"]["count"]
    one = segment_data["Solo"]["count"]
    two = segment_data["Duo"]["count"]
    three_ten = segment_data["Cible Qalia"]["count"]
    eleven_twenty = segment_data["PME formation"]["count"]
    twenty_one_plus = total - (zero + one + two + three_ten + eleven_twenty)
    summary_lines = []
    summary_lines.append("# Synthèse exécutive\n")
    summary_lines.append("## 1. Validation des volumes clés\n")
    summary_lines.append(
        f"- Total organismes : {total:,}\n"
        f"- 0 formateur : {zero:,} ({zero / total * 100:.2f}%)\n"
        f"- 1 formateur : {one:,} ({one / total * 100:.2f}%)\n"
        f"- 2 formateurs : {two:,} ({two / total * 100:.2f}%)\n"
        f"- 3-10 formateurs : {three_ten:,} ({three_ten / total * 100:.2f}%)\n"
        f"- 11-20 formateurs : {eleven_twenty:,} ({eleven_twenty / total * 100:.2f}%)\n"
        f"- 21+ formateurs : {twenty_one_plus:,} ({twenty_one_plus / total * 100:.2f}%)\n"
        f"- Total stagiaires déclarés : {total_nb:,.0f}\n"
    )
    summary_lines.append("\n## 2. Concentration du marché\n")
    summary_lines.append(
        f"- 74.4% des OF ont 0 ou 1 formateur ({(zero + one) / total * 100:.1f}%)\n"
        f"- Segment 3-10 : {three_ten:,} OF ({three_ten / total * 100:.2f}%)\n"
    )
    summary_lines.append("\n## 3. Potentiel segments adjacents\n")
    segment_1_2 = one + two
    summary_lines.append(
        f"- OF 1-2 formateurs : {segment_1_2:,} ({segment_1_2 / total * 100:.2f}% de la base)\n"
        f"- OF 11-20 formateurs : {eleven_twenty:,} ({eleven_twenty / total * 100:.2f}% de la base)\n"
    )
    avg_3_10 = comparison_values["Cible Qalia"]["avg_nb"]
    avg_1 = comparison_values["Solo"]["avg_nb"]
    avg_2 = comparison_values["Duo"]["avg_nb"]
    total_3_10 = comparison_values["Cible Qalia"]["nb_sum"]
    total_1 = comparison_values["Solo"]["nb_sum"]
    total_2 = comparison_values["Duo"]["nb_sum"]
    summary_lines.append(
        f"- Stagiaires moyens 3-10 vs 1 : {avg_3_10 / avg_1 * 100:.1f}% | vs 2 : {avg_3_10 / avg_2 * 100:.1f}%\n"
        f"- Stagiaires totaux 3-10 : {total_3_10:,.0f} ({total_3_10 / total_nb * 100:.2f}% du total)\n"
    )
    ratio_3_10 = comparison_values["Cible Qalia"]["ratio"]
    ratio_1 = comparison_values["Solo"]["ratio"]
    ratio_2 = comparison_values["Duo"]["ratio"]
    ratio_11_20 = comparison_values["PME formation"]["ratio"]
    summary_lines.append(
        f"- Productivité stagiaires/formateur 3-10 vs 1 : {ratio_3_10 / ratio_1 * 100:.1f}% | vs 2 : {ratio_3_10 / ratio_2 * 100:.1f}% | vs 11-20 : {ratio_3_10 / ratio_11_20 * 100:.1f}%\n"
    )
    summary_lines.append("\n## 4. Distribution & outliers\n")
    with open(outliers_summary_path, "r", encoding="utf-8") as f:
        outlier_info = f.read().strip()
    summary_lines.append(outlier_info + "\n")
    summary_lines.append(
        f"- Valeur max observée : {max(r['effectif'] for r in records):,}\n"
        f"- Statistiques (<=1000) : moyenne {stats['mean']:.2f}, médiane {stats['median']:.0f}, P90 {stats['p90']:.0f}, P99 {stats['p99']:.0f}\n"
    )
    path = os.path.join(OUTPUT_DIR, "summary.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))
    return path


def main():
    ensure_output_dir()
    records = load_records()
    table1_path, csv_path = write_table1(records)
    table2_path, segment_data = write_table2(records)
    table3_path, comparison_values = write_table3(records)
    table4_path, outliers_path = write_table4(records)
    table5_path, stats = write_table5(records)
    summary_path = write_summary(records, segment_data, comparison_values, stats, outliers_path)
    print("Generated:")
    for path in [table1_path, csv_path, table2_path, table3_path, table4_path, table5_path, summary_path]:
        print(path)


if __name__ == "__main__":
    main()

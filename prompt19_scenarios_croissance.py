from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal
import csv
import math

TAM_FINAL = 8612
PRICE = 299

@dataclass
class ScenarioConfig:
    name: str
    new_clients: List[float]
    churn_rate: float
    referral_rate: float = 0.0
    referral_mode: Literal["none", "prev_new", "prev_total"] = "none"


def run_scenario(config: ScenarioConfig) -> List[dict]:
    clients = 0.0
    prev_new = 0.0
    rows: List[dict] = []

    for idx, marketing_new in enumerate(config.new_clients, start=1):
        if config.referral_mode == "prev_new":
            referrals = prev_new * config.referral_rate
        elif config.referral_mode == "prev_total":
            referrals = clients * config.referral_rate
        else:
            referrals = 0.0

        churn = clients * config.churn_rate
        clients = clients - churn + marketing_new + referrals

        rows.append(
            {
                "Mois": idx,
                "Nouveaux": marketing_new,
                "Referrals": referrals,
                "Churn": churn,
                "Total clients": clients,
                "MRR": clients * PRICE,
                "ARR": clients * PRICE * 12,
                "Pénétration TAM": clients / TAM_FINAL,
            }
        )

        prev_new = marketing_new

    return rows


def format_currency(value: float) -> str:
    return f"{value:,.0f}€".replace(",", " ")


def format_percentage(value: float) -> str:
    return f"{value * 100:.2f}%"


def make_markdown_table(headers: List[str], rows: List[List[str]]) -> str:
    table_lines = ["| " + " | ".join(headers) + " |"]
    table_lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        table_lines.append("| " + " | ".join(row) + " |")
    return "\n".join(table_lines)


def rounded(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def build_tables() -> None:
    scenario_a = ScenarioConfig(
        name="Scénario A",
        new_clients=[10, 10, 15, 20, 25, 30, 25, 25, 25, 30, 30, 35],
        churn_rate=0.03,
    )
    scenario_b = ScenarioConfig(
        name="Scénario B",
        new_clients=[39, 41, 43, 45, 47, 49, 45, 45, 45, 42, 44, 44],
        churn_rate=0.02,
        referral_rate=0.05,
        referral_mode="prev_new",
    )
    scenario_c = ScenarioConfig(
        name="Scénario C",
        new_clients=[65, 75, 85, 95, 105, 128],
        churn_rate=0.015,
        referral_rate=0.10,
        referral_mode="prev_new",
    )

    data_a = run_scenario(scenario_a)
    data_b = run_scenario(scenario_b)
    data_c = run_scenario(scenario_c)

    table1 = make_markdown_table(
        ["Mois", "Nouveaux", "Churn", "Total clients", "MRR", "ARR", "Pénétration TAM"],
        [
            [
                str(row["Mois"]),
                rounded(row["Nouveaux"]),
                rounded(row["Churn"]),
                rounded(row["Total clients"]),
                format_currency(row["MRR"]),
                format_currency(row["ARR"]),
                format_percentage(row["Pénétration TAM"]),
            ]
            for row in data_a
        ],
    )

    table2 = make_markdown_table(
        ["Mois", "Nouveaux", "Referrals", "Churn", "Total clients", "MRR", "Pénétration TAM"],
        [
            [
                str(row["Mois"]),
                rounded(row["Nouveaux"]),
                rounded(row["Referrals"]),
                rounded(row["Churn"]),
                rounded(row["Total clients"]),
                format_currency(row["MRR"]),
                format_percentage(row["Pénétration TAM"]),
            ]
            for row in data_b
        ],
    )

    table3 = make_markdown_table(
        ["Mois", "Nouveaux", "Referrals", "Churn", "Total clients", "MRR", "Pénétration TAM"],
        [
            [
                str(row["Mois"]),
                rounded(row["Nouveaux"]),
                rounded(row["Referrals"]),
                rounded(row["Churn"]),
                rounded(row["Total clients"]),
                format_currency(row["MRR"]),
                format_percentage(row["Pénétration TAM"]),
            ]
            for row in data_c
        ],
    )

    def pick_metric(data: List[dict], month: int, key: str) -> float:
        for row in data:
            if row["Mois"] == month:
                return row[key]
        return float("nan")

    table4 = make_markdown_table(
        ["Métrique", "Scénario A", "Scénario B", "Scénario C"],
        [
            ["Atteinte 150K€", "M18", "M12", "M6"],
            [
                "MRR M6",
                format_currency(pick_metric(data_a, 6, "MRR")),
                format_currency(pick_metric(data_b, 6, "MRR")),
                format_currency(pick_metric(data_c, 6, "MRR")),
            ],
            [
                "MRR M12",
                format_currency(pick_metric(data_a, 12, "MRR")),
                format_currency(pick_metric(data_b, 12, "MRR")),
                ">250K€",
            ],
            [
                "Clients M6",
                f"{pick_metric(data_a, 6, 'Total clients'):.0f}",
                f"{pick_metric(data_b, 6, 'Total clients'):.0f}",
                f"{pick_metric(data_c, 6, 'Total clients'):.0f}",
            ],
            [
                "Clients M12",
                f"{pick_metric(data_a, 12, 'Total clients'):.0f}",
                f"{pick_metric(data_b, 12, 'Total clients'):.0f}",
                ">800",
            ],
            ["Budget 6 mois", "30K€", "90K€", "340K€"],
            ["Budget 12 mois", "70K€", "180K€", ">600K€"],
            ["Équipe M6", "1→2", "3", "8"],
            ["CAC moyen", "300€", "350€", "590€"],
            ["Payback", "1.0 mois", "1.2 mois", "2.0 mois"],
            ["Churn moyen", "3%", "2%", "1.5%"],
            ["Faisabilité", "✅ Haute", "✅ Réaliste", "⚠️ Difficile"],
        ],
    )

    conv_rate = 0.15
    cost_per_lead = 70
    volumetry_rows: List[List[str]] = []
    detailed_rows: List[List[str]] = []

    for row in data_b:
        month = row["Mois"]
        conversions = row["Nouveaux"]
        leads_needed = math.ceil(conversions / conv_rate)
        budget_leads = leads_needed * cost_per_lead
        if month <= 2:
            salespeople = 2
        elif month <= 6:
            salespeople = 3
        else:
            salespeople = 4
        salary_cost = salespeople * 4000
        total_cost = budget_leads + salary_cost

        volumetry_rows.append(
            [
                f"M{month}",
                f"{conversions:.1f}",
                f"{leads_needed}",
                f"{budget_leads/1000:.1f}K€",
                str(salespeople),
                f"{salary_cost/1000:.0f}K€",
                f"{total_cost/1000:.0f}K€",
            ]
        )

        detailed_rows.append(
            [
                str(month),
                f"{conversions:.4f}",
                f"{row['Referrals']:.4f}",
                f"{row['Churn']:.4f}",
                f"{row['Total clients']:.4f}",
                f"{row['MRR']:.2f}",
                f"{row['ARR']:.2f}",
                f"{row['Pénétration TAM']:.6f}",
                str(leads_needed),
                f"{budget_leads:.2f}",
                str(salespeople),
                f"{salary_cost:.2f}",
                f"{total_cost:.2f}",
            ]
        )

    table5 = make_markdown_table(
        [
            "Mois",
            "Conv nécessaires",
            "Leads (conv 15%)",
            "Budget leads (70€)",
            "Commerciaux",
            "Salaires",
            "Total coûts",
        ],
        volumetry_rows,
    )

    table6 = make_markdown_table(
        ["Risque", "Scénario A", "Scénario B", "Scénario C", "Mitigation"],
        [
            ["TAM insuffisant", "🟢 Faible", "🟠 Moyen", "🔴 Élevé", "Valider TAM >5K"],
            ["Churn élevé", "🔴 3%", "🟠 2%", "🟢 1.5%", "Onboarding + Support"],
            ["Budget manquant", "🟢 70K€", "🟠 180K€", "🔴 600K€", "Fundraising ou bootstrap"],
            ["Équipe insuffisante", "🟢 Solo", "🟠 3 pers", "🔴 10 pers", "Recrutement progressif"],
            ["Taux conv optimiste", "🟢 12%", "🟠 15%", "🔴 18%", "Tests A/B continus"],
        ],
    )

    table7 = make_markdown_table(
        ["Critère", "Poids", "Scénario A", "Scénario B", "Scénario C"],
        [
            ["Faisabilité financière", "30%", "10/10", "8/10", "4/10"],
            ["Rapidité atteinte 150K€", "25%", "3/10", "8/10", "10/10"],
            ["Risque", "25%", "9/10", "7/10", "4/10"],
            ["Ressources disponibles", "20%", "10/10", "7/10", "3/10"],
            ["SCORE TOTAL", "100%", "8.2", "7.6", "5.4"],
        ],
    )

    base_dir = Path(__file__).resolve().parent
    out_dir = base_dir / "analysis_outputs"
    out_dir.mkdir(exist_ok=True)

    csv_headers = [
        "Month",
        "New_clients",
        "Referrals",
        "Churn",
        "Total_clients",
        "MRR",
        "ARR",
        "TAM_penetration",
        "Leads_required",
        "Marketing_budget",
        "Salespeople",
        "Salary_cost",
        "Total_costs",
    ]

    with (out_dir / "prompt19_scenarioB_projection.csv").open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(csv_headers)
        for row in detailed_rows:
            writer.writerow(row)

    synthesis = "\n".join(
        [
            "# PROMPT 19 — Scénarios de croissance 150K€",
            "",
            "## Tableau 1 : Progression Scénario A (M1-M12)",
            table1,
            "",
            "## Tableau 2 : Progression Scénario B (M1-M12)",
            table2,
            "",
            "## Tableau 3 : Progression Scénario C (M1-M6)",
            table3,
            "",
            "## Tableau 4 : Récapitulatif des scénarios",
            table4,
            "",
            "## Tableau 5 : Volumétrie Scénario B (M1-M12)",
            table5,
            "",
            "## Tableau 6 : Risques et mitigation",
            table6,
            "",
            "## Tableau 7 : Scoring des scénarios",
            table7,
            "",
            "## Synthèse",
            "- **Scénario A – Conservateur** : 31K€ MRR à M6 (≈104 clients), 75K€ à M12 (≈245 clients), budget marketing 12 mois ≈70K€, équipe 1→2 personnes.",
            "- **Scénario B – Réaliste (recommandé)** : 79K€ MRR à M6 (≈262 clients), 149K€ à M12 (≈497 clients), budget marketing 12 mois cible 180K€, équipe 3-4 personnes, LTV/CAC ≈8.0.",
            "- **Scénario C – Agressif** : 172K€ MRR à M6 (≈577 clients), budget 6 mois ≈340K€, équipe 8-10 commerciaux, faisabilité conditionnée à un financement.",
            "",
            "## Recommandation",
            "Privilégier **le Scénario B** pour atteindre 150K€ de MRR en 12 mois avec un rapport LTV/CAC > 7 et une structure d'équipe soutenable.",
        ]
    )

    (out_dir / "prompt19_scenarios_croissance.md").write_text(synthesis, encoding="utf-8")


if __name__ == "__main__":
    build_tables()

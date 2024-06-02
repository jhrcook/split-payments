#!/usr/bin/env python

"""Splitting costs from Seattle & Olympic NP trip."""

from dataclasses import dataclass

import polars as pl
from rich import print as rprint

PEOPLE = ("Car", "JHC", "CTH", "Jorrelle")
PRECISION = 2


@dataclass
class Payer:
    """Expense payer."""

    name: str
    spent: float
    cashflow: float

    @property
    def owes_money(self) -> bool:
        """Payer owes money to the others."""
        return round(self.cashflow, PRECISION) > 0

    @property
    def is_owed_money(self) -> bool:
        """Payer is owed money by the others."""
        return round(self.cashflow, PRECISION) < 0


def resolve_expenses(expenses: pl.DataFrame) -> None:
    payers: list[Payer] = []
    for name, spent, cashflow in expenses.iter_rows():
        payers.append(Payer(name=name, spent=spent, cashflow=cashflow))
    payers.sort(key=lambda p: -p.cashflow)

    for payer in filter(lambda p: p.owes_money, payers):
        for recipient in filter(lambda p: p.is_owed_money, reversed(payers)):
            amount_exchanged = min(abs(payer.cashflow), abs(recipient.cashflow))
            amount_exchanged = round(amount_exchanged, PRECISION)
            if amount_exchanged == 0:
                continue
            rprint(f"{payer.name} {amount_exchanged} -> {recipient.name}")
            payer.cashflow -= amount_exchanged
            recipient.cashflow += amount_exchanged

    rprint("[blue]Final cashflow states:[/blue]")
    for payer in payers:
        print(f"  {payer.name}: {payer.cashflow:0,.4f}")


def main() -> None:
    """Splitting costs from Seattle & Olympic NP trip."""
    expenses = (
        pl.read_csv("./expenses.csv")
        .rename({"Payer": "payer"})
        .with_columns(pl.col("Amount").str.replace("\\$", "").cast(pl.Float64))
        .group_by("payer")
        .agg(pl.col("Amount").sum().alias("per person spent"))
    )
    for p in PEOPLE:
        if p not in expenses["payer"]:
            expenses = pl.concat(
                [expenses, pl.DataFrame({"payer": p, "per person spent": 0.0})]
            )

    total_expenses = expenses["per person spent"].sum()
    rprint(f"Total expenses: ${total_expenses:,.3f}")

    owed_per_person = total_expenses / expenses["payer"].n_unique()
    rprint(f"Total owed per person: ${owed_per_person:,.3f}")

    expenses = expenses.with_columns(
        (owed_per_person - pl.col("per person spent")).alias("per person cashflow")
    )
    rprint(expenses)
    resolve_expenses(expenses)


if __name__ == "__main__":
    main()

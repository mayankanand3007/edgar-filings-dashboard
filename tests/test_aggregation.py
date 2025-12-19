from __future__ import annotations

import pandas as pd
import dask.dataframe as dd
from edgar_pipeline.aggregate.build_gold import gold_forms_by_year

def test_gold_forms_by_year_counts() -> None:
    pdf = pd.DataFrame([
        {"year": 2024, "form_type": "10-K"},
        {"year": 2024, "form_type": "10-K"},
        {"year": 2024, "form_type": "8-K"},
    ])
    ddf = dd.from_pandas(pdf, npartitions=1)
    g = gold_forms_by_year(ddf).compute()
    counts = {(int(r["year"]), r["form_type"]): int(r["filings_count"]) for _, r in g.iterrows()}
    assert counts[(2024, "10-K")] == 2
    assert counts[(2024, "8-K")] == 1

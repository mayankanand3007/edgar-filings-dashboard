from __future__ import annotations

import pandas as pd
import dask.dataframe as dd
from edgar_pipeline.clean.transform import clean_raw_ddf

def test_cleaning_normalizes_form_type_and_trims_text() -> None:
    pdf = pd.DataFrame([{
        "cik": 1,
        "company_name": "  Acme   Inc  ",
        "form_type": " 10-k ",
        "date_filed": "2024-02-03",
        "accession_number": "0001",
        "file_name": "edgar/data/1/0001.txt",
        "year": 2024,
        "quarter": "2024Q1",
        "edgar_url": "https://www.sec.gov/Archives/edgar/data/1/0001.txt",
        "ingest_ts": "2024-02-03T00:00:00",
    }])
    ddf = dd.from_pandas(pdf, npartitions=1)
    out = clean_raw_ddf(ddf).compute()
    assert out.loc[0, "form_type"] == "10-K"
    assert out.loc[0, "company_name"] == "Acme Inc"

"""
Transform Excel rows → hh.ru JSON-like dicts.

Used by synthetic_hh_dag to convert manually prepared Excel files
into the same format that HHExtractor would return from the API.
"""

from typing import Any, Dict, List

import pandas as pd


def transform_excel_to_hh(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Convert a DataFrame (from Excel) into a list of hh.ru-style dicts.

    Expected columns:
        id, name, published_at,
        employer_id, employer_name, employer_industry,
        area_id, area_name, area_parent_id, area_parent_name,
        salary_from, salary_to, salary_currency, salary_gross,
        experience_id, experience_name,
        employment_id, employment_name,
        schedule_id, schedule_name,
        key_skills (comma-separated)
    """
    records = []

    for _, row in df.iterrows():
        skills = [
            s.strip()
            for s in str(row["key_skills"]).split(",")
            if s.strip()
        ]

        record = {
            "id": str(row["id"]),
            "name": row["name"],
            "published_at": str(row["published_at"]),
            "employer": {
                "id": str(row["employer_id"]),
                "name": row["employer_name"],
                "industry": str(row.get("employer_industry", "")),
            },
            "area": {
                "id": str(row["area_id"]),
                "name": row["area_name"],
                "parent": {
                    "id": str(row["area_parent_id"]),
                    "name": row["area_parent_name"],
                },
            },
            "salary": {
                "from": _safe_int(row.get("salary_from")),
                "to": _safe_int(row.get("salary_to")),
                "currency": str(row.get("salary_currency", "RUR")),
                "gross": str(row.get("salary_gross", "")).upper() == "TRUE",
            },
            "experience": {
                "id": str(row.get("experience_id", "")),
                "name": str(row.get("experience_name", "")),
            },
            "employment": {
                "id": str(row.get("employment_id", "")),
                "name": str(row.get("employment_name", "")),
            },
            "schedule": {
                "id": str(row.get("schedule_id", "")),
                "name": str(row.get("schedule_name", "")),
            },
            "key_skills": [{"name": s} for s in skills],
        }
        records.append(record)

    return records


def _safe_int(value) -> int | None:
    """Convert to int, return None if NaN or invalid."""
    if pd.isna(value):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
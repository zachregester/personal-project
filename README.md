**Zach's People Analytics Repo — Miniature dbt Project (View-Only)**
--------
Purpose: Demonstrate how I structure dbt projects using **fully synthetic data**.

Goal: Highlight process, design, and best practices - not to replicate real business complexity.

Please note: All data is fake and illustrative. Nothing comes from any employer or proprietary system.

**Highlights**
--------

**Models**: staging → production → aggregated_external_models  

**Tests**: custom SQL test for dbt model

**Macros**: generate_schema_name.sql (schema config override instructions)

**Python examples:** separate API notebooks (`x_python_example/`) to show comfort pulling from REST APIs and shaping data with pandas (separate from dbt project)

**Repo tour**
---------

```text
models/
  ├─ sources/                              # raw-like tables (synthetic)
  ├─ staging/                              # cleaned, conformed tables
  ├─ production/                           # business-ready models (analysis-ready / dashboard-ready)
  └─ aggregated_external_models/           # aggregated tables (safe to share with business stakeholders)

macros/
  └─ generate_schema_name.sql              # tiny utility macros (schema config instructions)

tests/
  └─ no_dups_tableau_leader_scorecard.sql  # example singular test

x_python_example/
  └─ pokemon_api.ipynb                     # standalone Python API scripts (unrelated to dbt, included to demonstrate REST API + pandas)
  └─ restcountries.ipynb
  └─ rickandmorty.ipynb  

dbt_project.yml                            # conventions, folders, quoting, tests
```

**Notes**
--------

This repo is for viewing only - it omits environment configuration and credentials.

Definitions are generic (industry - standard HR metrics like headcount, hires, attrition, etc).

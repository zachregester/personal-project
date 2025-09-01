**Zach's People Analytics Repo — Miniature dbt Project (View-Only)**
--------
Purpose: Demonstrate how I structure dbt projects using **fully synthetic data**.

Goal: Highlight process, design, and best practices - not to replicate real business complexity.

Note: All data is fake and illustrative. Nothing comes from any employer or proprietary system.

**Highlights**
--------

**Models**: full flow from sources → staging → production → aggregated_external_models  

**Tests**: exampple custom SQL test for a dbt model

**Macros**: generate_schema_name.sql for schema config overrides

**Python examples:** separate API notebooks (`x_python_example/`) to show pulling from REST APIs and shaping data with pandas (separate from dbt project)

**Repo Tour**
---------

```text
models/
  ├─ sources/                              # information on raw data (synthetic)
  ├─ staging/                              # cleaned, conformed tables
  ├─ production/                           # business-ready models (analysis-ready / dashboard-ready)
  └─ aggregated_external_models/           # aggregated tables (safe to share with business stakeholders)

macros/
  └─ generate_schema_name.sql              # tiny utility macros (schema config instructions)

tests/
  └─ no_dups_tableau_leader_scorecard.sql  # example singular test

x_python_example/
  └─ pokemon_api.ipynb                     # REST API + Pandas examples (unrelated to dbt project, included to demonstrate ability)
  └─ restcountries.ipynb
  └─ rickandmorty.ipynb  

dbt_project.yml                            # conventions, folders, quoting, tests
```

**Notes**
--------
In a real production environment, I would schedule a daily job (seed → run → test) to refesh data and add source freshness checks on regular cadence.

This repo is for viewing only - it omits environment configuration and credentials.

Definitions are generic (industry - standard HR metrics like headcount, hires, attrition, etc).

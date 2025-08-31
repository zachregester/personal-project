**Example People Analytics Repo — Miniature dbt Project (View-Only)**

Purpose: Demonstrate how I structure dbt projects using **fully synthetic data**. The goal is to highlight process, design, and best practices - not to replicate real business complexity.

Please note: All data is fake and illustrative. Nothing comes from any employer or proprietary system.

**What to look at**

Models: sources → staging → production → aggregated_external_models 

Testing: schema.yml with not_null

Macros: schema config instructions

**Repo tour**

models/
  sources/                     # raw-like tables (synthetic)
  staging/                     # cleaned, conformed tables
  production/                  # business-ready models (analysis-ready / dashboard-ready)
  aggregated_external_models/  # aggregated tables (aggregated data safe to share with business stakeholders) 
macros/                        # tiny utility macros (schema config instructions)
tests/                         # example singular tests (if any)
dbt_project.yml                # conventions, folders, quoting, tests

**Notes**

This repo is for viewing only - it omits environment configuration and credentials.

Definitions are generic (industry - standard HR metrics like headcount, hires, attrition, etc).

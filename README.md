**Example People Analytics Repo — Miniature dbt Project (View-Only)**

Purpose: Demonstrate how I structure dbt projects using **fully synthetic data**.

Goal: Highlight process, design, and best practices - not to replicate real business complexity.

Please note: All data is fake and illustrative. Nothing comes from any employer or proprietary system.

**What to look at**

models:
  - staging
  - production
  - aggregated_external_models 

tests:
  - schema.yml (custom SQL test)

macros:
  - generate_schema_name.sql (schema config override instructions)

X_python_example:
  - pokemon_api.ipynb
  - restcountries_api.ipynb
  - rickandmorty_apo.ipynb

**Repo tour**

**models**/

        -sources/                         # raw-like tables (synthetic)
  
        -staging/                         # cleaned, conformed tables
  
        -production/                      # business-ready models (analysis-ready / dashboard-ready)
  
        -aggregated_external_models/      # aggregated tables (aggregated data safe to share with business stakeholders) 
  
**macros**/                     
        -generate_schema_name.sql         # tiny utility macros (schema config instructions)

**tests**/                      
        -no_dups_tableau_leader_scorecard # example singular tests

**X_python_example**/           # standalone Python API scripts (unrelated to dbt, included to demonstrate ability to pull from REST APIs and transform JSON with pandas)

**Notes**

This repo is for viewing only - it omits environment configuration and credentials.

Definitions are generic (industry - standard HR metrics like headcount, hires, attrition, etc).

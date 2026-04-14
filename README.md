# Data Quality & Validation Pipeline

> **Tools:** Python · Pandas · SQLite · Matplotlib · Seaborn  
> **Data:** Synthetic sales dataset — 5,150 records · 13 injected issue types · 16 columns  
> **Author:** Akash Trivedi · [LinkedIn](#) · [Portfolio](#)

---

## Business Question

**How do we systematically identify, quantify, and remediate data quality issues before they corrupt reporting and analytics?**

This project builds a production-style data quality validation pipeline for a retail sales dataset. It simulates the real-world problem of receiving messy CRM/transactional data and establishing automated checks that catch issues before they reach dashboards or downstream models.

---

## Key Findings

| Metric | Value |
|--------|-------|
| Total Records Ingested | **5,150** |
| Overall DQ Score | **76.9 / 100** |
| Records with Issues | **1,396 (27.1%)** |
| Records Remediated (Clean) | **4,754 (92.3% retention)** |
| Records Quarantined | **253 (4.9%)** |
| CRITICAL Issues Found | **6 check categories** |
| WARNING Issues Found | **3 check categories** |

---

## Data Quality Issues Injected

| Issue Type | Count | Severity |
|------------|-------|----------|
| Missing values (email, phone, province, customer_id) | ~730 cells | CRITICAL / WARNING |
| Duplicate records (exact + near-duplicate) | 150 rows | CRITICAL |
| Invalid email formats | 200 records | WARNING |
| Invalid phone formats | 180 records | WARNING |
| Inconsistent province names (ON, ontario, Ont., ONTARIO) | 600 records | WARNING |
| Negative / zero unit prices | 130 records | CRITICAL |
| Price outliers (50× average) | 30 records | CRITICAL |
| Ship date before order date | 120 records | CRITICAL |
| Type mismatches (quantity as string: "5 units") | 100 records | CRITICAL |
| Future order dates (beyond 2024-07-01) | 60 records | CRITICAL |
| Whitespace / encoding issues | 150 records | INFO |
| Referential integrity (invalid customer IDs) | 90 records | WARNING |
| Invalid discount values (< 0 or > 100%) | 70 records | WARNING |

---

## Pipeline Architecture

```
sales_dirty.csv (5,150 rows)
        ↓
[01_generate_messy_data.py]  → Injects 13 issue types into clean base data
        ↓
[02_validation_pipeline.py]  → DataQualityCheck class, 12 automated checks
        ↓
   ┌────────────────────────────────────────┐
   │  sales_flagged.csv   (5,150 rows)      │  ← All rows, _issues + _severity cols
   │  sales_remediated.csv (4,754 rows)     │  ← Passed all CRITICAL checks
   │  sales_quarantined.csv (253 rows)      │  ← Failed CRITICAL, needs review
   └────────────────────────────────────────┘
        ↓
[data_quality_checks.sql]   → SQL checks for completeness, uniqueness, validity,
                               consistency + CREATE TABLE sales_clean
```

---

## DQ Score Methodology

The DQ Score (0–100) penalises each CRITICAL issue more heavily than WARNING:

| Severity | Weight per affected row |
|----------|------------------------|
| CRITICAL | −2.0 points |
| WARNING  | −0.5 points |
| INFO     | −0.1 points |

Score is capped at 0, normalised over total records, and reported as a single composite metric for executive reporting.

---

## Validation Checks Performed

### Section 1 — Completeness
- Missing values count per column
- Completeness rate % (email, phone, province, customer_id)

### Section 2 — Uniqueness
- Duplicate order_id detection
- Full duplicate rows (all key fields match)
- Duplicate emails across different customer IDs

### Section 3 — Validity
- Email format validation (`LIKE '%_@__%.__%'`)
- Negative or zero unit prices
- Extreme price outliers (> 50× average)
- Invalid quantity (non-numeric, zero, negative)
- Invalid discount percentage (< 0 or > 100)

### Section 4 — Consistency
- Ship date before order date (logical violation)
- Future order dates (beyond data collection period)
- Non-standard province names (vs. canonical 13-province list)
- Revenue cross-validation: `qty × price × (1 − discount)` vs stored revenue

### Section 5 — DQ Summary Dashboard
- Master summary query: one row per check, severity, affected count, % of total

### Section 6 — Remediation
- `CREATE TABLE sales_clean` with auto-standardisation:
  - Province codes → full names (ON → Ontario, QC → Quebec, etc.)
  - ABS() applied to negative prices/quantities
  - Discount clamped to [0, 100]
  - Deduplication via MIN(rowid) per (order_id, customer_id, order_date)
  - Exclusion filters for CRITICAL violations

---

## Visualizations

| # | Chart | Insight |
|---|-------|---------|
| 1 | Issues by Type (bar) | Province inconsistency is the most frequent issue (600 rows) |
| 2 | Severity Distribution (pie) | 27% WARNING, 8% CRITICAL, 65% clean |
| 3 | DQ Score Gauge | Overall score: 76.9/100 |
| 4 | Completeness Heatmap | Email and phone have highest missing rates |
| 5 | Price Distribution (before/after) | Outliers compress the scale significantly |
| 6 | Issues Timeline | Issue injection spread uniformly across order dates |

---

## Project Structure

```
data-quality-pipeline/
├── data/
│   ├── sales_dirty.csv           # 5,150 messy records (generated)
│   ├── sales_clean_reference.csv # 5,000 clean reference records
│   ├── sales_flagged.csv         # All rows with _issues + _severity cols
│   ├── sales_remediated.csv      # 4,754 records that passed CRITICAL checks
│   └── sales_quarantined.csv     # 253 records quarantined for review
├── notebooks/
│   ├── 01_generate_messy_data.py # Synthetic dirty dataset generator
│   └── 02_validation_pipeline.py # DataQualityCheck class + 12 checks + 6 charts
├── sql/
│   └── data_quality_checks.sql   # SQL quality library (6 sections)
├── charts/
│   ├── 01_issues_by_type.png
│   ├── 02_severity_distribution.png
│   ├── 03_dq_score.png
│   ├── 04_completeness_heatmap.png
│   ├── 05_price_distribution.png
│   └── 06_issues_timeline.png
└── README.md
```

---

## How to Run

```bash
git clone https://github.com/akash-trivedi/data-quality-pipeline
cd data-quality-pipeline
pip install pandas numpy matplotlib seaborn

python notebooks/01_generate_messy_data.py   # generates sales_dirty.csv
python notebooks/02_validation_pipeline.py   # runs all checks, exports CSVs + charts
```

To run the SQL checks, load `data/sales_dirty.csv` into SQLite (DB Browser, DBeaver, or similar) and execute `sql/data_quality_checks.sql` section by section.

---

## Business Recommendations

**Immediate fixes (data engineering):**
- **Province standardisation** — 600 records affected. Implement a lookup table at ingestion time to map all abbreviations/variants to canonical names. Single highest-impact fix.
- **Email validation at source** — 200 invalid formats suggest the CRM form has no front-end validation. Add regex check at point of entry.

**Process improvements:**
- **Revenue cross-validation** should run as a nightly reconciliation job. Any discrepancy > $0.01 triggers an alert to the finance team.
- **Duplicate detection** should be built into the ETL using a composite key (order_id + customer_id + order_date). 150 duplicates in 5,000 records = 3% — unacceptable in a billing context.

**Monitoring:**
- Publish the DQ Score dashboard (Section 5 SQL query) to the BI tool. Track the score weekly. Target: **> 95/100** within 2 sprints.

---

## Technologies Used

| Tool | Purpose |
|------|---------|
| Python 3.10 | Data generation, validation pipeline, visualisations |
| Pandas | Data wrangling, type casting, deduplication |
| Matplotlib / Seaborn | 6 DQ diagnostic charts |
| SQLite / SQL | Structured quality checks, remediation table creation |
| GitHub | Version control + portfolio |

---

## About

Part of a **7-day Canadian Data Analyst Portfolio Sprint**.

**Target roles:** Data Analyst · Data Quality Analyst · Junior Data Engineer · BI Analyst  
**NOC Code:** 21223 — Data Analyst (TEER 2, CEC eligible)

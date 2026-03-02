# Lead Salvage Pipeline

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A robust, enterprise-grade deterministic data cleaning pipeline with an integrated LLM fallback for semantic processing. Designed for high reliability, memory safety, and seamless extraction of critical business leads.

---

## ⚡ Features

- **Deterministic Base Cleaning:** Normalizes and dedupes high-confidence leads instantly (emails, phone numbers, names, formats).
- **LLM Semantic Fallback:** For complex unstructured records, routes data to a powerful Llama-based local MLX model to synthetically clean records.
- **Strict Memory Safety:** Implements a 95% RAM usage ceiling, forcing garbage collection across chunks to prevent out-of-memory crashes even on massive jobs.
- **Enterprise Security:** Built-in sanitization blocks formula injection attacks in CSVs (`=`, `+`, `-`, `@`) and destroys immediate malware signatures like `=cmd|`.

---

## 🚀 Quick Start

### 1. Installation

Requires Python 3.9+. It is highly recommended to use a virtual environment.

```bash
# Clone the repository
git clone https://github.com/your-org/data_cleaning_pipeline.git
cd data_cleaning_pipeline

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Usage

**CSV Cleaning (Standard Run)**
```bash
python run_pipeline.py <input_csv> <output_directory>

# Example
python run_pipeline.py input/leads.csv output/cleaned_run
```

**Health Check (Dry Run)**
Assess data quality and predict AI consumption requirements without writing outputs:
```bash
python run_pipeline.py <input_csv> --health-check
```

**DOCX Cleaning**
Extract and clean targets from unstructured Word documents:
```bash
python -m lead_cleaner.docx_cleaner.runner <input.docx> [output.docx]
```

### 3. Review Results

Quick audit for the most recent pipeline execution:
```bash
python analyze_results.py --latest
```

Outputs will be securely deposited in your designated output path:
- `output/<dir>/final_output_<run_id>.csv` (Cleaned results)
- `output/<dir>/reject_store_<run_id>.csv` (Irrecoverable records)
- `logs/run_<run_id>.csv` (Trace audit of all structural changes)

### 4. Post-Pipeline Validation

You can filter the cleaned output against custom business rules using the `validate_leads` utility.
This tool uses a simple, human-readable syntax to enforce data quality requirements.

**Validation Syntax:**
Rule files use "Easy Syntax" (`rules.txt`):
```text
# Require specific fields to be present and valid
Column[email] == Valid
Column[phone] == Valid

# Exact matches and OR conditions
Column[Status] == "Active"
Column[Response] == "Yes" OR "Y"

# Truthy check (must not be empty)
Column[first_name] == True
```

**Usage:**
Filter your pipeline output to isolate only leads that meet your strict criteria:
```bash
python validate_leads/validate_leads.py --input output/cleaned_run/final_output_<run_id>.csv --rules validate_leads/sample_rules.txt
```
This will output `valid_leads.csv` in the same directory as the input file, containing only records that passed all specified rules.

---

## 📋 Input Specifications

**CSV format** must contain at least one primary unqiue identifier:
- `email`
- `phone`

*Optional auxiliary columns*: `first_name`, `last_name`, `job_title`, `company`, `date`. The pipeline will automatically lowercase and strip column spaces.

**DOCX format** can be any valid `.docx` text file containing potential leads.

---

## 🧠 Architecture Data Flow

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                           LEAD CLEANER PIPELINE                             │
└─────────────────────────────────────────────────────────────────────────────┘

                                   ┌──────────────┐
                                   │  INPUT CSV   │
                                   └──────┬───────┘
                                          │
    ┌─────────────────────────────────────▼─────────────────────────────────────┐
    │  PHASE 0: VALIDATION                                                      │
    │  • Verify file health, unroll headers, define UUIDs                       │
    └─────────────────────────────────────┬─────────────────────────────────────┘
                                          │
    ┌─────────────────────────────────────▼─────────────────────────────────────┐
    │  PHASE 1: DETERMINISTIC FAST-PASS                                         │
    │  • Email/Phone regex extraction, Proper-case Normalization                │
    │  • Cross-row Fingerprinting for duplicate isolation                       │
    │  • Calculate confidence score (0.0 to 1.0)                                │
    └─────────────────────────────────────┬─────────────────────────────────────┘
                                          │
                  ┌───────────────────────┼──────────────────────┐
            [ ≥0.7 Score ]          [ <0.7 Score ]        [ Duplicates ]
                  │                       │                      │
                  ▼                       ▼                      ▼
            ┌─────────┐             ┌────────────┐         ┌──────────┐
            │  CLEAN  │             │ AI_REQUIRED│         │ REJECTED │
            └────┬────┘             └─────┬──────┘         └─────┬────┘
                 │                        │                      │
                 │          ┌─────────────▼──────────────┐       │
                 │          │  PHASE 2: SEMANTIC LLM     │       │
                 │          │  • Chunked LLM Parsing     │       │
                 │          │  • Temperature=0, GC-safe  │       │
                 │          └─────────────┬──────────────┘       │
                 │                        │                      │
                 ▼                        ▼                      ▼
    ┌───────────────────────────────────────────────────────────────────────────┐
    │  PHASE 3: MERGE & VERIFY                                                  │
    │  • Stitch processed records, map back row limits, assert invariants       │
    └───────────────────────────────────────────────────────────────────────────┘
```

---

## 🛡️ Operational Safety

### Hardened Execution
- **Out of Memory Resistance:** In the event memory usage exceeds the 95% threshold `lead_cleaner/config.py:CHUNK_SIZE`, execution is gracefully suspended—securing the current chunk before closing.
- **Deterministic Bounds:** Input counts *strictly* equal terminal output bounds (`clean` + `rejected`). Missing or dropped pointers will invalidate the build internally, shielding against silently lost data.

### Troubleshooting Codes
| State | Cause |
|-------|-------|
| `AI_REQUIRED` | Initial deterministic sweep fell below 70% confidence index. Sent to model. |
| `INVALID_FORMAT` | LLM synthetic generation failed strict JSON spec mapping. |
| `MODEL_CRASH` | Interrupted memory limit during inference. |
| `DUPLICATE` | Deduplication flagged against primary signature. |
| `UNKNOWN` | Fatal runtime exception. |

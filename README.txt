# Scraper Guardrails — Fault-Tolerant Web Data Acquisition (Python)

A reliability-first toolkit for **resilient web scraping** that survives:
- HTML/API changes  
- rate limits  
- transient failures  
- network flakiness  
- upstream instability  

This project demonstrates the mindset and tooling of a Support Engineer / SRE:
**observability → resilience → graceful degradation → recovery paths.**

Built as part of my SRE-track portfolio while applying for roles such as  
**Tesla — Software Support Engineer, Cell Software.**

---

## Overview

Scraper Guardrails provides a small, production-inspired framework that includes:

### 1. Resilient Fetching (retry + backoff + jitter)
- Exponential backoff  
- Circuit breaker protection  
- Token-bucket rate limiting  
- Automatic retry classification  

### 2. Anti-Fragile Selectors
Fallback strategies that continue working even when HTML structure changes.

### 3. Structural Diff Detection
Detect template drift early using hashed DOM fingerprints.

### 4. Dead-Letter Queue
Failed URLs are captured cleanly for post-run triage.

### 5. Structured JSON Logging
Machine-readable logs designed for debugging and observability.

### 6. Run Health Report
Writes `health.json` summarizing pipeline success/fail counts.

---

## Project Structure

```text
scraper-guardrails/
├─ guardrails/
│  ├─ fetch.py          # retry/backoff, rate limit, circuit breaker
│  ├─ selectors.py      # resilient selectors with fallback sets
│  ├─ diffwatch.py      # detect HTML/API structural changes
│  ├─ health.py         # write health.json after runs
│  ├─ logs.py           # structured JSON logging
│  └─ __init__.py
│
├─ pipelines/
│  └─ nba_boxscores.py  # example pipeline using guardrails
│
├─ sample/
│  └─ urls.txt
│
├─ cli.py               # CLI to run pipelines + triage failures
├─ requirements.txt
└─ README.md
'''

## Example Pipeline — NBA Boxscores 
The included example pipeline (pipelines/nba_boxscores.py) demonstrates:  
-Resilient fetching  
-Fallback selectors   
-DOM structure diffing  
-Dead-letter capture  
-Structured logging  
-Health report output  

## Installation  
git clone https://github.com/richie-p-meyer/scraper-guardrails.git  
cd scraper-guardrails  
pip install -r requirements.txt  

## Usage    
    
### Run the NBA pipeline    
python3 cli.py run \  
  --pipeline nba \  
  --urls-file sample/urls.txt \  
  --out out/data.jsonl \  
  --dead-letter out/dead_letter.jsonl \  
  --health out/health.json  
  
## Triage dead-letter failures    
python3 cli.py triage --dead-letter out/dead_letter.jsonl  
  
## Inspect structural diffs   
python3 cli.py diffs --cache .cache  
  

## Future Improvements  
  
-Add Playwright integration for full JS rendering  
-Add schema detection for JSON APIs  
-Add automated alerting for structural drift  
-Add dashboard for visualizing failures + trends  

## About the Author — Richard Wilders  

-Marine Corps veteran — Afghanistan deployment (mission-critical language ops)  
-Graduate student in Data Science (M.S. in progress)  
-Reliability-minded engineer with experience debugging distributed systems  
-Strong background in Python, SQL, scraping, ETL, and observability  
-Ten 10-day Vipassana courses — calm under pressure during incidents  
-Based in Reno/Sparks, NV

## Connect  
  
GitHub: https://github.com/richie-p-meyer    
LinkedIn: https://www.linkedin.com/in/richard-wilders-915395106/  
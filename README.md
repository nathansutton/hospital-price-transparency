# Hospital Price Transparency

[![URL Validation](https://github.com/nathansutton/hospital-price-transparency/actions/workflows/scrape.yml/badge.svg)](https://github.com/nathansutton/hospital-price-transparency/actions/workflows/scrape.yml)
[![Tests](https://github.com/nathansutton/hospital-price-transparency/actions/workflows/test.yml/badge.svg)](https://github.com/nathansutton/hospital-price-transparency/actions/workflows/test.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Automated scraper for hospital price transparency data, collecting standardized pricing information from hospitals across (eventually) 50 US states.

## The Problem: Disappearing Data

Hospital price transparency files are required by law, but they're ephemeral. Hospitals update or replace their files without notice, and there's no official archive. When a hospital changes their prices, the old data vanishes. This makes it impossible to:

- Track how prices change over time
- Analyze trends in healthcare pricing
- Hold hospitals accountable to their published rates
- Research the effects of price transparency regulations

## The Solution: Git as a Time Machine

This project uses [git-scraping](https://simonwillison.net/2020/Oct/9/git-scraping/)—a technique pioneered by Simon Willison—to create a living archive of hospital pricing data. By committing scraped data to git on a regular schedule, we get:

**Version Control as a Database**
- Every commit is a snapshot of hospital prices at a point in time
- `git log data/VT/470011.jsonl` shows the complete price history for a hospital
- `git diff` reveals exactly what changed between any two dates

**Slowly Changing Dimension (Type 2)**
- New prices are captured without destroying old data
- The git history preserves the full timeline
- You can reconstruct prices as of any historical date using `git checkout`

**Free, Distributed Storage**
- GitHub hosts the archive at no cost
- Anyone can fork and maintain their own copy
- Data survives even if hospitals take down their files

### Example: Tracking Price Changes

```bash
# See all price changes for a hospital
git log --oneline data/VT/470011.jsonl

# Compare prices between two dates
git diff abc123..def456 data/VT/470011.jsonl

# Get prices as they were on a specific date
git checkout $(git rev-list -n 1 --before="2025-06-01" HEAD) -- data/VT/470011.jsonl
```

## Overview

The Centers for Medicare and Medicaid Services requires hospitals under [45 CFR §180.50](https://www.federalregister.gov/d/2019-24931/p-1010) to publish [machine-readable price lists](https://www.cms.gov/hospital-price-transparency). This project automates the collection and normalization of this data for research purposes.

**Current Coverage:** 20 states with hospital URLs sourced from [hospitalpricingfiles.org](https://hospitalpricingfiles.org)

## Quick Start

```bash
# Clone the repository
git clone https://github.com/nathansutton/hospital-price-transparency.git
cd hospital-price-transparency

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Validate all URLs are accessible
uv run python scripts/scrape.py --validate-only

# Scrape a single state
uv run python scripts/scrape.py --state VT

# Scrape all states
uv run python scripts/scrape.py
```

## Data Format

Each hospital's data is stored as a JSONL file in `data/{STATE}/{CCN}.jsonl`:

```json
{"cpt": "99213", "type": "cash", "price": 89.50}
{"cpt": "99213", "type": "gross", "price": 150.00}
{"cpt": "99214", "type": "cash", "price": 125.00}
{"cpt": "99214", "type": "gross", "price": 210.00}
```

| Field | Description |
|-------|-------------|
| `cpt` | CPT/HCPCS procedure code (5 alphanumeric characters) |
| `type` | Price type: `cash` (self-pay discounted) or `gross` (chargemaster) |
| `price` | Price in USD |

## CLI Usage

```bash
# Scrape all states
uv run python scripts/scrape.py

# Scrape specific state
uv run python scripts/scrape.py --state NC

# Scrape specific hospital by CCN
uv run python scripts/scrape.py --ccn 340001

# Validate URLs only (no scraping)
uv run python scripts/scrape.py --validate-only

# Dry run (fetch and parse but don't save)
uv run python scripts/scrape.py --dry-run

# Verbose logging
uv run python scripts/scrape.py -v

# Generate summary from status files
uv run python scripts/generate_summary.py
```

## Architecture

```
hospital-price-transparency/
├── src/
│   ├── scrapers/          # Strategy pattern: format-specific scrapers
│   │   ├── base.py        # Abstract base class
│   │   ├── cms_json_scraper.py  # CMS standard JSON
│   │   ├── cms_csv_scraper.py   # CMS standard CSV
│   │   ├── cms_xlsx_scraper.py  # Excel format
│   │   └── registry.py    # Factory pattern for scraper selection
│   ├── normalizers/       # CPT code normalization
│   ├── utils/             # HTTP client, logging
│   ├── models.py          # Pydantic data models
│   └── config.py          # Configuration management
├── scripts/
│   ├── scrape.py          # Main CLI entry point
│   └── generate_summary.py # Status aggregation
├── dim/
│   └── urls/              # Hospital URLs by state (*.json)
├── data/                  # Output: data/{STATE}/{CCN}.jsonl
├── status/                # Status tracking: status/{STATE}.csv
└── tests/                 # Unit tests
```

## How Git-Scraping Works

1. **Daily Schedule**: GitHub Actions runs the validator daily at 8 AM UTC
2. **URL Validation**: Each state's hospital URLs are checked for accessibility
3. **Status Tracking**: Results are saved to `status/{STATE}.csv`
4. **Auto-Commit**: Changes are committed back to the repository
5. **History Preserved**: Git maintains the complete history of all changes

The scraper itself can be run manually or on your own schedule to capture full price data. Each run creates a new snapshot in git history.

## Coverage

| Region | States |
|--------|--------|
| Northeast | CT, MA, NH, NJ, NY, PA, RI, VT |
| Southeast | AL, DE, FL, GA, KY, MD, NC, SC, TN, VA, WV |
| Southwest | TX |

## Development

```bash
# Install dependencies (includes dev tools)
uv sync

# Run tests
uv run pytest tests/ -v

# Run type checking
uv run mypy src/

# Run linter
uv run ruff check src/

# Format code
uv run ruff format src/
```

## Ontology

We use the [OHDSI Athena](https://athena.ohdsi.org/) vocabulary to normalize CPT and HCPCS codes to a common data model. This ensures consistent procedure identification across hospital systems.

**Note:** Hospital-specific items (e.g., room types, facility fees) are excluded as they don't map to standard codes.

## Research Questions

This data enables investigation of healthcare pricing economics:

> "The trade association for insurers said it was 'an anomaly' that some insured patients got worse prices than those paying cash."
> — [NYT, August 2021](https://www.nytimes.com/interactive/2021/08/22/upshot/hospital-prices.html)

**Open questions:**
- How often do cash prices beat negotiated insurance rates?
- What is the variance in prices for common procedures across hospitals?
- How have prices changed since transparency requirements took effect?
- Which hospitals have raised or lowered prices since the mandate?

## Related Projects

- [Simon Willison's Git Scraping](https://simonwillison.net/2020/Oct/9/git-scraping/) - The technique this project uses
- [Turquoise Health](https://turquoise.health/) - Consumer-friendly price lookup tool
- [OHDSI Athena](https://athena.ohdsi.org/) - Healthcare vocabulary browser
- [Hospital Chargemaster](https://github.com/vsoch/hospital-chargemaster) - Alternative scraping approach

## License

MIT License - See [LICENSE](LICENSE) for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests (`uv run pytest tests/`)
5. Submit a pull request

Issues and pull requests welcome!

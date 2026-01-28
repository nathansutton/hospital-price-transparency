.PHONY: install test scrape scrape-all scrape-fresh

# Defaults for parallel scraping
PARALLEL ?= 8
MAX_AGE_DAYS ?= 30
TIMEOUT ?= 600

install:
	uv sync

test:
	uv run pytest tests/ -v

# Scrape with incremental updates (skip files < 30 days old by default)
scrape:
	@mkdir -p logs
	uv run python scripts/scrape.py --parallel $(PARALLEL) --max-age-days $(MAX_AGE_DAYS) --timeout $(TIMEOUT) 2>&1 | tee logs/scrape-$$(date +%Y%m%d-%H%M%S).log
	uv run python scripts/generate_summary.py

# Full scrape ignoring existing files
scrape-fresh:
	uv run python scripts/scrape.py --parallel $(PARALLEL) --timeout $(TIMEOUT) $(if $(STATE),--state $(STATE))
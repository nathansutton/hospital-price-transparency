.PHONY: install test scrape scrape-all

install:
	uv sync

test:
	uv run pytest tests/ -v

scrape:
	uv run python scripts/scrape.py $(if $(STATE),--state $(STATE))

scrape-all:
	@mkdir -p logs
	uv run python scripts/scrape.py 2>&1 | tee logs/scrape-$$(date +%Y%m%d-%H%M%S).log
	uv run python scripts/generate_summary.py

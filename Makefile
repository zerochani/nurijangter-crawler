# Makefile for NuriJangter Crawler

.PHONY: help install test run clean docker-build docker-run

# Default target
help:
	@echo "NuriJangter Crawler - Available Commands:"
	@echo ""
	@echo "  make install         Install dependencies and setup environment"
	@echo "  make test            Run test suite"
	@echo "  make run             Run crawler once"
	@echo "  make run-scheduled   Run crawler in scheduled mode"
	@echo "  make clean           Clean generated files"
	@echo "  make docker-build    Build Docker image"
	@echo "  make docker-run      Run crawler in Docker"
	@echo "  make lint            Run code linters"
	@echo "  make format          Format code with black"
	@echo ""

# Install dependencies
install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt
	@echo "Installing Playwright browsers..."
	playwright install chromium
	@echo "Setup complete!"

# Run tests
test:
	@echo "Running tests..."
	pytest tests/ -v
	@echo "Tests complete!"

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	pytest tests/ --cov=src --cov-report=html --cov-report=term
	@echo "Coverage report generated in htmlcov/"

# Run crawler
run:
	@echo "Running crawler..."
	python main.py --resume

# Run crawler without resume
run-fresh:
	@echo "Running crawler (fresh start)..."
	python main.py --no-resume --clear-checkpoint

# Run crawler in scheduled mode
run-scheduled:
	@echo "Running crawler in scheduled mode..."
	python main.py --scheduled --resume

# Run with debug logging
run-debug:
	@echo "Running crawler with debug logging..."
	python main.py --log-level DEBUG

# Dry run
dry-run:
	@echo "Dry run - checking configuration..."
	python main.py --dry-run

# Clean generated files
clean:
	@echo "Cleaning generated files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete
	@echo "Clean complete!"

# Clean data (use with caution!)
clean-data:
	@echo "WARNING: This will delete all collected data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		rm -rf data/*.json data/*.csv logs/*.log checkpoints/*.json; \
		echo "Data cleaned!"; \
	else \
		echo "Cancelled."; \
	fi

# Docker commands
docker-build:
	@echo "Building Docker image..."
	docker-compose build
	@echo "Docker image built!"

docker-run:
	@echo "Running crawler in Docker..."
	docker-compose up crawler

docker-run-scheduled:
	@echo "Running scheduled crawler in Docker..."
	docker-compose --profile scheduled up crawler-scheduled

docker-shell:
	@echo "Opening shell in Docker container..."
	docker-compose run --rm crawler /bin/bash

# Linting
lint:
	@echo "Running linters..."
	flake8 src/ tests/ --max-line-length=100 --exclude=__pycache__
	mypy src/ --ignore-missing-imports
	@echo "Linting complete!"

# Format code
format:
	@echo "Formatting code..."
	black src/ tests/ --line-length=100
	isort src/ tests/
	@echo "Formatting complete!"

# Setup development environment
dev-setup: install
	@echo "Setting up development environment..."
	pip install black flake8 mypy isort pytest-cov
	cp .env.example .env
	@echo "Development environment ready!"

# View logs
logs:
	@echo "Recent log entries:"
	@tail -n 50 logs/crawler_*.log 2>/dev/null || echo "No logs found"

# View errors
errors:
	@echo "Recent errors:"
	@tail -n 50 logs/errors_*.log 2>/dev/null || echo "No errors found"

# Show statistics
stats:
	@echo "Crawler Statistics:"
	@echo "-------------------"
	@echo "JSON files: $$(ls -1 data/*.json 2>/dev/null | wc -l)"
	@echo "CSV files: $$(ls -1 data/*.csv 2>/dev/null | wc -l)"
	@echo "Log files: $$(ls -1 logs/*.log 2>/dev/null | wc -l)"
	@echo "Checkpoints: $$(ls -1 checkpoints/*.json 2>/dev/null | wc -l)"

# Test extraction (quick test)
test-extraction:
	@echo "Running extraction test (first page only)..."
	python test_extraction.py

# Debug detail page
debug-detail:
	@echo "Running detail page debug..."
	python debug_detail_page.py

# Clear cache
clear-cache:
	@echo "Clearing cache and checkpoints..."
	python clear_cache.py

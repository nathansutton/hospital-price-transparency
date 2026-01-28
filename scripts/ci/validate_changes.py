#!/usr/bin/env python3
"""
Validate proposed changes before creating a PR.

Safety checks:
1. Only allowed file paths are modified
2. No forbidden patterns (secrets, passwords, API keys)
3. Valid JSON/Python syntax
4. Dry-run scrape succeeds for affected CCNs
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


# Allowed file path patterns
ALLOWED_PATHS = [
    r"^dim/urls/[a-z]{2}\.json$",
    r"^src/scrapers/registry\.py$",
]

# Forbidden patterns in file content
FORBIDDEN_PATTERNS = [
    r"password\s*[:=]",
    r"secret\s*[:=]",
    r"api[_-]?key\s*[:=]",
    r"token\s*[:=]",
    r"bearer\s+",
    r"authorization\s*[:=]",
    r"-----BEGIN (RSA |EC |DSA )?PRIVATE KEY-----",
    r"AKIA[0-9A-Z]{16}",  # AWS Access Key ID
    r"sk-[a-zA-Z0-9]{48}",  # OpenAI/Anthropic API key pattern
]


def is_allowed_path(path: str) -> bool:
    """Check if a file path is in the allowed list."""
    for pattern in ALLOWED_PATHS:
        if re.match(pattern, path):
            return True
    return False


def check_forbidden_patterns(content: str, filename: str) -> list[str]:
    """Check for forbidden patterns in file content."""
    violations = []
    for pattern in FORBIDDEN_PATTERNS:
        if re.search(pattern, content, re.IGNORECASE):
            violations.append(f"{filename}: Contains forbidden pattern matching '{pattern}'")
    return violations


def validate_json_syntax(path: Path) -> list[str]:
    """Validate JSON file syntax."""
    try:
        with open(path) as f:
            json.load(f)
        return []
    except json.JSONDecodeError as e:
        return [f"{path}: Invalid JSON - {e}"]


def validate_python_syntax(path: Path) -> list[str]:
    """Validate Python file syntax."""
    try:
        with open(path) as f:
            compile(f.read(), path, "exec")
        return []
    except SyntaxError as e:
        return [f"{path}: Invalid Python - {e}"]


def get_modified_files() -> list[str]:
    """Get list of modified files from git."""
    result = subprocess.run(
        ["git", "diff", "--name-only", "HEAD"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return []
    return [f.strip() for f in result.stdout.splitlines() if f.strip()]


def get_staged_files() -> list[str]:
    """Get list of staged files from git."""
    result = subprocess.run(
        ["git", "diff", "--name-only", "--cached"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return []
    return [f.strip() for f in result.stdout.splitlines() if f.strip()]


def extract_affected_ccns(files: list[str]) -> list[str]:
    """Extract CCNs from modified URL JSON files."""
    ccns = []
    for filepath in files:
        if filepath.startswith("dim/urls/") and filepath.endswith(".json"):
            try:
                # Get the diff to find which entries changed
                result = subprocess.run(
                    ["git", "diff", "HEAD", "--", filepath],
                    capture_output=True,
                    text=True,
                )
                # Look for CCN patterns in the diff
                for match in re.finditer(r'"ccn":\s*"(\d{6})"', result.stdout):
                    ccn = match.group(1)
                    if ccn not in ccns:
                        ccns.append(ccn)
            except Exception:
                pass
    return ccns


def run_dry_run_scrape(ccns: list[str]) -> tuple[bool, str]:
    """Run a dry-run scrape for the given CCNs."""
    if not ccns:
        return True, "No CCNs to validate"

    results = []
    all_success = True

    for ccn in ccns[:5]:  # Limit to 5 CCNs for speed
        result = subprocess.run(
            ["uv", "run", "python", "scripts/scrape.py", "--ccn", ccn, "--dry-run"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            results.append(f"  {ccn}: PASS")
        else:
            results.append(f"  {ccn}: FAIL - {result.stderr[:100]}")
            all_success = False

    return all_success, "\n".join(results)


def validate_manifest(manifest: dict) -> list[str]:
    """Validate a fix manifest structure."""
    errors = []

    required_fields = ["fix_type", "files", "verification_ccns", "commit_message"]
    for field in required_fields:
        if field not in manifest:
            errors.append(f"Manifest missing required field: {field}")

    fix_type = manifest.get("fix_type", "")
    if fix_type not in ("url-update", "registry-update", "encoding-fix"):
        errors.append(f"Invalid fix_type: {fix_type}")

    for file_entry in manifest.get("files", []):
        path = file_entry.get("path", "")
        if not is_allowed_path(path):
            errors.append(f"File not in allowed paths: {path}")

    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Validate proposed changes before creating a PR"
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Path to fix manifest JSON file",
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip dry-run scrape validation",
    )
    parser.add_argument(
        "--check-staged",
        action="store_true",
        help="Check staged files instead of modified files",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on any warning, not just errors",
    )

    args = parser.parse_args()

    errors = []
    warnings = []

    # Get files to validate
    if args.check_staged:
        files = get_staged_files()
    else:
        files = get_modified_files()

    if not files:
        print("No modified files to validate")
        return

    print(f"Validating {len(files)} modified file(s)...")

    # Check allowed paths
    for filepath in files:
        if not is_allowed_path(filepath):
            errors.append(f"File not in allowed paths: {filepath}")

    # Validate file content
    for filepath in files:
        if not is_allowed_path(filepath):
            continue

        path = Path(filepath)
        if not path.exists():
            warnings.append(f"File does not exist (deleted?): {filepath}")
            continue

        with open(path) as f:
            content = f.read()

        # Check forbidden patterns
        forbidden = check_forbidden_patterns(content, filepath)
        errors.extend(forbidden)

        # Syntax validation
        if filepath.endswith(".json"):
            errors.extend(validate_json_syntax(path))
        elif filepath.endswith(".py"):
            errors.extend(validate_python_syntax(path))

    # Validate manifest if provided
    if args.manifest:
        if args.manifest.exists():
            with open(args.manifest) as f:
                manifest = json.load(f)
            errors.extend(validate_manifest(manifest))
        else:
            errors.append(f"Manifest file not found: {args.manifest}")

    # Extract and validate affected CCNs
    ccns = extract_affected_ccns(files)
    if ccns:
        print(f"Affected CCNs: {', '.join(ccns)}")

        if not args.skip_scrape:
            print("Running dry-run scrape validation...")
            success, results = run_dry_run_scrape(ccns)
            print(results)
            if not success:
                errors.append("Dry-run scrape failed for some CCNs")

    # Report results
    print()
    if warnings:
        print("Warnings:")
        for w in warnings:
            print(f"  - {w}")
        print()

    if errors:
        print("Errors:")
        for e in errors:
            print(f"  - {e}")
        print()
        print("Validation FAILED")
        sys.exit(1)

    if args.strict and warnings:
        print("Validation FAILED (strict mode)")
        sys.exit(1)

    print("Validation PASSED")


if __name__ == "__main__":
    main()

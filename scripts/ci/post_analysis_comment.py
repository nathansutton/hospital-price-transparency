#!/usr/bin/env python3
"""
Post Claude analysis as a comment on a GitHub issue.

Invokes Claude Code to analyze the failure issue and posts the result.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path


def get_issue_details(issue_number: int, repo: str | None = None) -> dict | None:
    """Get issue details from GitHub."""
    args = ["gh", "issue", "view", str(issue_number), "--json", "title,body,labels"]
    if repo:
        args.extend(["--repo", repo])

    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error getting issue: {result.stderr}", file=sys.stderr)
        return None

    return json.loads(result.stdout)


def run_claude_analysis(issue_details: dict, prompt_file: Path) -> str | None:
    """Run Claude Code to analyze the failure."""
    # Read the analysis prompt
    with open(prompt_file) as f:
        base_prompt = f.read()

    # Construct the full prompt with issue context
    full_prompt = f"""
{base_prompt}

## Issue to Analyze

**Title:** {issue_details['title']}

**Body:**
{issue_details['body']}

**Current Labels:** {', '.join(label['name'] for label in issue_details.get('labels', []))}

---

Please analyze this scrape failure issue and provide your assessment following the output format specified above.
"""

    # Write prompt to temp file for Claude Code
    prompt_path = Path("/tmp/claude_prompt.md")
    with open(prompt_path, "w") as f:
        f.write(full_prompt)

    # Run Claude Code
    # Using --print to get output directly, --dangerously-skip-permissions for CI
    result = subprocess.run(
        [
            "claude",
            "--print",
            "--dangerously-skip-permissions",
            "-p", str(prompt_path),
        ],
        capture_output=True,
        text=True,
        timeout=300,  # 5 minute timeout
    )

    if result.returncode != 0:
        print(f"Claude error: {result.stderr}", file=sys.stderr)
        return None

    return result.stdout


def parse_analysis_json(analysis_text: str) -> dict | None:
    """Extract the JSON analysis from Claude's output."""
    # Look for ANALYSIS_JSON: marker
    match = re.search(r"ANALYSIS_JSON:\s*```json?\s*({.*?})\s*```", analysis_text, re.DOTALL)
    if not match:
        # Try without code block
        match = re.search(r"ANALYSIS_JSON:\s*({.*?})\s*(?:$|---)", analysis_text, re.DOTALL)

    if not match:
        return None

    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def add_comment(issue_number: int, body: str, repo: str | None = None) -> bool:
    """Add a comment to the issue."""
    args = ["gh", "issue", "comment", str(issue_number), "--body", body]
    if repo:
        args.extend(["--repo", repo])

    result = subprocess.run(args, capture_output=True, text=True)
    return result.returncode == 0


def update_labels(issue_number: int, analysis: dict, repo: str | None = None) -> bool:
    """Update issue labels based on analysis."""
    category = analysis.get("category", "")
    auto_fixable = analysis.get("auto_fixable", False)
    confidence = analysis.get("confidence", 0)

    labels_to_add = []
    labels_to_remove = ["ai-triage"]  # Remove triage label after analysis

    # Add category-specific label
    if category in ("url-update", "registry-update", "encoding-fix"):
        labels_to_add.append(category)

    # Add auto-fix label if appropriate
    if auto_fixable and confidence >= 0.7:
        labels_to_add.append("auto-fix-ready")
    elif not auto_fixable or confidence < 0.5:
        labels_to_add.append("needs-human")

    # Remove old triage label
    for label in labels_to_remove:
        args = ["gh", "issue", "edit", str(issue_number), "--remove-label", label]
        if repo:
            args.extend(["--repo", repo])
        subprocess.run(args, capture_output=True)

    # Add new labels
    for label in labels_to_add:
        args = ["gh", "issue", "edit", str(issue_number), "--add-label", label]
        if repo:
            args.extend(["--repo", repo])
        subprocess.run(args, capture_output=True)

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Analyze a failure issue with Claude and post the result"
    )
    parser.add_argument(
        "--issue",
        type=int,
        required=True,
        help="GitHub issue number to analyze",
    )
    parser.add_argument(
        "--repo",
        help="GitHub repository (owner/repo)",
    )
    parser.add_argument(
        "--prompt-file",
        type=Path,
        default=Path("scripts/ci/prompts/analyze_failure.md"),
        help="Path to the analysis prompt file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print analysis without posting comment",
    )
    parser.add_argument(
        "--skip-labels",
        action="store_true",
        help="Don't update labels based on analysis",
    )

    args = parser.parse_args()

    # Check for ANTHROPIC_API_KEY
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("Error: ANTHROPIC_API_KEY environment variable not set", file=sys.stderr)
        sys.exit(1)

    # Get issue details
    print(f"Fetching issue #{args.issue}...", file=sys.stderr)
    issue_details = get_issue_details(args.issue, args.repo)
    if not issue_details:
        sys.exit(1)

    print(f"Issue: {issue_details['title']}", file=sys.stderr)

    # Run Claude analysis
    print("Running Claude analysis...", file=sys.stderr)
    analysis_text = run_claude_analysis(issue_details, args.prompt_file)
    if not analysis_text:
        print("Error: Claude analysis failed", file=sys.stderr)
        sys.exit(1)

    # Parse the JSON analysis
    analysis_json = parse_analysis_json(analysis_text)

    if args.dry_run:
        print("\n=== Analysis Output ===\n")
        print(analysis_text)
        if analysis_json:
            print("\n=== Parsed JSON ===\n")
            print(json.dumps(analysis_json, indent=2))
        return

    # Post the analysis as a comment
    comment_header = "## Claude Analysis\n\n"
    comment_body = comment_header + analysis_text

    print("Posting analysis comment...", file=sys.stderr)
    if not add_comment(args.issue, comment_body, args.repo):
        print("Error: Failed to post comment", file=sys.stderr)
        sys.exit(1)

    print(f"Posted analysis to issue #{args.issue}", file=sys.stderr)

    # Update labels based on analysis
    if not args.skip_labels and analysis_json:
        print("Updating labels...", file=sys.stderr)
        update_labels(args.issue, analysis_json, args.repo)
        print(f"Labels updated based on category: {analysis_json.get('category')}", file=sys.stderr)


if __name__ == "__main__":
    main()

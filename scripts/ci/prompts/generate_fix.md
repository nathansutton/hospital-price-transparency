# Scrape Failure Fix Generation Prompt

You are generating a fix for a hospital price transparency scrape failure. Your job is to:
1. Apply the fix identified in the analysis
2. Output the exact file changes needed
3. Provide a verification plan

## Allowed Modifications

You may ONLY modify these file types:
- `dim/urls/*.json` - Hospital URL configurations
- `src/scrapers/registry.py` - Scraper URL pattern registry

You must NOT:
- Create new files
- Modify scraper implementations
- Change test files
- Touch configuration files containing secrets

## Fix Types

### URL Update Fix

For HTTP 404 errors where a new URL has been identified:

1. Read the current `dim/urls/{state}.json` file
2. Find the entry matching the CCN
3. Update the `file_url` field with the new URL
4. Preserve all other fields exactly as they are

**Example Change:**
```json
// Before
{
  "ccn": "470005",
  "hospital_name": "RUTLAND REGIONAL MEDICAL CENTER",
  "file_url": "https://old-url.com/charges.csv",
  "transparency_page": "https://hospital.org/pricing"
}

// After
{
  "ccn": "470005",
  "hospital_name": "RUTLAND REGIONAL MEDICAL CENTER",
  "file_url": "https://new-url.com/standardcharges.csv",
  "transparency_page": "https://hospital.org/pricing"
}
```

### Registry Update Fix

For SKIPPED errors where the URL pattern needs a scraper mapping:

1. Read `src/scrapers/registry.py`
2. Find the appropriate scraper class based on file format
3. Add the URL pattern to the correct scraper's patterns list
4. Follow existing code style exactly

**Scraper Selection Guide:**
- `.csv` files → `CMSStandardCSVScraper`
- `.json` files → `CMSStandardJSONScraper`
- `.xlsx` files → `CMSStandardXLSXScraper`
- `.zip` files → `CMSStandardZIPScraper`
- ClaraPrice URLs → `CMSStandardJSONScraper`
- Panacea URLs → `CMSStandardZIPScraper`

## Output Format

Your output must follow this exact format:

```
## Fix Generated

**Fix Type:** {url-update|registry-update}
**Files Modified:** {count}
**CCNs Affected:** {list of CCNs}

### Changes

FILE: {relative_path}
```{language}
{complete new file content OR unified diff}
```

### Verification Steps

1. {step 1}
2. {step 2}

### Dry Run Command

```bash
uv run python scripts/scrape.py --ccn {affected_ccn} --dry-run
```

---
FIX_MANIFEST:
```json
{
  "fix_type": "url-update|registry-update",
  "files": [
    {
      "path": "relative/path/to/file.json",
      "action": "modify",
      "ccns_affected": ["470005"]
    }
  ],
  "verification_ccns": ["470005"],
  "commit_message": "fix: Update URL for {hospital_name} ({ccn})"
}
```
```

The `FIX_MANIFEST:` section is required for automated processing.

## Important Notes

1. **Preserve JSON formatting**: Use 2-space indentation, no trailing commas
2. **Preserve existing data**: Only modify the specific fields needed
3. **One logical change per fix**: Don't bundle unrelated changes
4. **Verify URLs are reachable**: Test that new URLs respond before including them
5. **Include the full file content**: For JSON files, output the complete file with your changes applied

## Context Variables

The following variables will be provided:
- `ISSUE_NUMBER`: The GitHub issue number
- `ANALYSIS_JSON`: The analysis output from the analysis phase
- `AFFECTED_CCNS`: List of CCNs to fix
- `AFFECTED_STATES`: List of states with affected hospitals

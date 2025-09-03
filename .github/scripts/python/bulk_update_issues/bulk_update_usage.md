# Bulk Update Issues Script Usage

This Python script (`bulk_update_issues.py`) converts the bash `run_bulk_update` function to use asyncio and httpx for better performance and concurrency control.

## Features

- **Asyncio-based**: Uses Python asyncio for concurrent processing
- **Rate limiting**: Implements exponential backoff when hitting GitHub API rate limits
- **Semaphore control**: Limits concurrent requests to 5 (configurable)
- **Error handling**: Robust error handling and retry logic
- **Progress tracking**: Shows detailed progress and statistics

## Dependencies

Install the required dependencies:

```bash
pip install -r requirements-bulk-update.txt
```

Or install httpx directly:

```bash
pip install httpx>=0.24.0
```

## Usage

1. **Set environment variables:**
   ```bash
   export GITHUB_TOKEN="your_github_token_here"
   export GITHUB_REPOSITORY="tenstorrent/tt-mlir"  # optional, defaults to this
   ```

2. **Create issue numbers file:**
   Create `/tmp/issue_numbers.txt` with one issue number per line:
   ```
   1234
   1235
   1236
   ```

3. **Run the script:**
   ```bash
   ./bulk_update_issues.py
   ```

## How it works

The script:

1. **Issue Discovery:**
   - First tries to read issue numbers from `/tmp/issue_numbers.txt`
   - If no file exists, automatically fetches all open issues from the repository using the async generator
   - Saves fetched issue numbers to the file for future runs

2. **Processing (with max 5 concurrent):**
   - Gets issue details from GitHub API
   - Finds the project item ID using GraphQL pagination
   - Checks the Status and Work Started fields
   - Updates Work Started field if Status is "In Progress" and Work Started is empty
   - Caches processed issue numbers to avoid re-processing

3. **Resilience:**
   - Uses exponential backoff when rate limited
   - Handles file not found errors gracefully
   - Provides detailed statistics at completion

## New Features

### Async Generator for Repository Issues

The script now includes `get_all_repository_issues()` - an async generator function that:
- Fetches all open issues from a GitHub repository with pagination
- Yields issue numbers one by one for memory efficiency
- Handles GitHub API pagination automatically (up to 50 pages / 5000 issues)
- Can be used independently for other scripts

**Usage example:**
```python
async with httpx.AsyncClient() as client:
    async for issue_number in get_all_repository_issues(client, token, "owner/repo"):
        print(f"Found issue #{issue_number}")
```

### Automatic Issue Discovery

- If `/tmp/issue_numbers.txt` doesn't exist, the script automatically fetches all open issues
- Saves the fetched issue numbers to the file for future runs
- No manual file creation needed for first-time use

## Configuration

The script uses these GitHub project IDs (hardcoded to match the workflow):
- **Project ID**: `PVT_kwDOA9MHEM4AjeTl`
- **Work Started Field ID**: `PVTF_lADOA9MHEM4AjeTlzgzZQtk`
- **Max Concurrent**: 5 (controlled by semaphore)

## Output Example

```
No existing cache file found, starting fresh
Loaded 150 issue numbers from /tmp/issue_numbers.txt
Starting bulk update for 150 issues with max 5 concurrent requests
Processing issue #1234...
Issue #1234 has Status='In Progress' but no Work Started date - updating...
Using status change date: 2024-01-15
✅ Updated Work Started for issue #1234 to 2024-01-15
Processing issue #1235...
Issue #1235: Status='Done', Work Started='2024-01-10' - no update needed
Saving last processed issue number 1236 to cache file
...
==================================================
BULK UPDATE COMPLETED
==================================================
Total issues processed: 150
Issues updated: 23
Errors encountered: 2
Duration: 45.67 seconds
==================================================
```

### First Run (No File) Example

```
No existing cache file found, starting fresh
No issue numbers found in file, fetching all open issues from repository...
Fetching all open issues from repository tenstorrent/tt-mlir...
Fetching page 1 of issues...
Found 100 issues on page 1
Fetching page 2 of issues...
Found 50 issues on page 2
Last page reached (less than 100 issues)
Saving 150 issue numbers to /tmp/issue_numbers.txt
Starting bulk update for 150 issues with max 5 concurrent requests
...
```

## Rate Limiting

The script handles GitHub API rate limits automatically:
- Detects rate limit responses (403, 429)
- Uses exponential backoff (120-180 seconds base + exponential factor)
- Automatically retries failed requests
- Respects retry-after headers when provided

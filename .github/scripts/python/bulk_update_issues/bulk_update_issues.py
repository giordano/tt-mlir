#!/usr/bin/env python3
"""
Bulk update script for GitHub project issues Work Started field.
Converts the bash run_bulk_update function to Python using asyncio and httpx.
"""

import asyncio
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import List, Optional, Dict, Any, AsyncGenerator

import httpx

processed_issue_numbers = {}

# Global debug mode flag
DEBUG_MODE = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")

def print_flush(*args, **kwargs):
    """Print with immediate flush for GitHub Actions visibility."""
    print(*args, **kwargs)
    sys.stdout.flush()

def debug_print(*args, **kwargs):
    """Print debug messages only when DEBUG_MODE is enabled."""
    if DEBUG_MODE:
        print_flush(*args, **kwargs)


async def track_progress(updater, total_issues: int, start_time: float):
    """Track and report progress during concurrent processing."""
    while True:
        await asyncio.sleep(30)  # Report progress every 30 seconds
        elapsed = time.time() - start_time
        processed = updater.processed_count + updater.error_count
        
        if processed > 0:
            rate = processed / elapsed
            estimated_remaining = (total_issues - processed) / rate if rate > 0 else 0
            print_flush(f"📊 Progress: {processed}/{total_issues} issues processed "
                       f"({processed/total_issues*100:.1f}%) - "
                       f"Rate: {rate:.1f}/sec - "
                       f"ETA: {estimated_remaining/60:.1f}min - "
                       f"✅{updater.processed_count} ❌{updater.error_count} 🔄{updater.updated_count}")
        else:
            print_flush(f"📊 Progress: Waiting for first issue to complete... ({elapsed:.1f}s elapsed)")
            
        if processed >= total_issues:
            break


class GitHubProjectUpdater:
    """Handles bulk updates of GitHub project issue fields with rate limiting."""
    
    def __init__(self, token: str, repository: str, project_id: str, 
                 work_started_field_id: str, client: httpx.AsyncClient, max_concurrent: int = 5):
        self.token = token
        self.repository = repository
        self.project_id = project_id
        self.work_started_field_id = work_started_field_id
        self.client = client
        self.max_concurrent = max_concurrent
        
        # API endpoints
        self.github_api_url = "https://api.github.com"
        self.graphql_url = f"{self.github_api_url}/graphql"
        
        # Headers
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        
        # Statistics
        self.processed_count = 0
        self.updated_count = 0
        self.error_count = 0
        
    async def make_api_request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Make API request with infinite retry for rate limits, limited retry for other errors."""
        debug_print(f"   🌐 Making {method} request to {url.replace(self.github_api_url, 'GitHub API')}")
        
        attempt = 0
        max_non_rate_limit_attempts = 5
        
        while True:
            attempt += 1
            
            try:
                response = await self.client.request(method, url, **kwargs)
                
                # Check for rate limiting - retry infinitely
                if response.status_code == 403:
                    try:
                        error_data = response.json()
                        if "API rate limit exceeded" in str(error_data):
                            wait_time = 60 + random.randint(30, 120)  # 60-180 seconds
                            print_flush(f"🛑 Rate limited (403): Retrying in {wait_time}s (attempt #{attempt})")
                            await asyncio.sleep(wait_time)
                            continue  # Infinite retry for rate limits
                    except Exception:
                        pass
                        
                if response.status_code == 429:
                    retry_after = response.headers.get('retry-after')
                    if retry_after:
                        wait_time = int(retry_after) + random.randint(10, 30)
                    else:
                        wait_time = 60 + random.randint(30, 120)
                        
                    print_flush(f"🛑 Rate limited (429): Retrying in {wait_time}s (attempt #{attempt})")
                    await asyncio.sleep(wait_time)
                    continue  # Infinite retry for rate limits
                    
                # Check for other HTTP errors - limited retry
                if response.status_code >= 400:
                    if attempt < max_non_rate_limit_attempts:
                        wait_time = (2 ** attempt) + random.randint(1, 10)
                        print_flush(f"❌ HTTP {response.status_code}: Retrying in {wait_time}s (attempt {attempt}/{max_non_rate_limit_attempts})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        print_flush(f"❌ HTTP {response.status_code} after {max_non_rate_limit_attempts} attempts: {response.text}")
                        response.raise_for_status()
                        
                return response
                
            except httpx.RequestError as e:
                if attempt < max_non_rate_limit_attempts:
                    wait_time = (2 ** attempt) + random.randint(1, 10)
                    print_flush(f"❌ Request error: {e}, retrying in {wait_time}s (attempt {attempt}/{max_non_rate_limit_attempts})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    print_flush(f"❌ Request error after {max_non_rate_limit_attempts} attempts: {e}")
                    raise
    
    async def make_graphql_request_with_infinite_retry(self, query: str, variables: dict) -> dict:
        """Make GraphQL request with infinite retry for rate limits."""
        payload = {
            "query": query,
            "variables": variables
        }
        
        attempt = 0
        while True:
            attempt += 1
            
            try:
                response = await self.make_api_request(
                    "POST", self.graphql_url, 
                    headers=self.headers, json=payload
                )
                
                if response.status_code != 200:
                    print_flush(f"❌ GraphQL HTTP {response.status_code}: {response.text}")
                    return {"errors": [{"message": f"HTTP {response.status_code}"}]}
                
                data = response.json()
                
                # Check for GraphQL rate limit errors - retry infinitely
                if data.get("errors"):
                    for error in data['errors']:
                        if error.get('type') == 'RATE_LIMITED':
                            wait_time = 60 + random.randint(30, 120)
                            print_flush(f"🛑 GraphQL Rate Limited: Retrying in {wait_time}s (attempt #{attempt})")
                            await asyncio.sleep(wait_time)
                            continue  # Continue the retry loop for rate limit
                        else:
                            break
                
                return data
                
            except Exception as e:
                # For other exceptions, let them propagate up
                print_flush(f"❌ GraphQL request exception: {e}")
                raise
    
    async def get_issue_details(self, issue_number: int) -> Optional[Dict[str, Any]]:
        """Get issue details including node_id from GitHub API."""
        url = f"{self.github_api_url}/repos/{self.repository}/issues/{issue_number}"
        
        try:
            response = await self.make_api_request("GET", url, headers=self.headers)
            return response.json()
        except Exception as e:
            print_flush(f"   ❌ Failed to get issue #{issue_number} details: {e}")
            return None
    
    async def find_project_item_id(self, issue_id: str) -> Optional[str]:
        """Find project item ID for an issue using GraphQL with pagination (matching bash workflow)."""
        # Validate issue_id format
        if not issue_id:
            print_flush(f"   ❌ Empty issue_id provided")
            return None
             
        cursor = None
        page_count = 0
        max_pages = 100  # Safety limit like the bash version
        
        while page_count < max_pages:
            page_count += 1
            debug_print(f"   🔍 Searching project page {page_count}...")
            
            # Build query with or without cursor
            if cursor is None:
                query = """
                query($projectId: ID!) {
                  node(id: $projectId) {
                    ... on ProjectV2 {
                      items(first: 100) {
                        pageInfo {
                          hasNextPage
                          endCursor
                        }
                        nodes {
                          id
                          content {
                            ... on Issue {
                              id
                            }
                          }
                        }
                      }
                    }
                  }
                }
                """
                variables = {"projectId": self.project_id}
            else:
                query = """
                query($projectId: ID!, $cursor: String!) {
                  node(id: $projectId) {
                    ... on ProjectV2 {
                      items(first: 100, after: $cursor) {
                        pageInfo {
                          hasNextPage
                          endCursor
                        }
                        nodes {
                          id
                          content {
                            ... on Issue {
                              id
                            }
                          }
                        }
                      }
                    }
                  }
                }
                """
                variables = {"projectId": self.project_id, "cursor": cursor}
            
            try:
                data = await self.make_graphql_request_with_infinite_retry(query, variables)
                
                # Check for GraphQL errors (rate limits already handled by the wrapper)
                if data.get("errors"):
                    errors = data['errors']
                    for error in errors:
                        if error.get('type') == 'FORBIDDEN':
                            print_flush("\n❌ PERMISSION ERROR: GitHub token doesn't have project access.")
                            print_flush("💡 Use a Personal Access Token with 'project' and 'repo' scopes")
                            return None
                            
                    print_flush(f"GraphQL error finding project item: {errors}")
                    return None
                    
                # Navigate the response structure safely with None checks
                data_section = data.get("data")
                if not data_section:
                    print_flush(f"   ❌ No data section in GraphQL response for page {page_count}")
                    return None
                
                project_data = data_section.get("node")
                if not project_data:
                    print_flush(f"   ❌ No project node data for page {page_count}")
                    return None
                
                items_data = project_data.get("items")
                if not items_data:
                    print_flush(f"   ❌ No items data for page {page_count}")
                    return None
                
                items = items_data.get("nodes") or []
                page_info = items_data.get("pageInfo") or {}
                
                # Look for the issue in current batch (matching bash logic)
                for item in items:
                    if not item:  # Skip None items
                        continue
                    content = item.get("content")
                    if not content:  # Skip items without content
                        continue
                    if content.get("id") == issue_id:
                        debug_print(f"   ✅ Found issue in project page {page_count}")
                        return item.get("id")
                
                # Check if there are more pages (matching bash logic)
                has_next_page = page_info.get("hasNextPage", False)
                if not has_next_page:
                    debug_print(f"   📄 Searched all {page_count} pages. No more pages available.")
                    break
                
                # Get cursor for next page
                cursor = page_info.get("endCursor")
                debug_print(f"   ➡️ Moving to next page with cursor: {cursor}")
                
            except Exception as e:
                print_flush(f"   ❌ Error finding project item for issue {issue_id} on page {page_count}: {e}")
                return None
        
        if page_count >= max_pages:
            print_flush(f"   ⚠️ Searched {max_pages} pages (10,000+ items). Stopping to prevent infinite loop.")
            
        print_flush(f"   ⚠️ Issue {issue_id} not found in project after searching {page_count} pages.")
        print_flush(f"   💡 The issue may not be added to this project yet.")
        return None
    
    async def get_issue_field_values(self, item_id: str) -> Optional[Dict[str, Any]]:
        """Get issue field values including Status and Work Started."""
        query = """
        query($itemId: ID!) {
          node(id: $itemId) {
            ... on ProjectV2Item {
              fieldValues(first: 20) {
                nodes {
                  ... on ProjectV2ItemFieldSingleSelectValue {
                    field {
                      ... on ProjectV2Field {
                        id
                        name
                      }
                    }
                    name
                    optionId
                    updatedAt
                  }
                  ... on ProjectV2ItemFieldDateValue {
                    field {
                      ... on ProjectV2Field {
                        id
                        name
                      }
                    }
                    date
                    updatedAt
                  }
                }
              }
            }
          }
        }
        """
        
        variables = {"itemId": item_id}
        
        try:
            data = await self.make_graphql_request_with_infinite_retry(query, variables)
            
            # Check for GraphQL errors (rate limits already handled by the wrapper)
            if data.get("errors"):
                errors = data['errors']
                print_flush(f"GraphQL error getting field values: {errors}")
                return None
                
            # Navigate response structure
            item_data = data.get("data", {}).get("node", {})
            field_values = item_data.get("fieldValues", {}).get("nodes", [])
            
            # Extract field values with timestamps (matching workflow logic)
            fields = {}
            status_info = {}
            work_started_info = {}
            
            for field_value in field_values:
                if not field_value:
                    continue
                    
                field_info = field_value.get("field", {})
                field_name = field_info.get("name")
                field_id = field_info.get("id")
                
                if field_name:
                    if "name" in field_value:  # Single select field (Status)
                        status_value = field_value["name"]
                        # Check if this is a known Status value (matching workflow)
                        if status_value in ["In Progress", "Assigned", "Screen", "Blocked", "Done", "In Review"]:
                            fields[field_name] = status_value
                            status_info = {
                                "value": status_value,
                                "updatedAt": field_value.get("updatedAt"),
                                "fieldId": field_id
                            }
                    elif "date" in field_value:  # Date field (Work Started)
                        # Check both field name and field ID (matching workflow)
                        if field_name == "Work Started" or field_id == self.work_started_field_id:
                            fields[field_name] = field_value["date"]
                            work_started_info = {
                                "value": field_value["date"],
                                "updatedAt": field_value.get("updatedAt"),
                                "fieldId": field_id
                            }
            
            # Add metadata for date calculation logic
            fields["_status_info"] = status_info
            fields["_work_started_info"] = work_started_info
                        
            return fields
            
        except Exception as e:
            print_flush(f"   ❌ Error getting field values for item {item_id}: {e}")
            return None
    
    async def update_work_started_field(self, item_id: str, date_value: str) -> bool:
        """Update Work Started field for a project item."""
        mutation = """
        mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $value: ProjectV2FieldDateValue!) {
          updateProjectV2ItemFieldValue(
            input: {
              projectId: $projectId
              itemId: $itemId
              fieldId: $fieldId
              value: $value
            }
          ) {
            clientMutationId
          }
        }
        """
        
        variables = {
            "projectId": self.project_id,
            "itemId": item_id,
            "fieldId": self.work_started_field_id,
            "value": {"date": date_value}
        }
        
        try:
            data = await self.make_graphql_request_with_infinite_retry(mutation, variables)
            
            # Check for GraphQL errors (rate limits already handled by the wrapper)
            if data.get("errors"):
                errors = data['errors']
                print_flush(f"GraphQL error updating Work Started field: {errors}")
                return False
                
            # Check for successful mutation
            mutation_data = data.get("data", {}).get("updateProjectV2ItemFieldValue")
            if mutation_data:
                debug_print(f"   ✅ Updated Work Started field")
                return True
            else:
                print_flush(f"   ❌ No mutation data in update response")
                return False
                
        except Exception as e:
            print_flush(f"   ❌ Error updating Work Started field for item {item_id}: {e}")
            return False
    
    async def process_issue_with_infinite_retry(self, issue_number: int) -> None:
        """Process a single issue with infinite retry until it succeeds."""
        attempt = 0
        
        while True:
            attempt += 1
            
            try:
                await self.process_issue(issue_number)
                return  # Success, exit the retry loop
                
            except Exception as e:
                wait_time = min(30 + (attempt * 10), 300) + random.randint(5, 15)  # Cap at 5 minutes
                print_flush(f"⚠️ Issue #{issue_number} failed (attempt #{attempt}): {e}")
                print_flush(f"⏰ Retrying issue #{issue_number} in {wait_time}s...")
                await asyncio.sleep(wait_time)
    
    async def process_issue(self, issue_number: int) -> None:
        """Process a single issue: check status and update Work Started if needed."""
        print_flush(f"🔄 Processing issue #{issue_number}...")
        
        # Step 1: Get issue details
        debug_print(f"   📋 Getting issue details...")
        issue_data = await self.get_issue_details(issue_number)
        
        if not issue_data:
            raise Exception(f"Failed to get issue #{issue_number} details")
            
        issue_id = issue_data.get("node_id")
        if not issue_id:
            raise Exception(f"No node_id found for issue #{issue_number}")
            
        debug_print(f"   ✅ Got issue details (ID: {issue_id})")
        
        # Step 2: Find project item ID
        debug_print(f"   🔍 Finding project item...")
        item_id = await self.find_project_item_id(issue_id)
        
        if not item_id:
            print_flush(f"   ⚠️ Issue #{issue_number} not found in project, skipping")
            self.processed_count += 1
            return
            
        debug_print(f"   ✅ Found project item (ID: {item_id})")
        
        # Step 3: Get current field values
        debug_print(f"   📊 Getting field values...")
        field_values = await self.get_issue_field_values(item_id)
        
        if field_values is None:
            raise Exception(f"Failed to get field values for issue #{issue_number}")
            
        status = field_values.get("Status", "")
        work_started = field_values.get("Work Started", "")
        status_info = field_values.get("_status_info", {})
        work_started_info = field_values.get("_work_started_info", {})
        
        print_flush(f"   📊 Status: '{status}', Work Started: '{work_started}'")
        
        # Step 4: Update Work Started if needed (matching workflow logic exactly)
        # Condition: Status is "In Progress" AND Work Started is empty/null
        if status == "In Progress" and (not work_started or work_started == "null"):
            debug_print(f"   🔧 Updating Work Started field...")
            
            # Use the date when status changed to 'In Progress', with fallback to current date (matching workflow)
            status_updated_at = status_info.get("updatedAt")
            if status_updated_at and status_updated_at != "null":
                # Extract date part from timestamp (YYYY-MM-DD)
                work_started_date = status_updated_at.split('T')[0]
                print_flush(f"   📅 Using status change date: {work_started_date} (from timestamp: {status_updated_at})")
            else:
                # Fallback to current date
                work_started_date = time.strftime("%Y-%m-%d")
                print_flush(f"   📅 Could not determine status change date, using current date: {work_started_date}")
            
            success = await self.update_work_started_field(item_id, work_started_date)
            
            if success:
                self.updated_count += 1
                print_flush(f"   ✅ Updated Work Started for issue #{issue_number} to {work_started_date} (date when status changed)")
            else:
                raise Exception(f"Failed to update issue #{issue_number}")
        else:
            print_flush(f"   ⏭️ Issue #{issue_number}: Status='{status}', Work Started='{work_started}' - no update needed")
            
        self.processed_count += 1
        
        # Add rate limiting delay like workflow (1 second between issues)
        await asyncio.sleep(1)


async def get_all_repository_issues(client: httpx.AsyncClient, token: str, repositories: List[str]) -> AsyncGenerator[tuple, None]:
    """
    Fetch all open issues from repositories using pagination.
    
    Args:
        client: HTTP client
        token: GitHub token
        repositories: List of repository names (e.g., ["owner/repo"])
        
    Yields:
        (repo_name, list_of_issue_numbers) tuples
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    for repo in repositories:
        print_flush(f"Fetching all open issues from repository {repo}...")
        all_issue_numbers = []
        
        for page in range(1, 51):  # Max 50 pages like workflow
            print_flush(f"   Fetching page {page} of issues...")
            url = f"https://api.github.com/repos/{repo}/issues"
            params = {
                "state": "open",
                "per_page": 100,
                "page": page
            }
            
            try:
                # Simple retry with infinite retry for rate limits
                attempt = 0
                max_attempts = 5
                
                while True:
                    attempt += 1
                    
                    try:
                        response = await client.get(url, headers=headers, params=params)
                        
                        # Handle rate limiting - retry infinitely
                        if response.status_code == 429:
                            retry_after = response.headers.get('retry-after')
                            wait_time = int(retry_after) + random.randint(10, 30) if retry_after else 60 + random.randint(30, 120)
                            print_flush(f"🛑 Rate limited fetching issues: Retrying in {wait_time}s (attempt #{attempt})")
                            await asyncio.sleep(wait_time)
                            continue  # Infinite retry for rate limits
                            
                        if response.status_code == 403:
                            try:
                                error_data = response.json()
                                if "API rate limit exceeded" in str(error_data):
                                    wait_time = 60 + random.randint(30, 120)
                                    print_flush(f"🛑 Rate limited (403) fetching issues: Retrying in {wait_time}s (attempt #{attempt})")
                                    await asyncio.sleep(wait_time)
                                    continue  # Infinite retry for rate limits
                            except Exception:
                                pass
                                
                        # For other errors, limited retry
                        if response.status_code >= 400:
                            if attempt < max_attempts:
                                wait_time = (2 ** attempt) + random.randint(1, 10)
                                print_flush(f"❌ HTTP {response.status_code} fetching issues: Retrying in {wait_time}s (attempt {attempt}/{max_attempts})")
                                await asyncio.sleep(wait_time)
                                continue
                        
                        break  # Success or non-retryable error
                        
                    except httpx.RequestError as e:
                        if attempt < max_attempts:
                            wait_time = (2 ** attempt) + random.randint(1, 10)
                            print_flush(f"❌ Request error fetching issues: {e}, retrying in {wait_time}s (attempt {attempt}/{max_attempts})")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            raise
                
                if response.status_code != 200:
                    print_flush(f"   ❌ Error fetching issues page {page}: {response.status_code}")
                    break
                    
                issues = response.json()
                page_issue_count = len(issues)
                
                if page_issue_count == 0:
                    print_flush(f"   ✅ No more issues on page {page}")
                    break
                    
                page_issue_numbers = [issue["number"] for issue in issues]
                all_issue_numbers.extend(page_issue_numbers)
                print_flush(f"   ✅ Found {page_issue_count} issues on page {page}")
                
                if page_issue_count < 100:  # Last page
                    print_flush(f"   ✅ Last page reached")
                    break
                    
            except Exception as e:
                print_flush(f"   ❌ Error fetching issues page {page}: {e}")
                break
                
        print_flush(f"✅ Found {len(all_issue_numbers)} total issues in {repo}")
        yield (repo, all_issue_numbers)


async def main():
    """Main function that orchestrates the bulk update process."""
    print_flush("🚀 Starting GitHub Project Issue Bulk Update")
    print_flush("=" * 60)
    
    # Configuration from environment variables
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print_flush("❌ ERROR: GITHUB_TOKEN environment variable is required")
        sys.exit(1)
         
    repositories = ["tenstorrent/tt-mlir"]
    project_id = os.getenv("project_id")
    work_started_field_id = os.getenv("work_started_field_id")
    max_concurrent = int(os.getenv("MAX_CONCURRENT", "5"))
    
    if not project_id:
        print_flush("❌ ERROR: PROJECT_ID environment variable is required")
        sys.exit(1)
        
    if not work_started_field_id:
        print_flush("❌ ERROR: WORK_STARTED_FIELD_ID environment variable is required")
        sys.exit(1)
    
    print_flush(f"🔧 Configuration:")
    print_flush(f"   Repository: {repository}")
    print_flush(f"   Project ID: {project_id}")
    print_flush(f"   Work Started Field ID: {work_started_field_id}")
    print_flush(f"   Max Concurrent: {max_concurrent}")
    print_flush(f"   Debug Mode: {DEBUG_MODE}")
    print_flush(f"   Token Type: {'PAT' if token.startswith('ghp_') else 'Other'}")
    
    # Try to read cached issue numbers first
    cache_file = Path("/tmp/processed_issue_numbers.json")
    
    if cache_file.exists():
        print_flush(f"📂 Loading cached issue numbers from {cache_file}")
        try:
            with open(cache_file, 'r') as f:
                processed_issue_numbers = json.load(f)
            print_flush(f"✅ Loaded {len(processed_issue_numbers)} processed issue numbers from cache")
        except Exception as e:
            print_flush(f"⚠️ Error reading cache file: {e}")
            processed_issue_numbers = []
    else:
        processed_issue_numbers = []
    
    
    semaphore = asyncio.Semaphore(max_concurrent)

    print_flush(f"⚡ Starting concurrent processing with max {max_concurrent} simultaneous requests...")
    start_time = time.time()

    timeout = httpx.Timeout(300.0, connect=60.0)  # Much higher timeouts to avoid network interference
    async with httpx.AsyncClient(timeout=timeout) as client:
        async for repo_name, repo_issue_numbers in get_all_repository_issues(client, token, repositories):
            print_flush(f"📊 Processing {len(repo_issue_numbers)} issues from repository: {repo_name}")
            
            # Filter out already processed issues
            issues_to_process = [issue for issue in repo_issue_numbers if issue not in processed_issue_numbers]
            if len(issues_to_process) != len(repo_issue_numbers):
                print_flush(f"📂 Skipping {len(repo_issue_numbers) - len(issues_to_process)} already processed issues")
            
            if not issues_to_process:
                print_flush(f"✅ All issues in {repo_name} already processed, skipping")
                continue
                
            # Create updater instance for this repository
            updater = GitHubProjectUpdater(
                token=token,
                repository=repo_name,
                project_id=project_id,
                work_started_field_id=work_started_field_id,
                client=client,
                max_concurrent=max_concurrent
            )
            
            print_flush(f"🔄 Creating {len(issues_to_process)} processing tasks...")
            
            async def process_with_semaphore(issue_number):
                async with semaphore:
                    await updater.process_issue_with_infinite_retry(issue_number)
                    # Track processed issue
                    processed_issue_numbers.append(issue_number)
            
            tasks = [process_with_semaphore(issue_number) for issue_number in issues_to_process]
            
            # Add progress tracking
            progress_task = asyncio.create_task(track_progress(updater, len(issues_to_process), start_time))
            
            # Process all issues
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                progress_task.cancel()
                try:
                    await progress_task
                except asyncio.CancelledError:
                    pass
            
            # Save progress after each repository
            print_flush(f"💾 Saving progress after processing {repo_name}")
            try:
                with open(cache_file, 'w') as f:
                    json.dump(processed_issue_numbers, f)
            except Exception as e:
                print_flush(f"⚠️ Error saving progress: {e}")
        
    # Final statistics
    elapsed = time.time() - start_time
    print_flush("=" * 60)
    print_flush("🏁 Bulk Update Complete!")
    print_flush(f"⏱️ Total Time: {elapsed:.1f} seconds")
    print_flush(f"📊 Total Issues Processed: {len(processed_issue_numbers)}")
    if len(processed_issue_numbers) > 0:
        print_flush(f"📈 Average Rate: {len(processed_issue_numbers) / elapsed:.2f} issues/sec")
    print_flush(f"💾 Progress saved to: {cache_file}")
    


if __name__ == "__main__":
    asyncio.run(main())
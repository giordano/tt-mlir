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
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

last_processed_issue_number = {}

def print_flush(*args, **kwargs):
    """Print with immediate flush for GitHub Actions visibility."""
    print(*args, **kwargs)
    sys.stdout.flush()


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
        
    @retry(
        stop=stop_after_attempt(10), 
        wait=wait_exponential(multiplier=1, min=30, max=300),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError))
    )
    async def make_api_request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Make API request with automatic retry using tenacity."""
        print_flush(f"   🌐 Making {method} request to {url.replace(self.github_api_url, 'GitHub API')}")
        
        response = await self.client.request(method, url, **kwargs)
        
        # Check for rate limiting
        if response.status_code == 403:
            try:
                error_data = response.json()
                if "API rate limit exceeded" in str(error_data):
                    print_flush(f"🛑 Rate limited (403): {error_data}")
                    response.raise_for_status()  # This will trigger retry
            except Exception:
                pass
                
        if response.status_code == 429:
            print_flush(f"🛑 Rate limited (429): {response.headers.get('retry-after', 'unknown')} seconds")
            response.raise_for_status()  # This will trigger retry
            
        # Check for other HTTP errors
        if response.status_code >= 400:
            print_flush(f"❌ HTTP {response.status_code}: {response.text}")
            response.raise_for_status()  # This will trigger retry for 4xx/5xx
            
        return response
    
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
        """Find project item ID for an issue using GraphQL."""
        query = """
        query($projectId: ID!, $issueId: ID!) {
          node(id: $projectId) {
            ... on ProjectV2 {
              items(first: 100) {
                nodes {
                  id
                  content {
                    ... on Issue {
                      id
                    }
                  }
                }
                pageInfo {
                  hasNextPage
                  endCursor
                }
              }
            }
          }
        }
        """
        
        variables = {
            "projectId": self.project_id,
            "issueId": issue_id
        }
        
        payload = {
            "query": query,
            "variables": variables
        }
        
        try:
            response = await self.make_api_request(
                "POST", self.graphql_url, 
                headers=self.headers, json=payload
            )
            
            data = response.json()
            
            # Check for GraphQL errors
            if data.get("errors"):
                errors = data['errors']
                for error in errors:
                    if error.get('type') == 'FORBIDDEN':
                        print_flush("\n❌ PERMISSION ERROR: GitHub token doesn't have project access.")
                        print_flush("💡 Use a Personal Access Token with 'project' and 'repo' scopes")
                        return None
                    elif error.get('type') == 'RATE_LIMITED':
                        print_flush(f"🛑 GraphQL Rate Limited: {errors}")
                        raise httpx.HTTPStatusError("GraphQL Rate Limited", request=None, response=response)
                        
                print_flush(f"GraphQL error finding project item: {errors}")
                return None
                
            # Navigate the response structure safely
            project_data = data.get("data", {}).get("node", {})
            items = project_data.get("items", {}).get("nodes", [])
            
            for item in items:
                content = item.get("content", {})
                if content.get("id") == issue_id:
                    return item.get("id")
                    
            print_flush(f"   ⚠️ Issue {issue_id} not found in project")
            return None
            
        except Exception as e:
            print_flush(f"   ❌ Error finding project item for issue {issue_id}: {e}")
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
                  }
                  ... on ProjectV2ItemFieldDateValue {
                    field {
                      ... on ProjectV2Field {
                        id
                        name
                      }
                    }
                    date
                  }
                }
              }
            }
          }
        }
        """
        
        variables = {"itemId": item_id}
        payload = {
            "query": query,
            "variables": variables
        }
        
        try:
            response = await self.make_api_request(
                "POST", self.graphql_url,
                headers=self.headers, json=payload
            )
            
            data = response.json()
            
            # Check for GraphQL errors
            if data.get("errors"):
                errors = data['errors']
                for error in errors:
                    if error.get('type') == 'RATE_LIMITED':
                        print_flush(f"🛑 GraphQL Rate Limited getting field values: {errors}")
                        raise httpx.HTTPStatusError("GraphQL Rate Limited", request=None, response=response)
                        
                print_flush(f"GraphQL error getting field values: {errors}")
                return None
                
            # Navigate response structure
            item_data = data.get("data", {}).get("node", {})
            field_values = item_data.get("fieldValues", {}).get("nodes", [])
            
            # Extract field values
            fields = {}
            for field_value in field_values:
                field_info = field_value.get("field", {})
                field_name = field_info.get("name")
                
                if field_name:
                    if "name" in field_value:  # Single select field
                        fields[field_name] = field_value["name"]
                    elif "date" in field_value:  # Date field
                        fields[field_name] = field_value["date"]
                        
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
        
        payload = {
            "query": mutation,
            "variables": variables
        }
        
        try:
            response = await self.make_api_request(
                "POST", self.graphql_url,
                headers=self.headers, json=payload
            )
            
            data = response.json()
            
            # Check for GraphQL errors
            if data.get("errors"):
                errors = data['errors']
                for error in errors:
                    if error.get('type') == 'RATE_LIMITED':
                        print_flush(f"🛑 GraphQL Rate Limited updating Work Started field: {errors}")
                        raise httpx.HTTPStatusError("GraphQL Rate Limited", request=None, response=response)
                        
                print_flush(f"GraphQL error updating Work Started field: {errors}")
                return False
                
            # Check for successful mutation
            mutation_data = data.get("data", {}).get("updateProjectV2ItemFieldValue")
            if mutation_data:
                print_flush(f"   ✅ Updated Work Started field")
                return True
            else:
                print_flush(f"   ❌ No mutation data in update response")
                return False
                
        except Exception as e:
            print_flush(f"   ❌ Error updating Work Started field for item {item_id}: {e}")
            return False
    
    async def process_issue(self, issue_number: int) -> None:
        """Process a single issue: check status and update Work Started if needed."""
        print_flush(f"🔄 Processing issue #{issue_number}...")
        
        try:
            # Step 1: Get issue details
            print_flush(f"   📋 Getting issue details...")
            issue_data = await asyncio.wait_for(
                self.get_issue_details(issue_number), timeout=60.0
            )
            
            if not issue_data:
                print_flush(f"   ❌ Failed to get issue #{issue_number} details")
                self.error_count += 1
                return
                
            issue_id = issue_data.get("node_id")
            if not issue_id:
                print_flush(f"   ❌ No node_id found for issue #{issue_number}")
                self.error_count += 1
                return
                
            print_flush(f"   ✅ Got issue details (ID: {issue_id})")
            
            # Step 2: Find project item ID
            print_flush(f"   🔍 Finding project item...")
            item_id = await asyncio.wait_for(
                self.find_project_item_id(issue_id), timeout=60.0
            )
            
            if not item_id:
                print_flush(f"   ⚠️ Issue #{issue_number} not found in project, skipping")
                self.processed_count += 1
                return
                
            print_flush(f"   ✅ Found project item (ID: {item_id})")
            
            # Step 3: Get current field values
            print_flush(f"   📊 Getting field values...")
            field_values = await asyncio.wait_for(
                self.get_issue_field_values(item_id), timeout=60.0
            )
            
            if field_values is None:
                print_flush(f"   ❌ Failed to get field values for issue #{issue_number}")
                self.error_count += 1
                return
                
            status = field_values.get("Status", "")
            work_started = field_values.get("Work Started", "")
            
            print_flush(f"   📊 Status: '{status}', Work Started: '{work_started}'")
            
            # Step 4: Update Work Started if needed
            if status == "In Progress" and not work_started:
                print_flush(f"   🔧 Updating Work Started field...")
                today = time.strftime("%Y-%m-%d")
                
                success = await asyncio.wait_for(
                    self.update_work_started_field(item_id, today), timeout=60.0
                )
                
                if success:
                    self.updated_count += 1
                    print_flush(f"   ✅ Issue #{issue_number} updated successfully")
                else:
                    self.error_count += 1
                    print_flush(f"   ❌ Failed to update issue #{issue_number}")
            else:
                print_flush(f"   ⏭️ Issue #{issue_number} doesn't need update")
                
            self.processed_count += 1
            
        except asyncio.TimeoutError:
            print_flush(f"   ⏰ Timeout processing issue #{issue_number}")
            self.error_count += 1
        except Exception as e:
            print_flush(f"   ❌ Unexpected error processing issue #{issue_number}: {e}")
            self.error_count += 1


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
                # Simple retry with tenacity for this API call too
                @retry(
                    stop=stop_after_attempt(3), 
                    wait=wait_exponential(multiplier=1, min=10, max=60)
                )
                async def fetch_issues_page():
                    response = await client.get(url, headers=headers, params=params)
                    if response.status_code == 429:
                        print_flush(f"🛑 Rate limited fetching issues, retrying...")
                        response.raise_for_status()
                    return response
                
                response = await fetch_issues_page()
                
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
        
    if token.startswith('ghs_'):
        print_flush("⚠️ WARNING: GITHUB_TOKEN appears to be a GitHub App token (ghs_)")
        print_flush("💡 For project access, consider using a Personal Access Token")
    
    repository = os.getenv("GITHUB_REPOSITORY", "tenstorrent/tt-mlir")
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
    print_flush(f"   Token Type: {'PAT' if token.startswith('ghp_') else 'Other'}")
    
    # Try to read cached issue numbers first
    cache_file = Path("/tmp/issue_numbers.txt")
    issue_numbers = []
    
    if cache_file.exists():
        print_flush(f"📂 Loading cached issue numbers from {cache_file}")
        try:
            with open(cache_file, 'r') as f:
                issue_numbers = [int(line.strip()) for line in f if line.strip().isdigit()]
            print_flush(f"✅ Loaded {len(issue_numbers)} issue numbers from cache")
        except Exception as e:
            print_flush(f"⚠️ Error reading cache file: {e}")
            issue_numbers = []
    
    # If no cached issues, fetch from repository
    if not issue_numbers:
        print_flush(f"🔍 No cached issues found, fetching from repository...")
        repositories = [repository]
        
        timeout = httpx.Timeout(60.0, connect=30.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async for repo_name, repo_issue_numbers in get_all_repository_issues(client, token, repositories):
                issue_numbers.extend(repo_issue_numbers)
                
        # Save to cache
        if issue_numbers:
            print_flush(f"💾 Saving {len(issue_numbers)} issue numbers to cache")
            with open(cache_file, 'w') as f:
                for issue_number in issue_numbers:
                    f.write(f"{issue_number}\n")
    
    if not issue_numbers:
        print_flush("❌ No issues found to process")
        sys.exit(1)
    
    print_flush(f"📊 Total issues to process: {len(issue_numbers)}")
    print_flush("=" * 60)
    
    # Process issues with concurrency control
    timeout = httpx.Timeout(60.0, connect=30.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # Create updater instance
        updater = GitHubProjectUpdater(
            token=token,
            repository=repository,
            project_id=project_id,
            work_started_field_id=work_started_field_id,
            client=client,
            max_concurrent=max_concurrent
        )
        
        print_flush(f"🔄 Creating {len(issue_numbers)} processing tasks...")
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_with_semaphore(issue_number):
            async with semaphore:
                await updater.process_issue(issue_number)
        
        tasks = [process_with_semaphore(issue_number) for issue_number in issue_numbers]
        
        print_flush(f"⚡ Starting concurrent processing with max {max_concurrent} simultaneous requests...")
        start_time = time.time()
        
        # Add progress tracking
        progress_task = asyncio.create_task(track_progress(updater, len(issue_numbers), start_time))
        
        # Process all issues
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass
        
        # Final statistics
        elapsed = time.time() - start_time
        print_flush("=" * 60)
        print_flush("🏁 Bulk Update Complete!")
        print_flush(f"⏱️ Total Time: {elapsed:.1f} seconds")
        print_flush(f"📊 Issues Processed: {updater.processed_count}")
        print_flush(f"🔄 Issues Updated: {updater.updated_count}")
        print_flush(f"❌ Issues with Errors: {updater.error_count}")
        if updater.processed_count > 0:
            print_flush(f"📈 Average Rate: {updater.processed_count / elapsed:.2f} issues/sec")


if __name__ == "__main__":
    asyncio.run(main())
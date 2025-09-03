#!/usr/bin/env python3
"""
Bulk update script for GitHub project issues Work Started field.
Converts the bash run_bulk_update function to Python using asyncio and httpx.
"""

import asyncio
import json
import random
import sys
import time
from pathlib import Path
from typing import List, Optional, Dict, Any, AsyncGenerator

import httpx

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
        """
        Initialize the updater.
        
        Args:
            token: GitHub token with project permissions
            repository: Repository in format "owner/repo"
            project_id: GitHub project v2 ID
            work_started_field_id: Field ID for Work Started field
            client: httpx AsyncClient for making requests
            max_concurrent: Maximum concurrent requests (default: 5)
        """
        self.token = token
        self.repository = repository
        self.project_id = project_id
        self.work_started_field_id = work_started_field_id
        self.client = client
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        self.github_api_url = "https://api.github.com"
        self.graphql_url = "https://api.github.com/graphql"
        
        # Statistics
        self.processed_count = 0
        self.updated_count = 0
        self.error_count = 0
        
    def _handle_graphql_errors(self, errors, context: str = "GraphQL operation") -> bool:
        """
        Handle GraphQL errors with specific permission error guidance.
        
        Args:
            errors: List of GraphQL errors
            context: Context of where the error occurred
            
        Returns:
            True if this is a recoverable error, False if it's a fatal permission error
        """
        print(f"GraphQL error in {context}: {errors}")
        
        # Check for permission issues
        for error in errors:
            if error.get('type') == 'FORBIDDEN':
                print("\n❌ PERMISSION ERROR: GitHub token doesn't have project access.")
                print("💡 SOLUTIONS:")
                print("   1. Use a Personal Access Token (PAT) instead of GITHUB_TOKEN")
                print("   2. Ensure token has 'project' and 'repo' scopes")
                print("   3. Add token owner as project collaborator")
                print("   4. In GitHub Actions, use secrets.TT_FORGE_PROJECT\n")
                return False  # Fatal permission error
                
        return True  # Other errors might be recoverable
        
    async def handle_rate_limit(self, response: httpx.Response) -> bool:
        """
        Handle GitHub API rate limiting with exponential backoff.
        
        Args:
            response: HTTP response to check for rate limiting
            
        Returns:
            True if should retry, False if no rate limiting detected
        """
        if response.status_code == 403:
            try:
                error_data = response.json()
                if "API rate limit exceeded" in str(error_data):
                    # Exponential backoff with jitter (120-180 seconds base)
                    base_sleep = random.randint(120, 180)
                    # Add exponential component based on current time
                    exp_factor = min(2 ** (self.error_count % 4), 8)
                    sleep_time = base_sleep * exp_factor + random.randint(0, 30)
                    
                    print_flush(f"🛑 RATE LIMITED (403): Sleeping for {sleep_time} seconds...")
                    print_flush(f"⏰ Will resume at approximately {time.strftime('%H:%M:%S', time.localtime(time.time() + sleep_time))}")
                    
                    # Add periodic updates during long sleeps
                    for i in range(0, sleep_time, 30):
                        remaining = sleep_time - i
                        if remaining > 30:
                            await asyncio.sleep(30)
                            print_flush(f"💤 Still waiting... {remaining - 30} seconds remaining")
                        else:
                            await asyncio.sleep(remaining)
                            break
                    
                    print_flush("⚡ Resuming after rate limit...")
                    return True
            except Exception:
                pass
                
        if response.status_code == 429:
            # Handle retry-after header if present
            retry_after = response.headers.get('retry-after')
            if retry_after:
                sleep_time = int(retry_after) + random.randint(10, 30)
                print_flush(f"🛑 RATE LIMITED (429): Server requested {retry_after}s wait, adding buffer -> {sleep_time}s")
            else:
                sleep_time = random.randint(60, 120)
                print_flush(f"🛑 RATE LIMITED (429): No retry-after header, using random backoff -> {sleep_time}s")
            
            print_flush(f"⏰ Will resume at approximately {time.strftime('%H:%M:%S', time.localtime(time.time() + sleep_time))}")
            
            # Add periodic updates during sleeps
            for i in range(0, sleep_time, 15):
                remaining = sleep_time - i
                if remaining > 15:
                    await asyncio.sleep(15)
                    print_flush(f"💤 Rate limit wait... {remaining - 15} seconds remaining")
                else:
                    await asyncio.sleep(remaining)
                    break
            
            print_flush("⚡ Resuming after rate limit...")
            return True
            
        return False
    
    async def make_request_with_retry(self, method: str, url: str, **kwargs) -> Optional[httpx.Response]:
        """
        Make HTTP request with automatic retry on rate limiting.
        
        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Additional request arguments
            
        Returns:
            Response object or None if all retries failed
        """
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = await self.client.request(method, url, **kwargs)
                
                if await self.handle_rate_limit(response):
                    retry_count += 1
                    continue
                    
                return response
                
            except Exception as e:
                print(f"Request error: {e}, retrying in {2 ** retry_count} seconds")
                await asyncio.sleep(2 ** retry_count)
                retry_count += 1
                
        print(f"Max retries exceeded for {method} {url}")
        return None
    
    async def get_issue_details(self, issue_number: int) -> Optional[Dict[str, Any]]:
        """
        Get issue details including node_id from GitHub API.
        
        Args:
            issue_number: Issue number to fetch
            
        Returns:
            Issue data dict or None if failed
        """
        url = f"{self.github_api_url}/repos/{self.repository}/issues/{issue_number}"
        
        response = await self.make_request_with_retry(
            "GET", url, headers=self.headers
        )
        
        if not response or response.status_code != 200:
            print_flush(f"   ❌ HTTP {response.status_code if response else 'No response'} getting issue #{issue_number} details")
            return None
            
        try:
            data = response.json()
            
            # Check for null response
            if data is None:
                print_flush(f"   ❌ Empty JSON response for issue #{issue_number}")
                return None
                
            return data
        except Exception as e:
            print_flush(f"   ❌ Failed to parse issue #{issue_number} JSON: {e}")
            return None
    
    async def find_project_item_id(self, issue_id: str) -> Optional[str]:
        """
        Find project item ID for a given issue using GraphQL pagination.
        
        Args:
            issue_id: GitHub issue node ID
            
        Returns:
            Project item ID or None if not found
        """
        cursor = ""
        page_count = 0
        
        while True:
            page_count += 1
            if page_count > 100:  # Safety check
                print(f"Searched 100 pages for issue {issue_id}. Stopping.")
                break
            
            # Build GraphQL query with or without cursor
            if not cursor:
                query = """
                query($projectId: ID!) {
                  node(id: $projectId) {
                    ... on ProjectV2 {
                      items(first: 100) {
                        pageInfo { hasNextPage endCursor }
                        nodes {
                          id
                          content { ... on Issue { id } }
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
                        pageInfo { hasNextPage endCursor }
                        nodes {
                          id
                          content { ... on Issue { id } }
                        }
                      }
                    }
                  }
                }
                """
                variables = {"projectId": self.project_id, "cursor": cursor}
            
            payload = {
                "query": query,
                "variables": variables
            }
            
            response = await self.make_request_with_retry(
                "POST", self.graphql_url, 
                headers=self.headers, json=payload
            )
            
            if not response or response.status_code != 200:
                print_flush(f"   ❌ HTTP {response.status_code if response else 'No response'} searching for issue {issue_id}")
                continue
                
            try:
                data = response.json()
                
                # Check for null response (like the bash version does)
                if data is None:
                    print_flush(f"   ❌ Empty JSON response for issue {issue_id}")
                    continue
                
                # Check for GraphQL errors (matching workflow error handling)
                if data.get("errors"):
                    if not self._handle_graphql_errors(data['errors'], f"finding project item for issue {issue_id}"):
                        return None  # Fatal permission error
                    break
                
                # Safely navigate the response structure
                node_data = data.get("data")
                if not node_data:
                    print_flush(f"   ❌ No 'data' in GraphQL response for issue {issue_id}")
                    continue
                    
                project_node = node_data.get("node")
                if not project_node:
                    print_flush(f"   ❌ No project 'node' in response for issue {issue_id}")
                    continue
                    
                items_data = project_node.get("items")
                if not items_data:
                    print_flush(f"   ❌ No 'items' in project node for issue {issue_id}")
                    continue
                
                # Look for the issue in current batch
                items = items_data.get("nodes", [])
                for item in items:
                    if item and item.get("content", {}).get("id") == issue_id:
                        return item.get("id")
                
                # Check if there are more pages
                page_info = items_data.get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break
                    
                cursor = page_info.get("endCursor")
                
            except Exception as e:
                print_flush(f"   ❌ Error parsing project search response for issue {issue_id}: {e}")
                print_flush(f"   📄 Response status: {response.status_code if response else 'No response'}")
                break
        
        return None
    
    async def get_issue_field_values(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Get issue field values including Status and Work Started.
        
        Args:
            item_id: Project item ID
            
        Returns:
            Field values dict or None if failed
        """
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
        
        payload = {
            "query": query,
            "variables": {"itemId": item_id}
        }
        
        response = await self.make_request_with_retry(
            "POST", self.graphql_url,
            headers=self.headers, json=payload
        )
        
        if not response or response.status_code != 200:
            print_flush(f"   ❌ HTTP {response.status_code if response else 'No response'} getting field values for item {item_id}")
            return None
            
        try:
            data = response.json()
            
            # Check for null response (matching workflow pattern)  
            if data is None:
                print_flush(f"   ❌ Empty JSON response getting field values for item {item_id}")
                return None
            
            # Check for GraphQL errors (matching workflow error handling)
            if data.get("errors"):
                if not self._handle_graphql_errors(data['errors'], "getting field values"):
                    return None  # Fatal permission error
                return None
                
            # Verify we have the expected structure
            if not data.get("data"):
                print_flush(f"   ❌ No 'data' in GraphQL response for field values")
                return None
                
            return data
            
        except Exception as e:
            print_flush(f"   ❌ Error parsing field values response: {e}")
            return None
    
    async def update_work_started_field(self, item_id: str, date: str) -> bool:
        """
        Update the Work Started field for a project item.
        
        Args:
            item_id: Project item ID
            date: Date to set in YYYY-MM-DD format
            
        Returns:
            True if successful, False otherwise
        """
        mutation = """
        mutation {
          updateProjectV2ItemFieldValue(input: {
            projectId: "%s",
            itemId: "%s",
            fieldId: "%s",
            value: { date: "%s" }
          }) {
            clientMutationId
          }
        }
        """ % (self.project_id, item_id, self.work_started_field_id, date)
        
        payload = {"query": mutation}
        
        response = await self.make_request_with_retry(
            "POST", self.graphql_url,
            headers=self.headers, json=payload
        )
        
        if not response or response.status_code != 200:
            print_flush(f"   ❌ HTTP {response.status_code if response else 'No response'} updating Work Started field")
            return False
            
        try:
            data = response.json()
            
            # Check for null response (matching workflow pattern)
            if data is None:
                print_flush(f"   ❌ Empty JSON response updating Work Started field")
                return False
            
            # Check for GraphQL errors (matching workflow error handling)
            if data.get("errors"):
                if not self._handle_graphql_errors(data['errors'], "updating Work Started field"):
                    return False  # Fatal permission error
                return False
                
            # Verify successful mutation (workflow checks for clientMutationId)
            mutation_data = data.get("data", {}).get("updateProjectV2ItemFieldValue")
            if not mutation_data:
                print_flush(f"   ❌ No mutation data in update response")
                return False
                
            return True
            
        except Exception as e:
            print_flush(f"   ❌ Error parsing update response: {e}")
            return False
    
    async def process_issue(self, issue_number: int) -> None:
        """
        Process a single issue: check status and update Work Started if needed.
        
        Args:
            issue_number: Issue number to process
        """

        repo = last_processed_issue_number.get(issue_number)
        if repo == self.repository:
            print_flush(f"⏭️  Issue #{issue_number} already processed, skipping...")
            return
        
        async with self.semaphore:
            try:
                print_flush(f"🔄 Processing issue #{issue_number}...")
                
                # Get issue details
                print_flush(f"   🔍 Getting details for issue #{issue_number}...")
                try:
                    issue_details = await asyncio.wait_for(
                        self.get_issue_details(issue_number), timeout=30.0
                    )
                except asyncio.TimeoutError:
                    print_flush(f"   ⏰ Timeout getting details for issue #{issue_number}")
                    self.error_count += 1
                    return
                if not issue_details:
                    print_flush(f"   ❌ Could not get issue #{issue_number} details, skipping")
                    self.error_count += 1
                    return
                
                issue_id = issue_details.get("node_id")
                if not issue_id:
                    print_flush(f"   ❌ Could not get issue ID for #{issue_number}, skipping")
                    self.error_count += 1
                    return
                
                print_flush(f"   ✅ Got issue details for #{issue_number} (ID: {issue_id[:20]}...)")
                
                # Find project item ID
                print_flush(f"   🔍 Finding project item for issue #{issue_number}...")
                try:
                    item_id = await asyncio.wait_for(
                        self.find_project_item_id(issue_id), timeout=45.0
                    )
                except asyncio.TimeoutError:
                    print_flush(f"   ⏰ Timeout finding project item for issue #{issue_number}")
                    self.error_count += 1
                    return
                if not item_id:
                    print_flush(f"   ⚠️  Issue #{issue_number} not found in project, skipping")
                    self.processed_count += 1
                    return
                
                print_flush(f"   ✅ Found project item for #{issue_number} (Item ID: {item_id[:20]}...)")
                
                # Get field values
                print_flush(f"   🔍 Getting field values for issue #{issue_number}...")
                try:
                    field_data = await asyncio.wait_for(
                        self.get_issue_field_values(item_id), timeout=30.0
                    )
                except asyncio.TimeoutError:
                    print_flush(f"   ⏰ Timeout getting field values for issue #{issue_number}")
                    self.error_count += 1
                    return
                if not field_data:
                    print_flush(f"   ❌ Could not get field values for issue #{issue_number}")
                    self.error_count += 1
                    return
                
                print_flush(f"   ✅ Got field values for issue #{issue_number}")
                
                # Parse field values
                field_nodes = field_data.get("data", {}).get("node", {}).get("fieldValues", {}).get("nodes", [])
                
                status_value = None
                status_updated_at = None
                work_started_value = None
                
                valid_statuses = {"In Progress", "Assigned", "Screen", "Blocked", "Done", "In Review"}
                
                for node in field_nodes:
                    # Check for Status field
                    if node.get("name") in valid_statuses:
                        status_value = node.get("name")
                        status_updated_at = node.get("updatedAt")
                    
                    # Check for Work Started field
                    field = node.get("field", {})
                    if (field.get("id") == self.work_started_field_id or 
                        field.get("name") == "Work Started"):
                        work_started_value = node.get("date")
                
                print_flush(f"   📋 Issue #{issue_number}: Status='{status_value}', Work Started='{work_started_value}'")
                
                # Check if we need to update
                if (status_value == "In Progress" and 
                    (not work_started_value or work_started_value == "null")):
                    
                    print_flush(f"   🔄 Issue #{issue_number} needs Work Started update...")
                    
                    # Determine the date to use
                    if status_updated_at and status_updated_at != "null":
                        work_started_date = status_updated_at.split('T')[0]
                        print_flush(f"   📅 Using status change date: {work_started_date}")
                    else:
                        from datetime import date
                        work_started_date = date.today().strftime('%Y-%m-%d')
                        print_flush(f"   📅 Using current date: {work_started_date}")
                    
                    # Update the field
                    print_flush(f"   🔍 Updating Work Started field for issue #{issue_number}...")
                    try:
                        update_result = await asyncio.wait_for(
                            self.update_work_started_field(item_id, work_started_date), timeout=30.0
                        )
                    except asyncio.TimeoutError:
                        print_flush(f"   ⏰ Timeout updating field for issue #{issue_number}")
                        self.error_count += 1
                        return
                    
                    if update_result:
                        print_flush(f"   ✅ Updated Work Started for issue #{issue_number} to {work_started_date}")
                        self.updated_count += 1
                    else:
                        print_flush(f"   ❌ Failed to update Work Started for issue #{issue_number}")
                        self.error_count += 1
                else:
                    print_flush(f"   ⏭️  Issue #{issue_number}: No update needed")
                    if status_value == "In Progress":
                        print_flush(f"   💾 Caching processed issue #{issue_number}")
                        last_processed_issue_number[issue_number] = self.repository
                
                print_flush(f"   ✅ Completed processing issue #{issue_number}")
                self.processed_count += 1
                
            except Exception as e:
                print_flush(f"   ❌ Error processing issue #{issue_number}: {e}")
                self.error_count += 1
    

async def get_all_repository_issues(client: httpx.AsyncClient, 
                                  token: str, 
                                  repository: List[str]) -> AsyncGenerator[tuple, None]:
    """
    Async generator that fetches all open issues from repositories with pagination.
    
    Args:
        client: httpx client
        token: GitHub token
        repository: List of repositories in format ["owner/repo", ...]
        
    Yields:
        Tuples of (repository_name, list_of_issue_numbers)
    """
    for repo in repository:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        page = 1
        max_pages = 50  # Safety check to prevent infinite loops
        
        print_flush(f"Fetching all open issues from repository {repo}...")
        
        while page <= max_pages:
            print_flush(f"Fetching page {page} of issues...")
            
            # Build URL with pagination parameters
            url = f"https://api.github.com/repos/{repo}/issues"
            params = {
                "state": "open",
                "per_page": 100,
                "page": page
            }
            
            try:
                response = await client.get(url, headers=headers, params=params)
                
                if response.status_code != 200:
                    print(f"Error fetching issues page {page} for {repo}: {response.status_code}")
                    break
                    
                issues_data = response.json()
                page_issue_count = len(issues_data)
                
                if page_issue_count == 0:
                    print(f"No more issues found on page {page} for {repo}")
                    break
                
                print(f"Found {page_issue_count} issues on page {page} for {repo}")
                
                # Yield each issue number from this page
                yield repo, [issue.get("number") for issue in issues_data if issue.get("number")]
                
                # Check if we got less than 100 issues (last page)
                if page_issue_count < 100:
                    print(f"Last page reached for {repo} (less than 100 issues)")
                    break
                    
                page += 1
                
            except Exception as e:
                print(f"Error fetching issues page {page} for {repo}: {e}")
                break
    
        if page > max_pages:
            print(f"Reached maximum pages ({max_pages}) for {repo}. Stopping to prevent infinite loop.")




async def main():
    """Main function to run the bulk update."""
    import os
    
    print_flush("🚀 Starting GitHub Issues Bulk Update Script")
    print_flush("=" * 50)

    global last_processed_issue_number
    try:
        with open("/tmp/last_processed_issue_number.txt", "r") as f:
            last_processed_issue_number = json.load(f)
        print_flush(f"📄 Loaded cache with {len(last_processed_issue_number)} processed issues")
    except FileNotFoundError:
        print_flush("📄 No existing cache file found, starting fresh")
        last_processed_issue_number = {}
    except Exception as e:
        print_flush(f"📄 Error loading cache file: {e}, starting fresh")
        last_processed_issue_number = {}
    

    # Configuration - these should match the GitHub workflow environment
    token = os.getenv("GITHUB_TOKEN")
    repositories = ["tenstorrent/tt-mlir"]
    project_id = "PVT_kwDOA9MHEM4AjeTl"
    work_started_field_id = "PVTF_lADOA9MHEM4AjeTlzgzZQtk"
    max_concurrent = 5
    
    print_flush(f"🔧 Configuration:")
    print_flush(f"   - Repositories: {repositories}")
    print_flush(f"   - Project ID: {project_id}")
    print_flush(f"   - Max concurrent: {max_concurrent}")
    
    if not token:
        print_flush("❌ Error: GITHUB_TOKEN environment variable is required")
        return    

    print_flush("\n🔗 Initializing HTTP client...")
    # Use httpx with connection pooling for better performance
    async with httpx.AsyncClient(
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        timeout=httpx.Timeout(60.0)  # Increased timeout to handle rate limits better
    ) as client:
        try:
            print_flush("🔍 Starting repository processing...")
            async for repo, issue_numbers in get_all_repository_issues(client, token, repositories):
                print_flush(f"\n📂 Processing repository: {repo}")
                print_flush(f"📊 Found {len(issue_numbers)} issues to process")
                
                # Create updater and run
                updater = GitHubProjectUpdater(
                    token=token,
                    repository=repo,
                    project_id=project_id,
                    work_started_field_id=work_started_field_id,
                    client=client,
                    max_concurrent=max_concurrent
                )
                
                print_flush(f"🔄 Creating {len(issue_numbers)} processing tasks...")
                # Create tasks for all issues
                tasks = [
                    updater.process_issue(issue_number) 
                    for issue_number in issue_numbers
                ]
                
                print_flush(f"⚡ Starting concurrent processing with max {max_concurrent} simultaneous requests...")
                start_time = time.time()
                
                # Add progress tracking
                progress_task = asyncio.create_task(track_progress(updater, len(issue_numbers), start_time))
                
                # Run all tasks concurrently (semaphore controls actual concurrency)
                try:
                    await asyncio.gather(*tasks, return_exceptions=True)
                finally:
                    progress_task.cancel()
                    try:
                        await progress_task
                    except asyncio.CancelledError:
                        pass
                
                end_time = time.time()
                duration = end_time - start_time
                
                print_flush(f"\n📈 Repository {repo} Summary:")
                print_flush(f"   ✅ Processed: {updater.processed_count}")
                print_flush(f"   🔄 Updated: {updater.updated_count}")
                print_flush(f"   ❌ Errors: {updater.error_count}")
                print_flush(f"   ⏱️  Duration: {duration:.2f} seconds")

        except Exception as e:
            print_flush(f"❌ Fatal error during processing: {e}")
            import traceback
            print_flush(traceback.format_exc())
        finally:
            print_flush("\n💾 Saving cache...")
            with open("/tmp/last_processed_issue_number.txt", "w") as f:
                json.dump(last_processed_issue_number, f)
            print_flush("✅ Cache saved successfully")


if __name__ == "__main__":
    asyncio.run(main())

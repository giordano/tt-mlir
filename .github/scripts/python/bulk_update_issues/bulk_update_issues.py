#!/usr/bin/env python3
"""
Bulk update script for GitHub project issues Work Started field.
Converts the bash run_bulk_update function to Python using asyncio and httpx.
"""

import asyncio
import json
import random
import time
from pathlib import Path
from typing import List, Optional, Dict, Any, AsyncGenerator

import httpx

last_processed_issue_number = {}

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
                    
                    print(f"Rate limited, sleeping for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)
                    return True
            except Exception:
                pass
                
        if response.status_code == 429:
            # Handle retry-after header if present
            retry_after = response.headers.get('retry-after')
            if retry_after:
                sleep_time = int(retry_after) + random.randint(10, 30)
            else:
                sleep_time = random.randint(60, 120)
            
            print(f"Rate limited (429), sleeping for {sleep_time} seconds")
            await asyncio.sleep(sleep_time)
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
            print(f"Failed to get issue #{issue_number} details")
            return None
            
        try:
            return response.json()
        except Exception as e:
            print(f"Failed to parse issue #{issue_number} JSON: {e}")
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
                continue
                
            try:
                data = response.json()
                
                if data.get("errors"):
                    print(f"GraphQL error finding issue {issue_id}: {data['errors']}")
                    break
                
                # Look for the issue in current batch
                items = data.get("data", {}).get("node", {}).get("items", {}).get("nodes", [])
                for item in items:
                    if item.get("content", {}).get("id") == issue_id:
                        return item.get("id")
                
                # Check if there are more pages
                page_info = data.get("data", {}).get("node", {}).get("items", {}).get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break
                    
                cursor = page_info.get("endCursor")
                
            except Exception as e:
                print(f"Error parsing project search response: {e}")
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
            return None
            
        try:
            data = response.json()
            
            if data.get("errors"):
                print(f"GraphQL error getting field values: {data['errors']}")
                return None
                
            return data
            
        except Exception as e:
            print(f"Error parsing field values response: {e}")
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
            return False
            
        try:
            data = response.json()
            
            if data.get("errors"):
                print(f"GraphQL error updating Work Started: {data['errors']}")
                return False
                
            return True
            
        except Exception as e:
            print(f"Error parsing update response: {e}")
            return False
    
    async def process_issue(self, issue_number: int) -> None:
        """
        Process a single issue: check status and update Work Started if needed.
        
        Args:
            issue_number: Issue number to process
        """

        repo = last_processed_issue_number.get(issue_number)
        if repo == self.repository:
            print(f"Issue #{issue_number} for repository {self.repository} already processed, skipping...")
            return
        
        async with self.semaphore:
            try:
                print(f"Processing issue #{issue_number}...")
                
                # Get issue details
                issue_details = await self.get_issue_details(issue_number)
                if not issue_details:
                    print(f"Could not get issue #{issue_number} details, skipping")
                    self.error_count += 1
                    return
                
                issue_id = issue_details.get("node_id")
                if not issue_id:
                    print(f"Could not get issue ID for #{issue_number}, skipping")
                    self.error_count += 1
                    return
                
                # Find project item ID
                item_id = await self.find_project_item_id(issue_id)
                if not item_id:
                    print(f"Issue #{issue_number} not found in project, skipping")
                    self.processed_count += 1
                    return
                
                # Get field values
                field_data = await self.get_issue_field_values(item_id)
                if not field_data:
                    print(f"Could not get field values for issue #{issue_number}")
                    self.error_count += 1
                    return
                
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
                
                # Check if we need to update
                if (status_value == "In Progress" and 
                    (not work_started_value or work_started_value == "null")):
                    
                    print(f"Issue #{issue_number} has Status='In Progress' but no Work Started date - updating...")
                    
                    # Determine the date to use
                    if status_updated_at and status_updated_at != "null":
                        work_started_date = status_updated_at.split('T')[0]
                        print(f"Using status change date: {work_started_date}")
                    else:
                        from datetime import date
                        work_started_date = date.today().strftime('%Y-%m-%d')
                        print(f"Using current date: {work_started_date}")
                    
                    # Update the field
                    if await self.update_work_started_field(item_id, work_started_date):
                        print(f"✅ Updated Work Started for issue #{issue_number} to {work_started_date}")
                        self.updated_count += 1
                    else:
                        print(f"❌ Failed to update Work Started for issue #{issue_number}")
                        self.error_count += 1
                else:
                    print(f"Issue #{issue_number}: Status='{status_value}', Work Started='{work_started_value}' - no update needed")
                    if status_value == "In Progress":
                        print(f"Saving last processed issue number {issue_number} to cache file")
                        last_processed_issue_number[issue_number] = self.repository
                
                self.processed_count += 1
                
            except Exception as e:
                print(f"Error processing issue #{issue_number}: {e}")
                self.error_count += 1
    

async def get_all_repository_issues(client: httpx.AsyncClient, 
                                  token: str, 
                                  repository: List[str]) -> AsyncGenerator[int, None]:
    """
    Async generator that fetches all open issues from a repository with pagination.
    
    Args:
        client: httpx client
        token: GitHub token
        repository: Repository in format "owner/repo"
        
    Yields:
        Issue numbers as integers
    """
    for repo in repository:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        page = 1
        max_pages = 50  # Safety check to prevent infinite loops
        
        print(f"Fetching all open issues from repository {repository}...")
        
        while page <= max_pages:
            print(f"Fetching page {page} of issues...")
            
            # Build URL with pagination parameters
            url = f"https://api.github.com/repos/{repository}/issues"
            params = {
                "state": "open",
                "per_page": 100,
                "page": page
            }
            
            try:
                response = await client.get(url, headers=headers, params=params)
                
                if response.status_code != 200:
                    print(f"Error fetching issues page {page}: {response.status_code}")
                    break
                    
                issues_data = response.json()
                page_issue_count = len(issues_data)
                
                if page_issue_count == 0:
                    print(f"No more issues found on page {page}")
                    break
                
                print(f"Found {page_issue_count} issues on page {page}")
                
                # Yield each issue number from this page
                yield repo, [issue.get("number") for issue in issues_data]
                
                # Check if we got less than 100 issues (last page)
                if page_issue_count < 100:
                    print("Last page reached (less than 100 issues)")
                    break
                    
                page += 1
                
            except Exception as e:
                print(f"Error fetching issues page {page}: {e}")
                break
    
    if page > max_pages:
        print(f"Reached maximum pages ({max_pages}). Stopping to prevent infinite loop.")




async def main():
    """Main function to run the bulk update."""
    import os

    global last_processed_issue_number
    try:
        with open("/tmp/last_processed_issue_number.txt", "r") as f:
            last_processed_issue_number = json.load(f)
    except FileNotFoundError:
        print("No existing cache file found, starting fresh")
        last_processed_issue_number = {}
    except Exception as e:
        print(f"Error loading cache file: {e}, starting fresh")
        last_processed_issue_number = {}
    

    # Configuration - these should match the GitHub workflow environment
    token = os.getenv("GITHUB_TOKEN")
    repositories = ["tenstorrent/tt-mlir"]
    project_id = "PVT_kwDOA9MHEM4AjeTl"
    work_started_field_id = "PVTF_lADOA9MHEM4AjeTlzgzZQtk"
    max_concurrent = 5
    
    if not token:
        print("Error: GITHUB_TOKEN environment variable is required")
        return    

    # Use httpx with connection pooling for better performance
    async with httpx.AsyncClient(
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        timeout=httpx.Timeout(30.0)
    ) as client:
        try:
            async for repo, issue_numbers in get_all_repository_issues(client, token, repositories):
                # Create updater and run
                updater = GitHubProjectUpdater(
                    token=token,
                    repository=repo,
                    project_id=project_id,
                    work_started_field_id=work_started_field_id,
                    client=client,
                    max_concurrent=max_concurrent
                )
                # Create tasks for all issues
                tasks = [
                    self.process_issue(issue_number) 
                    for issue_number in issue_numbers
                ]
                
                # Run all tasks concurrently (semaphore controls actual concurrency)
                await asyncio.gather(*tasks, return_exceptions=True)

        finally:
            with open("/tmp/last_processed_issue_number.txt", "w") as f:
                json.dump(last_processed_issue_number, f)


if __name__ == "__main__":
    asyncio.run(main())

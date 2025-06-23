#!/usr/bin/env python3
"""
MCP Client that connects to hosted MCP server over HTTP
This allows Claude Desktop to connect to remote MCP servers

Usage: python mcp_http_client.py http://your-server-url:5000
"""

import json
import sys
import requests
from typing import Any, Dict


class MCPHttpClient:
    def __init__(self, server_url: str):
        self.server_url = server_url.rstrip('/')
        self.mcp_endpoint = f"{self.server_url}/mcp"
        self.session = requests.Session()
        self.session.timeout = 30

    def send_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Send MCP request to remote server"""
        try:
            response = self.session.post(
                self.mcp_endpoint,
                json=request_data,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.ConnectionError:
            return {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "error": {
                    "code": -32603,
                    "message": f"Cannot connect to MCP server at {self.server_url}"
                }
            }
        except requests.exceptions.Timeout:
            return {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "error": {
                    "code": -32603,
                    "message": "Request to MCP server timed out"
                }
            }
        except requests.exceptions.RequestException as e:
            return {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "error": {
                    "code": -32603,
                    "message": f"HTTP error: {str(e)}"
                }
            }
        except json.JSONDecodeError:
            return {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "error": {
                    "code": -32603,
                    "message": "Invalid JSON response from server"
                }
            }

    def run(self):
        """Main client loop - reads from stdin, sends to HTTP server"""
        try:
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue

                try:
                    request_data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Handle notifications/initialized (no response needed)
                if request_data.get("method") == "notifications/initialized":
                    continue

                # Send request to remote MCP server
                response = self.send_request(request_data)

                # Send response back to Claude Desktop
                if response:
                    print(json.dumps(response), flush=True)

        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"Fatal error: {e}", file=sys.stderr)
            sys.exit(1)


def main():
    if len(sys.argv) != 2:
        print("Usage: python mcp_http_client.py http://server-url:port", file=sys.stderr)
        sys.exit(1)

    server_url = sys.argv[1]

    # Validate server URL
    if not server_url.startswith(('http://', 'https://')):
        print("Error: Server URL must start with http:// or https://", file=sys.stderr)
        sys.exit(1)

    client = MCPHttpClient(server_url)
    client.run()


if __name__ == "__main__":
    main()
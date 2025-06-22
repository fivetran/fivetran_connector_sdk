#!/usr/bin/env python3
"""
MCP Server that bridges to Flask API
This connects Claude Desktop to your Flask server
"""

import json
import sys
import requests
from typing import Any, Dict

# Configuration
FLASK_BASE_URL = "http://localhost:5050"  # Your Flask server URL


class MCPFlaskBridge:
    def __init__(self):
        self.tools = {
            "get_sdk_team": {
                "name": "get_sdk_team",
                "description": "Get the name of the team that handles SDK work",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            "get_sdk_manager": {
                "name": "get_sdk_manager",
                "description": "Get the manager of the SDK team",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            "get_sdk_senior_devs": {
                "name": "get_sdk_senior_devs",
                "description": "Get the senior developers on the SDK team",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            "get_sdk_junior_devs": {
                "name": "get_sdk_junior_devs",
                "description": "Get the junior developers on the SDK team",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            "get_product_managers": {
                "name": "get_product_managers",
                "description": "Get information about product managers",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            "get_technical_writing_team": {
                "name": "get_technical_writing_team",
                "description": "Get information about technical writing team",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        }
        self.initialized = False

    def call_flask_api(self, endpoint: str) -> str:
        """Make HTTP request to Flask API"""
        try:
            url = f"{FLASK_BASE_URL}{endpoint}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            # Format the response nicely
            if "team_name" in data:
                return data["team_name"]
            elif "manager" in data:
                return data["manager"]
            elif "developers" in data:
                return ", ".join(data["developers"])
            elif "product_managers" in data:
                result = "Product Managers across all teams:\n\n"
                for team, managers in data["product_managers"].items():
                    result += f"{team}:\n"
                    for manager in managers:
                        result += f"  - {manager}\n"
                return result.strip()
            elif "technical_writers" in data:
                return ", ".join(data["technical_writers"])
            else:
                return json.dumps(data, indent=2)

        except requests.exceptions.ConnectionError:
            return f"Error: Cannot connect to Flask server at {FLASK_BASE_URL}. Make sure your Flask server is running."
        except requests.exceptions.Timeout:
            return "Error: Request to Flask server timed out."
        except requests.exceptions.RequestException as e:
            return f"Error calling Flask API: {str(e)}"
        except Exception as e:
            return f"Unexpected error: {str(e)}"

    def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle incoming MCP requests"""
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id")

        try:
            if method == "initialize":
                self.initialized = True
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {
                                "listChanged": False
                            }
                        },
                        "serverInfo": {
                            "name": "team-info-flask-bridge",
                            "version": "1.0.0"
                        }
                    }
                }

            elif method == "notifications/initialized":
                return None

            elif method == "tools/list":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "tools": list(self.tools.values())
                    }
                }

            elif method == "tools/call":
                tool_name = params.get("name")

                # Map tool names to Flask endpoints
                endpoint_map = {
                    "get_sdk_team": "/sdk/team",
                    "get_sdk_manager": "/sdk/manager",
                    "get_sdk_senior_devs": "/sdk/developers/senior",
                    "get_sdk_junior_devs": "/sdk/developers/junior",
                    "get_product_managers": "/product-managers",
                    "get_technical_writing_team": "/technical-writing"
                }

                if tool_name in endpoint_map:
                    endpoint = endpoint_map[tool_name]
                    result = self.call_flask_api(endpoint)

                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": result
                                }
                            ]
                        }
                    }
                else:
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32601,
                            "message": f"Tool not found: {tool_name}"
                        }
                    }

            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method}"
                    }
                }

        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {str(e)}"
                }
            }

    def run(self):
        """Main server loop"""
        try:
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue

                try:
                    request = json.loads(line)
                except json.JSONDecodeError:
                    continue

                response = self.handle_request(request)
                if response is not None:
                    print(json.dumps(response), flush=True)

        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"Fatal error: {e}", file=sys.stderr)
            sys.exit(1)


def main():
    server = MCPFlaskBridge()
    server.run()


if __name__ == "__main__":
    main()
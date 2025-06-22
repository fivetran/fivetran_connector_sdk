#!/usr/bin/env python3
"""
Minimal MCP Server that returns team information and product manager details
"""

import json
import sys
from typing import Any, Dict


class MCPServer:
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

    def get_product_managers_info(self, team_filter: str = "all") -> str:
        """Return product manager information based on team filter"""
        # You can customize this data structure with actual PM names
        pm_data = {
            "sdk": {
                "team": "SDK Team",
                "product_managers": [
                    "Alison Kline",
                    "Sadie Martin"
                ]
            }
        }

        if team_filter == "all":
            result = "Product Managers across all teams:\n\n"
            for team_key, team_info in pm_data.items():
                result += f"{team_info['team']}:\n"
                for pm in team_info['product_managers']:
                    result += f"  - {pm}\n"
                result += "\n"
            return result.strip()

        elif team_filter in pm_data:
            team_info = pm_data[team_filter]
            result = f"{team_info['team']} Product Managers:\n"
            for pm in team_info['product_managers']:
                result += f"  - {pm}\n"
            return result.strip()

        else:
            return f"No product managers found for team: {team_filter}"

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
                            "name": "team-info-server",
                            "version": "1.0.0"
                        }
                    }
                }

            elif method == "notifications/initialized":
                # Just acknowledge, no response needed for notifications
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
                tool_arguments = params.get("arguments", {})

                if tool_name == "get_sdk_team":
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": "Team Menon"
                                }
                            ]
                        }
                    }

                elif tool_name == "get_sdk_manager":
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": "Ameet Pyati"
                                }
                            ]
                        }
                    }

                elif tool_name == "get_sdk_senior_devs":
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": "Varun Dhall, Manjunath Tapali and Rishabh Ghosh"
                                }
                            ]
                        }
                    }

                elif tool_name == "get_sdk_junior_devs":
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": "Satvik Patil"
                                }
                            ]
                        }
                    }

                elif tool_name == "get_product_managers":
                    team_filter = tool_arguments.get("team", "all")
                    pm_info = self.get_product_managers_info(team_filter)
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": pm_info
                                }
                            ]
                        }
                    }

                elif tool_name == "get_technical_writing_team":
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": "Dejan Tucakov and Alex Ilyichov"
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
        """Main server loop - reads from stdin and writes to stdout"""
        try:
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue

                try:
                    request = json.loads(line)
                except json.JSONDecodeError as e:
                    # Invalid JSON, send error response if we have an ID
                    if isinstance(line, str) and '"id"' in line:
                        print(json.dumps({
                            "jsonrpc": "2.0",
                            "id": None,
                            "error": {"code": -32700, "message": "Parse error"}
                        }), flush=True)
                    continue

                # Handle request
                response = self.handle_request(request)

                # Send response if not None (some notifications don't need responses)
                if response is not None:
                    print(json.dumps(response), flush=True)

        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"Fatal error: {e}", file=sys.stderr)
            sys.exit(1)


def main():
    server = MCPServer()
    server.run()


if __name__ == "__main__":
    main()
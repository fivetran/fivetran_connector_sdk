#!/usr/bin/env python3
"""
Flask server that hosts MCP Server over HTTP with Server-Sent Events
This allows remote MCP clients to connect over HTTP instead of stdin/stdout
"""

from flask import Flask, request, jsonify, Response
import json
import uuid
from typing import Dict, Any
import time

app = Flask(__name__)

# Team data
TEAM_DATA = {
    "sdk_team": "Team Menon",
    "sdk_manager": "Ameet Pyati",
    "sdk_senior_devs": ["Varun Dhall", "Manjunath Tapali", "Rishabh Ghosh"],
    "sdk_junior_devs": ["Satvik Patil"],
    "product_managers": {
        "SDK Team": ["Alison Kline", "Sadie Martin"]
    },
    "technical_writers": ["Dejan Tucakov", "Alex Ilyichov"]
}


# MCP Server implementation
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
        self.sessions = {}

    def get_product_managers_info(self) -> str:
        result = "Product Managers across all teams:\n\n"
        for team, managers in TEAM_DATA["product_managers"].items():
            result += f"{team}:\n"
            for manager in managers:
                result += f"  - {manager}\n"
        return result.strip()

    def handle_mcp_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP protocol requests"""
        method = request_data.get("method")
        params = request_data.get("params", {})
        request_id = request_data.get("id")

        try:
            if method == "initialize":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {"listChanged": False}
                        },
                        "serverInfo": {
                            "name": "hosted-team-info-server",
                            "version": "1.0.0"
                        }
                    }
                }

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

                if tool_name == "get_sdk_team":
                    text = TEAM_DATA["sdk_team"]
                elif tool_name == "get_sdk_manager":
                    text = TEAM_DATA["sdk_manager"]
                elif tool_name == "get_sdk_senior_devs":
                    text = ", ".join(TEAM_DATA["sdk_senior_devs"])
                elif tool_name == "get_sdk_junior_devs":
                    text = ", ".join(TEAM_DATA["sdk_junior_devs"])
                elif tool_name == "get_product_managers":
                    text = self.get_product_managers_info()
                elif tool_name == "get_technical_writing_team":
                    text = ", ".join(TEAM_DATA["technical_writers"])
                else:
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32601,
                            "message": f"Tool not found: {tool_name}"
                        }
                    }

                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{"type": "text", "text": text}]
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


# Initialize MCP server
mcp_server = MCPServer()


# Web interface routes
@app.route('/')
def home():
    """Home page explaining the hosted MCP server"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Hosted MCP Server - Team Info</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .endpoint { margin: 10px 0; padding: 10px; background: #f0f8ff; border-radius: 5px; }
            .code { background: #f5f5f5; padding: 10px; border-radius: 5px; font-family: monospace; }
        </style>
    </head>
    <body>
        <h1>🚀 Hosted MCP Server - Team Info</h1>
        <p>This server provides team information via the MCP (Model Context Protocol).</p>

        <h2>Available Endpoints:</h2>
        <div class="endpoint">
            <strong>POST /mcp</strong> - Send MCP JSON-RPC requests
        </div>
        <div class="endpoint">
            <strong>GET /mcp/tools</strong> - List available tools
        </div>
        <div class="endpoint">
            <strong>GET /mcp/info</strong> - Get server information
        </div>

        <h2>For Claude Desktop Users:</h2>
        <p>Add this to your <code>claude_desktop_config.json</code>:</p>
        <div class="code">
{
  "mcpServers": {
    "hosted-team-info": {
      "command": "python",
      "args": ["/path/to/mcp_http_client.py", "http://YOUR_SERVER_URL:5000"]
    }
  }
}
        </div>

        <h2>Test MCP Request:</h2>
        <div class="code">
curl -X POST http://localhost:5000/mcp \\
  -H "Content-Type: application/json" \\
  -d '{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "get_sdk_team"}}'
        </div>

        <p><strong>Server Status:</strong> ✅ Running</p>
        <p><strong>MCP Protocol Version:</strong> 2024-11-05</p>
    </body>
    </html>
    """


@app.route('/mcp', methods=['POST'])
def mcp_endpoint():
    """Handle MCP JSON-RPC requests"""
    try:
        request_data = request.get_json()
        if not request_data:
            return jsonify({"error": "Invalid JSON"}), 400

        response = mcp_server.handle_mcp_request(request_data)
        return jsonify(response)

    except Exception as e:
        return jsonify({
            "jsonrpc": "2.0",
            "id": request_data.get("id") if request_data else None,
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        }), 500


@app.route('/mcp/tools', methods=['GET'])
def list_tools():
    """List available MCP tools"""
    return jsonify({
        "tools": list(mcp_server.tools.values()),
        "count": len(mcp_server.tools)
    })


@app.route('/mcp/info', methods=['GET'])
def server_info():
    """Get server information"""
    return jsonify({
        "name": "hosted-team-info-server",
        "version": "1.0.0",
        "protocol_version": "2024-11-05",
        "capabilities": {
            "tools": {"listChanged": False}
        },
        "endpoints": {
            "mcp": "/mcp",
            "tools": "/mcp/tools",
            "info": "/mcp/info"
        }
    })


@app.route('/health')
def health():
    """Health check"""
    return jsonify({"status": "healthy", "service": "hosted-mcp-server"})


if __name__ == '__main__':
    print("🚀 Starting Hosted MCP Server...")
    print("📍 Server will be available at: http://localhost:5000")
    print("🔧 MCP endpoint: http://localhost:5000/mcp")
    print("📚 Documentation: http://localhost:5000")
    print("🛑 Press Ctrl+C to stop")
    print("-" * 50)

    app.run(
        host='0.0.0.0',
        port=5050,
        debug=True
    )
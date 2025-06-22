#!/usr/bin/env python3
"""
Local Flask server for team information
Run with: python flask_server.py
Access at: http://localhost:5000
"""

from flask import Flask, jsonify, render_template_string

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

# HTML template for the home page
HOME_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Team Info API - Flask</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .endpoint { margin: 10px 0; padding: 10px; background: #f0f8ff; border-radius: 5px; }
        .endpoint a { text-decoration: none; color: #0066cc; font-weight: bold; }
        .endpoint a:hover { text-decoration: underline; }
        .description { color: #666; margin-left: 10px; }
        .json-example { background: #f5f5f5; padding: 10px; border-radius: 5px; margin-top: 20px; }
        code { background: #e8e8e8; padding: 2px 4px; border-radius: 3px; }
    </style>
</head>
<body>
    <h1>Team Info API - Flask Server</h1>
    <p>Available endpoints:</p>

    <div class="endpoint">
        <a href="/sdk/team">/sdk/team</a>
        <span class="description">Get SDK team name</span>
    </div>

    <div class="endpoint">
        <a href="/sdk/manager">/sdk/manager</a>
        <span class="description">Get SDK team manager</span>
    </div>

    <div class="endpoint">
        <a href="/sdk/developers/senior">/sdk/developers/senior</a>
        <span class="description">Get senior developers</span>
    </div>

    <div class="endpoint">
        <a href="/sdk/developers/junior">/sdk/developers/junior</a>
        <span class="description">Get junior developers</span>
    </div>

    <div class="endpoint">
        <a href="/product-managers">/product-managers</a>
        <span class="description">Get product managers</span>
    </div>

    <div class="endpoint">
        <a href="/technical-writing">/technical-writing</a>
        <span class="description">Get technical writing team</span>
    </div>

    <div class="endpoint">
        <a href="/health">/health</a>
        <span class="description">Health check</span>
    </div>

    <div class="json-example">
        <h3>Example Usage:</h3>
        <p><strong>Command line:</strong></p>
        <code>curl http://localhost:5000/sdk/team</code>
        <br><br>
        <p><strong>Python:</strong></p>
        <code>
            import requests<br>
            response = requests.get('http://localhost:5000/sdk/team')<br>
            print(response.json())
        </code>
    </div>

    <hr>
    <p><strong>Server running on:</strong> http://localhost:5000</p>
    <p><strong>Framework:</strong> Flask {{ version }}</p>
</body>
</html>
"""


@app.route('/')
def home():
    """Home page with navigation"""
    import flask
    return render_template_string(HOME_TEMPLATE, version=flask.__version__)


@app.route('/sdk/team')
def get_sdk_team():
    """Get the name of the team that handles SDK work"""
    return jsonify({"team_name": TEAM_DATA["sdk_team"]})


@app.route('/sdk/manager')
def get_sdk_manager():
    """Get the manager of the SDK team"""
    return jsonify({"manager": TEAM_DATA["sdk_manager"]})


@app.route('/sdk/developers/senior')
def get_sdk_senior_devs():
    """Get the senior developers on the SDK team"""
    return jsonify({"developers": TEAM_DATA["sdk_senior_devs"]})


@app.route('/sdk/developers/junior')
def get_sdk_junior_devs():
    """Get the junior developers on the SDK team"""
    return jsonify({"developers": TEAM_DATA["sdk_junior_devs"]})


@app.route('/product-managers')
def get_product_managers():
    """Get information about product managers"""
    return jsonify({"product_managers": TEAM_DATA["product_managers"]})


@app.route('/technical-writing')
def get_technical_writing_team():
    """Get information about technical writing team"""
    return jsonify({"technical_writers": TEAM_DATA["technical_writers"]})


@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "team-info-api",
        "framework": "flask",
        "port": 5000
    })


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    print("🚀 Starting Team Info API server (Flask)...")
    print("📍 Server will be available at: http://localhost:5000")
    print("🌐 Access from other devices: http://YOUR_IP:5000")
    print("🛑 Press Ctrl+C to stop the server")
    print("-" * 50)

    app.run(
        host='0.0.0.0',  # Allows access from other devices on network
        port=5050,
        debug=True  # Auto-reload on code changes
    )
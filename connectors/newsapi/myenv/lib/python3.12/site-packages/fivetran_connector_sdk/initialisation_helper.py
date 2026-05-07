import os
import sys

import requests as rq

from fivetran_connector_sdk.logger import Logging
from fivetran_connector_sdk.constants import EXAMPLES_GITHUB_REPO, GITHUB_BRANCH, \
    AI_AGENTS, TEMPLATE_CONNECTOR_PATH, CONNECTORS_GITHUB_REPO, CONNECTORS_TEMPLATE_PREFIX
from fivetran_connector_sdk.helpers import print_library_log

def init(project_dir: str, template: str, force: bool):
    if not force:
        confirm = input(f"create new connector project at {project_dir}? (y/N): ")
    else:
        print_library_log("overriding existing files; --force is set")
        confirm = "y"
    if confirm.lower() != "y":
        print_library_log("project initialization canceled")
        sys.exit(0)

    try:
        setup_connector(project_dir, template, force)
        setup_ai_agent(project_dir, force)
        print_library_log("project initialized", log_icon=Logging.LogIcon.SUCCESS)
        print_library_log("Time to make a great connector; Happy coding")
        sys.exit(0)
    except Exception as e:
        print_library_log(f"failed to initialize project error: {e}", level=Logging.Level.SEVERE, log_icon=Logging.LogIcon.FAILURE)
        sys.exit(1)


def setup_connector(project_dir: str, template: str, force: bool):
    os.makedirs(project_dir, exist_ok=True)
    download_git_directory(template, project_dir, force)
    print_library_log(f"new project created at: {project_dir}", log_icon=Logging.LogIcon.SUCCESS)


def setup_ai_agent(project_dir: str, force: bool):
    ai_agent = input("Which AI Agent shall we optimize the project to work with?\n"
                     "1. Claude\n"
                     "2. Cursor\n"
                     "3. VSCode with a Copilot\n"
                     "4. Do nothing, I already have my AI context configured\n"
                     "\n"
                     "Please enter the number or name of your choice (e.g., '1' or 'Claude'): ").strip()

    ai_agent = ai_agent.lower()
    if ai_agent == "1" or ai_agent == "claude":
        ai_agent = "claude"
    elif ai_agent == "2" or ai_agent == "cursor":
        ai_agent = "cursor"
    elif ai_agent == "3" or ai_agent == "vscode with a copilot":
        ai_agent = "vscode"
    else:
        ai_agent = None

    if ai_agent in AI_AGENTS:
        download_git_directory(AI_AGENTS.get(ai_agent), project_dir, force)
    else:
        print_library_log("skipping AI agent setup", log_icon=Logging.LogIcon.STEP)


def validate_example_directory(files_to_download: list):
    connector_files = [
        f for f in files_to_download
        if f['local_path'].endswith("connector.py")
    ]

    if len(connector_files) != 1:
        print_library_log(
            "selected directory is not a valid example; missing connector.py",
            Logging.Level.SEVERE
        )
        raise ValueError("Invalid directory passed. Path did not resolve to a valid connector.")

def _resolve_repo_and_path(path_prefix: str) -> tuple:
    """Returns (repo, actual_path) based on template routing rules."""
    if path_prefix.startswith(CONNECTORS_TEMPLATE_PREFIX):
        return CONNECTORS_GITHUB_REPO, path_prefix[len(CONNECTORS_TEMPLATE_PREFIX):]
    if path_prefix in AI_AGENTS.values() or path_prefix.startswith("examples/"):
        return EXAMPLES_GITHUB_REPO, path_prefix
    return CONNECTORS_GITHUB_REPO, path_prefix


def download_git_directory(path_prefix: str, project_dir: str, force: bool):
    repo, actual_path = _resolve_repo_and_path(path_prefix)
    try:
        tree_url = f"https://api.github.com/repos/{repo}/git/trees/{GITHUB_BRANCH}?recursive=1"
        response = rq.get(tree_url, timeout=10)
        response.raise_for_status()

        tree_data = response.json()
        if 'tree' not in tree_data:
            print_library_log("failed to fetch repository from GitHub", level=Logging.Level.SEVERE, log_icon=Logging.LogIcon.FAILURE)
            return

        files_to_download = []
        for item in tree_data['tree']:
            if item['type'] == 'blob' and item['path'].startswith(actual_path):
                relative_path = item['path'][len(actual_path):].lstrip('/')
                if (path_prefix == TEMPLATE_CONNECTOR_PATH or path_prefix in AI_AGENTS.values()) and "readme" in relative_path.lower():
                    continue
                files_to_download.append({
                    'github_path': item['path'],
                    'local_path': relative_path,
                    'size': item.get('size', 0)
                })

        if not files_to_download:
            print_library_log("no files to download", Logging.Level.WARNING)
            return

        if path_prefix not in AI_AGENTS.values():
            validate_example_directory(files_to_download)

        print_library_log(f"downloading {len(files_to_download)} files from GitHub", log_icon=Logging.LogIcon.STEP)
        download_file_from_github(files_to_download, project_dir, force, repo)

    except Exception as e:
        print_library_log(f"failed to download files: {e}", Logging.Level.WARNING)
        print_library_log(f"files are available for manual download from: https://github.com/{repo}/tree/{GITHUB_BRANCH}/{actual_path}")


def download_file_from_github(files_to_download: list, project_dir: str, force: bool, repo: str = EXAMPLES_GITHUB_REPO):
    for file_info in files_to_download:
        # Construct raw download URL
        raw_url = f"https://raw.githubusercontent.com/{repo}/{GITHUB_BRANCH}/{file_info['github_path']}"

        # Create target path
        target_path = os.path.join(project_dir, file_info['local_path'])
        target_dir = os.path.dirname(target_path)

        # Create directory if needed
        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)

        # Download file
        try:
            file_response = rq.get(raw_url, timeout=10)
            file_response.raise_for_status()

            if os.path.exists(target_path):
                if not force:
                    override_file = input(f"File {file_info['local_path']} already exists. Overwrite? (y/N): ")
                else:
                    override_file = "y"
                if override_file.lower() != "y":
                    print_library_log(f"skipped {file_info['local_path']}", level=Logging.Level.FINE, log_icon=Logging.LogIcon.SUCCESS)
                    continue

            with open(target_path, 'wb') as f:
                f.write(file_response.content)

            print_library_log(f"downloaded {file_info['local_path']}", level=Logging.Level.FINE, log_icon=Logging.LogIcon.SUCCESS)
        except Exception as e:
            print_library_log(f"failed to download {file_info['local_path']}: {e}", level=Logging.Level.WARNING, log_icon=Logging.LogIcon.FAILURE)

# Contributing to Fivetran Connector SDK

Thank you for your interest in contributing to the Fivetran Connector SDK. This repository is an open-source collection of examples demonstrating how to build custom data connectors using the Fivetran Connector SDK. We welcome contributions from the community to expand and improve this collection.

## Code of conduct

This project adheres to the [Contributor Covenant Code of Conduct](https://github.com/fivetran/fivetran_connector_sdk/tree/main/CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Ways to contribute

We encourage the community to contribute in the following ways:

- New Connector Examples: Add examples for popular APIs, databases, or data sources
- Bug Fixes: Fix issues in existing connector examples
- Performance Improvements: Optimize existing connectors for better performance
- Code Quality Improvements: Refactor code to follow best practices
- New Common Patterns: Add reusable patterns and utilities that benefit multiple connectors

## Getting started

Before contributing, please:

1. Read the Documentation: Familiarize yourself with the [Connector SDK documentation](https://fivetran.com/docs/connector-sdk)
2. Review Existing Examples: Browse the [existing examples](https://github.com/fivetran/fivetran_connector_sdk/tree/main/README.md#examples) to understand our coding patterns and structure
3. Set Up Your Environment: Follow the [setup guide](https://fivetran.com/docs/connector-sdk/setup-guide) to install the Connector SDK


## How to fork and create a pull request

### Step 1: Fork the repository

1. Navigate to the [Fivetran Connector SDK repository](https://github.com/fivetran/fivetran_connector_sdk) on GitHub
2. Click the `Fork` button in the upper-right corner of the page
3. This creates a copy of the repository under your GitHub account

### Step 2: Clone your fork

```bash
# Clone your forked repository to your local machine
git clone https://github.com/<YOUR_USERNAME>/fivetran_connector_sdk.git

# Navigate to the repository directory
cd fivetran_connector_sdk

# Add the original repository as an upstream remote
git remote add upstream https://github.com/fivetran/fivetran_connector_sdk.git
```

### Step 3: Create a feature branch

```bash
# Ensure your main branch is up to date
git checkout main
git pull upstream main

# Create a new branch for your feature or fix
git checkout -b feature/your-example-name
```

Branch Naming Convention: Use descriptive branch names such as:
- `feature/salesforce-connector`
- `fix/pagination-bug-in-hubspot`

### Step 4: Make your changes

1. Create or modify connector examples in the appropriate directory
2. Ensure your code follows our [coding standards](#contribution-standards-and-guidelines)
3. Test your connector thoroughly using `fivetran debug`
4. Add or update documentation as needed

### Step 5: Set up pre-commit hooks

To set up the pre-commit hook, execute `.github/scripts/setup-hooks.sh` from the root of the repository for automatic code formatting

### Step 6: Commit your changes

```bash
# Stage your changes
git add .

# Commit with a descriptive message
git commit -m "brief description of changes"
```

### Step 7: Push to your fork

```bash
# Push your branch to your forked repository
git push origin feature/your-example-name
```

### Step 8: Create a pull request

1. Navigate to your forked repository on GitHub
2. Click the `Compare & pull request` button
3. Ensure the base repository is `fivetran/fivetran_connector_sdk` and the base branch is `main`
4. Fill out the pull request template with:
   - Clear title message
   - Detailed description of your changes
   - Screenshot of `fivetran debug` output (see [Testing Requirements](#testing-requirements))
   - Any additional context for reviewers

### Step 9: Keep your branch updated

```bash
# Sync your fork with the upstream repository
git fetch upstream
git checkout main
git merge upstream/main
git push origin main

# Rebase your feature branch if needed
git checkout feature/your-example-name
git rebase main
```

## Contribution standards and guidelines

All contributions must adhere to our coding standards and principles to ensure consistency, maintainability, and quality across the repository.

### Required reading

Before submitting a pull request, thoroughly review these documents:

1. [Python Coding Standards](https://github.com/fivetran/fivetran_connector_sdk/tree/main/PYTHON_CODING_STANDARDS.md): Comprehensive guidelines for Python code style, naming conventions, and best practices
2. [Fivetran Coding Principles](https://github.com/fivetran/fivetran_connector_sdk/tree/main/FIVETRAN_CODING_PRINCIPLES.md): High-level principles for code reviews, PR guidelines, and example development
3. [Connector SDK Best Practices](https://fivetran.com/docs/connector-sdk/best-practices): Official documentation on best practices

### Documentation requirements

Every new connector example contribution must include proper documentation:

#### README.md

Each new connector example must include a `README.md` file in its directory. Refer to the [template connector](https://github.com/fivetran/fivetran_connector_sdk/tree/main/template_connector) for structure and formatting.

#### Root README.md update

When adding a new connector example, you must update the root [README.md](https://github.com/fivetran/fivetran_connector_sdk/tree/main/README.md) file to include your example in the appropriate section.


## Review process

All pull requests go through a multi-stage review process to ensure quality and consistency:

### Stage 1: Automated checks

When you submit a pull request, automated checks will run:

- Code Quality Check: Flake8 linting and Black formatting validation
- README Update Check: Verifies that the root README.md is updated for new examples
- CLA Verification: Confirms that you have signed the Contributor License Agreement

These checks must pass before human review begins.

### Stage 2: GitHub copilot review

GitHub Copilot performs an initial automated review to:

- Identify logical and formatting issues
- Detect potential bottlenecks and performance problems
- Ensure code follows Fivetran Connector SDK guidelines
- Check for common security vulnerabilities

Address any feedback from Copilot before requesting human review. If you disagree with any suggestions generated by Copilot, you can provide a clear explanation in the comments to facilitate discussion.

### Stage 3: Human review

Your pull request requires two approvals from the Fivetran team:

#### Documentation review

A Fivetran team member will review:
- `README.md` completeness and clarity
- Alignment with documentation standards

#### Code review

A Fivetran team member will review:
- Code quality and architecture
- Adherence to coding standards
- Functionality and correctness
- Performance considerations
- Security best practices


### Addressing review feedback

1. Respond to Comments: Acknowledge each review comment
2. Make Changes: Update your code based on feedback
3. Push Updates: Commit and push changes to your branch
4. Request Re-review: Once all feedback is addressed, request re-review from reviewers
5. Resolve Conversations: Mark conversations as resolved when addressed

## Pull request checks

Your pull request must pass the following automated checks before it can be merged:

### 1. Python code quality check

This check uses Flake8 and Black to ensure code quality and formatting:

#### Flake8 linting

This check identifies potential errors, stylistic issues, and deviations from PEP8 python coding standards. It uses `.flake8` config file at the repository root. The check only analyzes Python files modified in your pull request and fails if any flake8 errors are detected.

#### Black formatting

This check ensures that all Python code adheres to the Black code formatter standards defined in the repository. It checks all Python files in the repository for compliance with Black formatting rules and fails if any formatting issues are detected.

To fix formatting issues locally:
```bash
# Run this command from the root of the repository
./.github/scripts/fix-python-formatting.sh
```

Or manually:
```bash
# Install tools
pip install flake8 black

# Check and fix formatting
black --line-length 99 .

# Check linting
flake8 .
```

### 2. README update check

This check ensures that new examples are documented in the root `README.md`. This check runs when your pull request adds new directories. This check fails if new directories are added without updating the root `README.md` to document them.

What to update in `README.md`:
- Add your connector to the appropriate section (Community Connectors, Quickstart Examples, etc.)
- Include a brief description
- Link to your connector's directory

### 3. Contributor license agreement

All contributors must sign the Fivetran CLA before their contributions can be accepted.

#### How the CLA works

- First-Time Contributors: When you submit your first pull request, the CLA bot will automatically comment on your PR
- Sign the CLA: Follow the link provided by the bot to sign the CLA electronically
- Automatic Verification: Once signed, the bot will automatically verify and update your PR status
- One-Time Process: You only need to sign the CLA once for all future contributions

## Testing requirements

All connector contributions must be thoroughly tested before submission. Testing ensures that your connector works correctly and helps maintain the quality of the repository.

### Local testing with `fivetran debug`

Before submitting your pull request, you must test your connector locally using the `fivetran debug` command:

#### Step 1: Install the connector SDK

```bash
pip install fivetran-connector-sdk
```

#### Step 2: Test your connector

```bash
# Navigate to your connector directory
cd connectors/your_connector_name

# Run the debug command
fivetran debug

# If you connector uses configuration file, run
fivetran debug --configuration=configuration.json
```

#### Step 3: Verify the output

After running `fivetran debug`, verify that the output indicates a successful run without errors. You should also verify the warehouse.db file to ensure data has been extracted and loaded correctly. For more information on working with the warehouse.db file, refer to the [Connector SDK documentation](https://fivetran.com/docs/connector-sdk/working-with-connector-sdk#workingwithwarehousedb).

#### Step 4: Take a screenshot and add to your PR

Capture a screenshot of the successful `fivetran debug` output and attach the screenshot in the PR description.


## Need help?

If you have questions or need assistance while contributing, you can use PR comments to ask questions or request clarification about the PR. For issues that can't be resolved through PR comments, You can reach out to our [Support team](https://support.fivetran.com/hc/en-us).

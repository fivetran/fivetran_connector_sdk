const { Octokit } = require("@octokit/core");
const { paginateRest } = require("@octokit/plugin-paginate-rest");
const minimatch = require("minimatch");
const fs = require("fs");
const path = require("path");

// Setup Octokit with pagination plugin
const MyOctokit = Octokit.plugin(paginateRest);

const token = process.env.GITHUB_TOKEN;
const [owner, repo] = process.env.GITHUB_REPOSITORY.split("/");
const pull_number = parseInt(process.env.PULL_REQUEST_NUMBER, 10);

const octokit = new MyOctokit({ auth: token });

async function main() {
  console.log(`🔍 Checking CODEOWNER approval for PR #${pull_number} in ${owner}/${repo}`);

  // Get PR info
  const { data: pr } = await octokit.request("GET /repos/{owner}/{repo}/pulls/{pull_number}", {
    owner,
    repo,
    pull_number,
  });

  // Get changed files
  const files = await octokit.paginate(
    "GET /repos/{owner}/{repo}/pulls/{pull_number}/files",
    { owner, repo, pull_number }
  );
  const changedFiles = files.map(f => f.filename);

  console.log("📂 Changed files:");
  changedFiles.forEach(f => console.log(`- ${f}`));

  // Check for CODEOWNERS file
  const codeownersPath = path.join(process.cwd(), "CODEOWNERS");
  if (!fs.existsSync(codeownersPath)) {
    console.log("⚠️ No CODEOWNERS file found. Skipping approval check.");
    return;
  }

  // Parse CODEOWNERS
  const codeownersText = fs.readFileSync(codeownersPath, "utf8");
  const codeownersRules = codeownersText
    .split("\n")
    .filter(line => line.trim() && !line.startsWith("#"))
    .map(line => {
      const [pattern, ...owners] = line.trim().split(/\s+/);
      return {
        pattern: pattern.startsWith("/") ? pattern.slice(1) : pattern,
        owners: owners.map(o => o.replace(/^@/, "").toLowerCase()),
      };
    });

  // Match files against CODEOWNERS
  const requiredOwners = new Set();
  for (const file of changedFiles) {
    for (const rule of codeownersRules) {
      if (minimatch(file, rule.pattern, { dot: true })) {
        rule.owners.forEach(owner => requiredOwners.add(owner));
      }
    }
  }

  if (requiredOwners.size === 0) {
    console.log("✅ No matching CODEOWNERS for the changed files. Skipping approval check.");
    return;
  }

  console.log("👥 Required CODEOWNERS:");
  console.log([...requiredOwners].join(", "));

  // Get PR reviews
  const { data: reviews } = await octokit.request(
    "GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews",
    { owner, repo, pull_number }
  );

  const approvedBy = new Set(
    reviews
      .filter(r => r.state === "APPROVED")
      .map(r => r.user.login.toLowerCase())
  );

  const isApproved = [...requiredOwners].some(owner => approvedBy.has(owner));

  if (!isApproved) {
    console.error(
      `❌ PR must be approved by at least one matching CODEOWNER: ${[...requiredOwners].join(", ")}`
    );
    process.exit(1);
  } else {
    console.log("✅ PR approved by a required CODEOWNER.");
  }
}

main().catch(err => {
  console.error("❌ Error checking CODEOWNER approval:", err);
  process.exit(1);
});

#!/bin/bash

# Define project directory
PROJECT_DIR="/Users/nileshhanotia/Projects/Firebase-ai-bot_1"

# Navigate to the project directory
cd "$PROJECT_DIR" || { echo "Directory not found!"; exit 1; }

# Initialize Git if not already done
if [ ! -d ".git" ]; then
    echo "Initializing Git repository..."
    git init
fi

# Add remote repository
REMOTE_URL="https://github.com/neilh44/NL_SQL" # Replace with your GitHub repository URL
git remote add origin "$REMOTE_URL" 2>/dev/null || echo "Remote already exists."

# Check for changes
git add .
git commit -m "Initial commit for Firebase AI Bot project"

# Push to GitHub
DEFAULT_BRANCH="main" # Change to 'master' if using the old naming convention
git push -u origin "$DEFAULT_BRANCH" || {
    echo "Push failed. Check authentication or branch name.";
    exit 1;
}

echo "Code successfully pushed to GitHub!"

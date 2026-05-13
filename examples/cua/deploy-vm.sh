#!/bin/bash
# deploy-vm.sh - Quick deployment script for CUA server on a remote VM
# Usage: ./deploy-vm.sh <user@host> <repo-url> [worktree-path] [server-port]
#
# Examples:
#   ./deploy-vm.sh ubuntu@1.2.3.4 https://github.com/user/quicrtc.git
#   ./deploy-vm.sh ubuntu@1.2.3.4 https://github.com/user/quicrtc.git my-branch-worktree 4444
#
# Leave worktree-path empty (the default) to build from the cloned
# repo root. Pass a path only if you're testing a feature branch via
# `git worktree add`.

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <user@host> <repo-url> [worktree-path] [server-port]"
    echo "Example: $0 ubuntu@1.2.3.4 https://github.com/user/quicrtc.git"
    exit 1
fi

HOST="$1"
REPO_URL="$2"
WORKTREE_PATH="${3:-}"
SERVER_PORT="${4:-4444}"

echo "Deploying CUA server to $HOST..."
echo "Repo: $REPO_URL"
if [ -n "$WORKTREE_PATH" ]; then
    echo "Worktree path: $WORKTREE_PATH"
fi
echo "Server port: $SERVER_PORT"
echo ""

# Install Go 1.25.x to match go.mod's go directive.
ssh "$HOST" bash -s << EOF
set -e

GO_VERSION=1.25.3
if ! command -v go &> /dev/null || [[ "\$(go version)" != *"go1.25"* ]]; then
    echo "Installing Go \$GO_VERSION..."
    wget -q https://go.dev/dl/go\${GO_VERSION}.linux-amd64.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf go\${GO_VERSION}.linux-amd64.tar.gz
    export PATH=\$PATH:/usr/local/go/bin
    grep -q '/usr/local/go/bin' ~/.bashrc || echo 'export PATH=\$PATH:/usr/local/go/bin' >> ~/.bashrc
else
    echo "Go already installed: \$(go version)"
    export PATH=\$PATH:/usr/local/go/bin
fi

# Clone or update the repo
if [ -d quicrtc ]; then
    echo "Updating existing repo..."
    cd quicrtc
    git fetch
    git pull
else
    echo "Cloning repo..."
    git clone "$REPO_URL" quicrtc
    cd quicrtc
fi

# Optionally enter a worktree (for testing feature branches).
if [ -n "$WORKTREE_PATH" ]; then
    if [ -d "$WORKTREE_PATH" ]; then
        echo "Using existing worktree: $WORKTREE_PATH"
        cd "$WORKTREE_PATH"
    else
        echo "Worktree not found at $WORKTREE_PATH. Creating from main..."
        git worktree add "$WORKTREE_PATH" main
        cd "$WORKTREE_PATH"
    fi
fi

# Build the server to verify it compiles
echo "Building server..."
go build ./examples/cua/server

echo ""
echo "Deployment complete!"
echo ""
echo "To run the server:"
if [ -n "$WORKTREE_PATH" ]; then
    echo "  cd quicrtc/$WORKTREE_PATH"
else
    echo "  cd quicrtc"
fi
echo "  go run ./examples/cua/server -mode=multistream -stress -bind=0.0.0.0:$SERVER_PORT"
echo ""
echo "Then from your local machine:"
echo "  go run ./examples/cua/client -turns=200 -turn-gap=10ms 'https://<PUBLIC_IP>:$SERVER_PORT/wt#slug=...&hash=...'"
EOF

echo ""
echo "Deployment script completed successfully."
echo "SSH into $HOST to start the server manually, or add auto-start logic to the script."

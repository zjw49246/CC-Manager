#!/bin/bash
# Start both backend and frontend dev servers

cd "$(dirname "$0")/.."

# Load .env so we can read TOKEN_MANAGER_* vars
if [ -f .env ]; then
  export $(grep -v '^#' .env | grep -v '^$' | xargs)
fi

PORT=${PORT:-8000}
TOKEN_MANAGER_PORT=${TOKEN_MANAGER_PORT:-8001}
export PORT

echo "Starting Claude Code Manager..."
echo "Backend:  http://localhost:${PORT}"
echo "Frontend: http://localhost:5173"
if [ "${TOKEN_MANAGER_ENABLED}" = "true" ]; then
  echo "Token Mgr: http://localhost:${TOKEN_MANAGER_PORT}"
fi
echo ""

# Start backend
uv run uvicorn backend.main:app --host 0.0.0.0 --port "${PORT}" --reload &
BACKEND_PID=$!

# Start frontend
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
cd frontend && npx vite --host 0.0.0.0 --port 5173 &
FRONTEND_PID=$!

cd ..

# Handle Ctrl+C
trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT TERM

echo ""
echo "Both servers started. Press Ctrl+C to stop."
echo "Note: Token Usage Manager is managed by the backend process (not a separate dev process)."
wait

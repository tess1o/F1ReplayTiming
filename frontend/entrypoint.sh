#!/bin/sh
# Replace the build-time placeholder with runtime NEXT_PUBLIC_API_URL.
# If unset, keep API calls same-origin (frontend proxy handles /api and /ws).
RUNTIME_URL="${NEXT_PUBLIC_API_URL:-}"
PLACEHOLDER="__NEXT_PUBLIC_API_URL__"

find /app/.next -name "*.js" -exec sed -i "s|$PLACEHOLDER|$RUNTIME_URL|g" {} +

if [ -n "$RUNTIME_URL" ]; then
  echo "Configured API URL override: $RUNTIME_URL"
else
  echo "Using same-origin API/WebSocket proxy"
fi

exec "$@"

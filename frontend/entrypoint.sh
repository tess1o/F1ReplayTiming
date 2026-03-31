#!/bin/sh
# Replace the build-time placeholder with runtime NEXT_PUBLIC_API_URL.
# If unset, keep API calls same-origin (frontend proxy handles /api and /ws).
RUNTIME_URL="${NEXT_PUBLIC_API_URL:-}"
PLACEHOLDER="__NEXT_PUBLIC_API_URL__"
INTERNAL_BACKEND_URL="${BACKEND_INTERNAL_URL:-http://f1-backend:8000}"
INTERNAL_PLACEHOLDER_URL="http://__BACKEND_INTERNAL_URL__"

escape_sed() {
  printf '%s' "$1" | sed 's/[&|]/\\&/g'
}

RUNTIME_URL_ESCAPED="$(escape_sed "$RUNTIME_URL")"
INTERNAL_BACKEND_URL_ESCAPED="$(escape_sed "$INTERNAL_BACKEND_URL")"

find /app -type f \( -name "*.js" -o -name "*.json" \) -exec sed -i "s|$PLACEHOLDER|$RUNTIME_URL_ESCAPED|g" {} +
find /app -type f \( -name "*.js" -o -name "*.json" \) -exec sed -i "s|$INTERNAL_PLACEHOLDER_URL|$INTERNAL_BACKEND_URL_ESCAPED|g" {} +

if [ -n "$RUNTIME_URL" ]; then
  echo "Configured API URL override: $RUNTIME_URL"
else
  echo "Using same-origin API/WebSocket proxy"
fi
echo "Configured internal proxy target: $INTERNAL_BACKEND_URL"

exec "$@"

#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/release-tag.sh [options] <tag> [commit]

Creates an annotated git tag and pushes it to remote.

Arguments:
  <tag>        Tag name (example: v1.2.3)
  [commit]     Optional target commit (default: HEAD)

Options:
  -m, --message <msg>   Tag message (default: "Release <tag>")
  -r, --remote <name>   Remote name (default: origin)
  -f, --force           Override existing tag locally/remotely
  -n, --dry-run         Print commands without executing
  -h, --help            Show this help

Examples:
  scripts/release-tag.sh v1.4.0
  scripts/release-tag.sh v1.4.0 a1b2c3d
  scripts/release-tag.sh --force -m "Re-release v1.4.0 with hotfix" v1.4.0
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

cmd() {
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "+ $*"
  else
    "$@"
  fi
}

FORCE=0
DRY_RUN=0
REMOTE="origin"
MESSAGE=""

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--force)
      FORCE=1
      shift
      ;;
    -n|--dry-run)
      DRY_RUN=1
      shift
      ;;
    -r|--remote)
      [[ $# -ge 2 ]] || die "Missing value for $1"
      REMOTE="$2"
      shift 2
      ;;
    -m|--message)
      [[ $# -ge 2 ]] || die "Missing value for $1"
      MESSAGE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      while [[ $# -gt 0 ]]; do
        POSITIONAL+=("$1")
        shift
      done
      ;;
    -*)
      die "Unknown option: $1"
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

set -- "${POSITIONAL[@]}"

[[ $# -ge 1 && $# -le 2 ]] || {
  usage
  exit 1
}

TAG="$1"
TARGET="${2:-HEAD}"

if [[ -z "$MESSAGE" ]]; then
  MESSAGE="Release ${TAG}"
fi

git rev-parse --git-dir >/dev/null 2>&1 || die "Not inside a git repository"
git rev-parse --verify "${TARGET}^{commit}" >/dev/null 2>&1 || die "Target commit not found: ${TARGET}"

if [[ "$FORCE" == "0" ]]; then
  if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null 2>&1; then
    die "Local tag already exists: ${TAG}. Use --force to override."
  fi
  if git ls-remote --exit-code --tags "$REMOTE" "refs/tags/${TAG}" >/dev/null 2>&1; then
    die "Remote tag already exists on ${REMOTE}: ${TAG}. Use --force to override."
  fi
  cmd git tag -a "$TAG" "$TARGET" -m "$MESSAGE"
  cmd git push "$REMOTE" "refs/tags/${TAG}"
  echo "Tag pushed: ${TAG} -> ${REMOTE}"
  exit 0
fi

echo "Force mode enabled: tag '${TAG}' will be overwritten on ${REMOTE}"
cmd git tag -fa "$TAG" "$TARGET" -m "$MESSAGE"
cmd git push --force "$REMOTE" "refs/tags/${TAG}"
echo "Tag force-pushed: ${TAG} -> ${REMOTE}"

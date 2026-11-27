#!/usr/bin/env bash
# Checks age and known vulnerabilities (OSV) of packages in uv.lock files.
# See readme.md for details.
set -uo pipefail

OSV_URL="${OSV_URL:-https://api.osv.dev/v1/query}"
DEFAULT_SPAN="1 day"

usage() {
    cat <<USAGE
Usage: ${0##*/} [--lock <uv.lock>] [--exclude-newer "<timedelta>"]

  --lock <file>              check only this lock file (default: all uv.lock in the git repo)
  --exclude-newer <delta>    min package age, e.g. "1 day", "12 hours", "1w", "P2D".
                             Default: [options] exclude-newer-span of the lock, else "1 day".
Exit code: 0 all passed, 1 any issue or error.
USAGE
}

die() { echo "ERROR: $*" >&2; exit 1; }

# timedelta -> seconds (stdout). Supports "<n> <unit>" (several allowed) and ISO8601 PnWnDTnHnMnS.
parse_span() {
    local s="${1,,}" total=0 n u
    s="${s//,/ }"
    if [[ "$s" =~ ^p([0-9]+w)?([0-9]+d)?(t([0-9]+h)?([0-9]+m)?([0-9]+s)?)?$ && "$s" != "p" && "$s" != *t ]]; then
        local part
        for part in $(grep -oE '[0-9]+[wdhms]' <<<"${s/t/ t}" | tr '\n' ' '); do
            n="${part%?}"; u="${part: -1}"
            # in the time part m means minutes (date part has no m)
            total=$((total + n * $(unit_secs "$u")))
        done
        echo "$total"; return 0
    fi
    local found=0 rest="$s"
    while [[ "$rest" =~ ^[[:space:]]*([0-9]+)[[:space:]]*([a-z]+)(.*)$ ]]; do
        n="${BASH_REMATCH[1]}"; u="${BASH_REMATCH[2]}"; rest="${BASH_REMATCH[3]}"
        total=$((total + n * $(unit_secs "$u") )) || return 1
        found=1
    done
    [[ $found == 1 && -z "${rest//[[:space:]]/}" ]] || return 1
    echo "$total"
}

unit_secs() {
    case "$1" in
        s|sec|secs|second|seconds) echo 1 ;;
        m|min|mins|minute|minutes) echo 60 ;;
        h|hr|hrs|hour|hours) echo 3600 ;;
        d|day|days) echo 86400 ;;
        w|week|weeks) echo 604800 ;;
        *) echo "unknown time unit '$1'" >&2; return 1 ;;
    esac
}

# lock file on stdin -> lines "name<TAB>version<TAB>earliest-upload-time" (registry packages only)
list_packages() {
    awk '
        function flush() { if (name != "" && reg) print name "\t" version "\t" earliest; name=""; version=""; earliest=""; reg=0 }
        /^\[\[package\]\]/ { flush(); inpkg=1; next }
        /^\[/ { flush(); inpkg=0; next }
        !inpkg { next }
        /^name = /    { split($0, a, "\""); name=a[2] }
        /^version = / { split($0, a, "\""); version=a[2] }
        /^source = .*registry/ { reg=1 }
        {
            line=$0
            while (match(line, /upload-time = "[^"]+"/)) {
                t=substr(line, RSTART+15, RLENGTH-16)
                if (earliest == "" || t < earliest) earliest=t   # ISO strings: lexicographic is fine
                line=substr(line, RSTART+RLENGTH)
            }
        }
        END { flush() }
    '
}

lock_span() {  # prints exclude-newer-span from [options], if any
    awk '/^\[/ { o = ($0 == "[options]") ; next } o && /^exclude-newer-span *=/ { split($0, a, "\""); print a[2]; exit }' "$1"
}

base_ref() {
    local r
    for r in main master origin/main origin/master; do
        git rev-parse --verify -q "$r^{commit}" >/dev/null && { echo "$r"; return; }
    done
    return 1
}

LOCK_ARG="" SPAN_ARG=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --lock) [[ $# -ge 2 ]] || die "--lock needs a value"; LOCK_ARG="$2"; shift 2 ;;
        --exclude-newer) [[ $# -ge 2 ]] || die "--exclude-newer needs a value"; SPAN_ARG="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; die "unknown argument: $1" ;;
    esac
done

command -v git >/dev/null || die "git not found"
command -v curl >/dev/null || die "curl not found"
command -v jq >/dev/null || die "jq not found"
ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || die "not inside a git repository (cwd: $PWD)"

if [[ -n "$LOCK_ARG" ]]; then
    [[ -f "$LOCK_ARG" ]] || die "lock file not found: $LOCK_ARG"
    LOCKS=("$(realpath "$LOCK_ARG")")
else
    mapfile -t LOCKS < <(git -C "$ROOT" ls-files -co --exclude-standard -- 'uv.lock' '**/uv.lock' | sort -u | sed "s|^|$ROOT/|")
    [[ ${#LOCKS[@]} -gt 0 ]] || die "no uv.lock files found in git repo $ROOT"
fi

# Branch / base determination
BRANCH="$(git -C "$ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null)" || die "cannot determine current branch"
ON_BASE=0
[[ "$BRANCH" == main || "$BRANCH" == master ]] && ON_BASE=1
BASE=""
if [[ $ON_BASE == 0 ]]; then
    BASE="$(cd "$ROOT" && base_ref)" || die "on branch '$BRANCH' but no main/master branch found to compare against"
    echo "Branch '$BRANCH': checking only packages changed vs '$BASE'"
else
    echo "Branch '$BRANCH': checking all packages"
fi

NOW=$(date +%s)
ISSUES=()
TOTAL=0

for lock in "${LOCKS[@]}"; do
    rel="${lock#"$ROOT"/}"
    echo "== $rel"
    [[ -r "$lock" ]] || die "cannot read $lock"
    grep -q '^\[\[package\]\]' "$lock" || die "$rel: no [[package]] entries; not a valid uv.lock?"

    span="${SPAN_ARG:-$(lock_span "$lock")}"
    span="${span:-$DEFAULT_SPAN}"
    span_s="$(parse_span "$span")" || die "invalid timedelta '$span'"
    cutoff=$((NOW - span_s))
    echo "   min age: $span"

    pkgs="$(list_packages <"$lock")"
    if [[ $ON_BASE == 0 ]]; then
        old=""
        if [[ "$lock" == "$ROOT"/* ]] && old_content="$(git -C "$ROOT" show "$BASE:$rel" 2>/dev/null)"; then
            old="$(list_packages <<<"$old_content" | cut -f1,2)"
        fi
        pkgs="$(awk -F'\t' 'NR==FNR { seen[$1 FS $2]=1; next } !(($1 FS $2) in seen)' <(printf '%s\n' "$old") <(printf '%s\n' "$pkgs"))"
    fi
    [[ -n "$pkgs" ]] || { echo "   no packages to check"; continue; }

    while IFS=$'\t' read -r name version uploaded; do
        [[ -n "$name" ]] || continue
        TOTAL=$((TOTAL + 1))
        # 1/ age
        if [[ -z "$uploaded" ]]; then
            ISSUES+=("$rel: $name==$version: no upload-time in lock, cannot verify age")
        else
            up_s="$(date -d "$uploaded" +%s 2>/dev/null)" || die "$rel: bad upload-time '$uploaded' for $name"
            if (( up_s > cutoff )); then
                ISSUES+=("$rel: $name==$version: TOO NEW (uploaded $uploaded, min age $span)")
            fi
        fi
        # 2/ vulnerabilities
        payload="$(jq -nc --arg n "$name" --arg v "$version" '{version:$v, package:{name:$n, ecosystem:"PyPI"}}')"
        resp="$(curl -sS --max-time 30 -XPOST --data "$payload" -w '\n%{http_code}' "$OSV_URL" 2>&1)" \
            || die "OSV communication failure for $name==$version: ${resp%%$'\n'*}"
        code="${resp##*$'\n'}"; body="${resp%$'\n'*}"
        if [[ "$code" == 404 ]]; then
            ISSUES+=("$rel: $name==$version: NOT FOUND in OSV (HTTP 404)")
        elif [[ ! "$code" =~ ^2 ]]; then
            die "OSV communication failure for $name==$version: HTTP $code: $body"
        elif ! jq -e . >/dev/null 2>&1 <<<"$body"; then
            die "OSV returned invalid JSON for $name==$version: $body"
        elif [[ "$(jq -c . <<<"$body")" != "{}" ]]; then
            ids="$(jq -r '[.vulns[]?.id] | join(", ")' <<<"$body")"
            ISSUES+=("$rel: $name==$version: VULNERABILITIES: ${ids:-unexpected response: $(jq -c . <<<"$body" | head -c 200)}")
        fi
    done <<<"$pkgs"
done

echo
echo "Checked $TOTAL package(s) in ${#LOCKS[@]} lock file(s)."
if [[ ${#ISSUES[@]} -eq 0 ]]; then
    echo "OK: all checks passed"
    exit 0
fi
echo "FAILED: ${#ISSUES[@]} issue(s):"
printf '  - %s\n' "${ISSUES[@]}"
exit 1

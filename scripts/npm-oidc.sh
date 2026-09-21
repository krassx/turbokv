#!/usr/bin/env bash
#
# Configure npm trusted publishing (OIDC) for this package, and retire the
# bootstrap token afterwards.
#
# WHY A SCRIPT AND NOT A README SECTION: the order matters and two of the steps
# are irreversible. A trusted publisher can only be attached to a package that
# ALREADY EXISTS on the registry, so the first version has to be published with
# a token -- and that token then has to be destroyed, because it is a
# long-lived credential whose whole purpose has just been taken over by OIDC.
# Doing those in the wrong order leaves either a package nobody can publish to
# or a token nobody remembers to revoke.
#
# Authentication is npm's browser OAuth flow (`npm login --auth-type=web`), and
# `npm trust` additionally demands an interactive 2FA challenge every time --
# deliberately, on npm's side: a granular token with "bypass 2FA" is REFUSED for
# trust operations, so this cannot be made unattended and should not be.
#
# Usage:
#   scripts/npm-oidc.sh              # run every step, skipping what is already done
#   scripts/npm-oidc.sh --check      # report state, change nothing
#   scripts/npm-oidc.sh --from 3     # resume at step 3
#
# Every step is idempotent: re-running after a failure picks up where it stopped.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

WORKFLOW_FILE="release.yml"                 # the name npm trust records, not a path
WORKFLOW_PATH=".github/workflows/$WORKFLOW_FILE"
BOOTSTRAP_SECRET="NPM_TOKEN"
NPM_MIN="11.5.1"                            # npm's floor for OIDC publishing
NODE_MIN="22.14.0"                          # and Node's

CHECK_ONLY=0
FROM_STEP=1
while [ $# -gt 0 ]; do
    case "$1" in
        --check) CHECK_ONLY=1 ;;
        --from) FROM_STEP="${2:?--from needs a step number}"; shift ;;
        -h|--help) sed -n '2,31p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done

say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
ok()   { printf '  \033[32mok\033[0m    %s\n' "$*"; }
warn() { printf '  \033[33mnote\033[0m  %s\n' "$*"; }
die()  { printf '\n\033[31m%s\033[0m\n' "$*" >&2; exit 1; }

# Sort-based comparison, so 11.12.1 is correctly NEWER than 11.5.1 -- which a
# string or float compare gets backwards, and this script's whole job is gated
# on that answer.
version_ge() { [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -1)" = "$2" ]; }

confirm() {
    [ "$CHECK_ONLY" = 1 ] && { warn "--check: would ask -- $1"; return 1; }
    printf '\n  %s [y/N] ' "$1"
    read -r reply </dev/tty
    [ "$reply" = "y" ] || [ "$reply" = "Y" ]
}

step() { [ "$FROM_STEP" -le "$1" ]; }

PKG_NAME="$(node -p "require('./package.json').name")"
PKG_VERSION="$(node -p "require('./package.json').version")"
# The repository field is the single source of truth for owner/repo: a script
# that asks the user to retype it is a script that can be pointed at the wrong
# repository, which for a trusted publisher means handing publish rights away.
GH_REPO="$(node -p "
  const u = require('./package.json').repository?.url ?? '';
  const m = u.match(/github\.com[/:]([^/]+)\/([^/.]+)/);
  if (!m) { console.error('package.json repository.url is not a GitHub URL'); process.exit(1); }
  m[1] + '/' + m[2];
")"

say "turbokv npm OIDC setup -- $PKG_NAME@$PKG_VERSION -> $GH_REPO"

# ---------------------------------------------------------------- 1. preflight
if step 1; then
    say "1. Preflight"

    command -v npm >/dev/null || die "npm is not on PATH"
    command -v gh  >/dev/null || die "the GitHub CLI (gh) is not on PATH -- needed to trigger the bootstrap publish and delete the token secret"

    NPM_VERSION="$(npm --version)"
    version_ge "$NPM_VERSION" "$NPM_MIN" \
        || die "npm $NPM_VERSION is too old for OIDC; need >= $NPM_MIN. Run: npm install -g npm@latest"
    ok "npm $NPM_VERSION (>= $NPM_MIN)"

    NODE_VERSION="$(node --version)"; NODE_VERSION="${NODE_VERSION#v}"
    version_ge "$NODE_VERSION" "$NODE_MIN" \
        || die "node $NODE_VERSION is too old for OIDC; need >= $NODE_MIN"
    ok "node $NODE_VERSION (>= $NODE_MIN)"

    npm trust --help >/dev/null 2>&1 \
        || die "this npm has no 'trust' command, so it cannot configure a trusted publisher"
    ok "npm trust is available"

    gh auth status >/dev/null 2>&1 || die "gh is not authenticated -- run: gh auth login"
    ok "gh is authenticated"

    [ -f "$WORKFLOW_PATH" ] || die "$WORKFLOW_PATH does not exist -- npm trust records a workflow FILE NAME, and it has to be real"
    ok "$WORKFLOW_PATH exists"

    # The publish job cannot mint an OIDC token without this, and the failure
    # mode is a publish that falls back to a token or fails at the registry --
    # long after this script said everything was fine.
    grep -q 'id-token: write' "$WORKFLOW_PATH" \
        || die "$WORKFLOW_PATH does not grant 'id-token: write' -- OIDC cannot work without it"
    ok "the workflow grants id-token: write"
fi

# ------------------------------------------------------------- 2. OAuth log in
if step 2; then
    say "2. Authenticate with npm (browser OAuth)"

    if NPM_USER="$(npm whoami 2>/dev/null)"; then
        ok "already logged in as $NPM_USER"
    elif [ "$CHECK_ONLY" = 1 ]; then
        warn "--check: not logged in; would run npm login --auth-type=web"
    else
        info "opening a browser for npm's OAuth flow"
        npm login --auth-type=web
        NPM_USER="$(npm whoami)"
        ok "logged in as $NPM_USER"
    fi
fi

# ------------------------------------------- 3. the package has to exist first
PKG_EXISTS=0
if step 3; then
    say "3. Does the package exist on the registry?"

    if npm view "$PKG_NAME" version >/dev/null 2>&1; then
        PKG_EXISTS=1
        ok "$PKG_NAME is published (latest: $(npm view "$PKG_NAME" version))"
    else
        warn "$PKG_NAME is not on the registry yet"
        info "A trusted publisher attaches to an EXISTING package, so the first"
        info "version must be published with the bootstrap token. That publish has"
        info "to run in CI rather than from here: the tarball ships prebuilt"
        info "binaries for six platform targets, and this machine can only build"
        info "one of them. A local 'npm publish' would ship a package that"
        info "compiles from source everywhere else -- and on Bun, not at all."
        echo

        if [ "$CHECK_ONLY" = 1 ]; then
            warn "--check: would offer to trigger the bootstrap publish"
        elif ! gh secret list --repo "$GH_REPO" 2>/dev/null | grep -q "^$BOOTSTRAP_SECRET"; then
            die "the $BOOTSTRAP_SECRET secret is gone from $GH_REPO, so the bootstrap publish cannot authenticate.
Add a granular npm token with publish rights as $BOOTSTRAP_SECRET, or publish the first version by hand."
        elif confirm "Trigger the bootstrap publish of $PKG_NAME@$PKG_VERSION now? (npm publishes cannot be taken back)"; then
            info "dispatching release.yml with publish=true bootstrap=true"
            gh workflow run "$WORKFLOW_FILE" --repo "$GH_REPO" -f publish=true -f bootstrap=true
            sleep 6
            RUN_ID="$(gh run list --repo "$GH_REPO" --workflow "$WORKFLOW_FILE" --limit 1 --json databaseId --jq '.[0].databaseId')"
            info "watching run $RUN_ID (the prebuild matrix takes a few minutes)"
            gh run watch "$RUN_ID" --repo "$GH_REPO" --exit-status || die "the bootstrap publish failed -- see the run log"
            npm view "$PKG_NAME" version >/dev/null 2>&1 \
                || die "the run succeeded but $PKG_NAME is still not on the registry"
            PKG_EXISTS=1
            ok "$PKG_NAME@$(npm view "$PKG_NAME" version) is published"
        else
            die "stopping: the trusted publisher cannot be configured until the package exists"
        fi
    fi
fi

# --------------------------------------------- 4. attach the trusted publisher
if step 4; then
    say "4. Configure the trusted publisher"

    if npm trust list "$PKG_NAME" 2>/dev/null | grep -qi 'github'; then
        ok "a GitHub Actions trusted publisher is already configured"
        npm trust list "$PKG_NAME" 2>/dev/null | sed 's/^/  /'
    elif [ "$CHECK_ONLY" = 1 ]; then
        warn "--check: no trusted publisher yet; would run npm trust github"
    elif [ "${PKG_EXISTS:-0}" = 0 ] && ! npm view "$PKG_NAME" version >/dev/null 2>&1; then
        die "the package does not exist yet -- re-run from step 3"
    else
        info "repository: $GH_REPO"
        info "workflow:   $WORKFLOW_FILE"
        info "environment: (none -- the publish job does not use one)"
        warn "npm will ask for a 2FA challenge in the browser. That is not optional:"
        warn "npm refuses trust operations from tokens that bypass 2FA."
        echo
        npm trust github "$PKG_NAME" --file "$WORKFLOW_FILE" --repo "$GH_REPO"
        ok "trusted publisher configured"
    fi
fi

# ------------------------------------------------------- 5. verify it took
if step 5; then
    say "5. Verify"

    if TRUST_JSON="$(npm trust list "$PKG_NAME" --json 2>/dev/null)"; then
        echo "$TRUST_JSON" | node -e "
          let s = ''; process.stdin.on('data', d => s += d).on('end', () => {
            let list; try { list = JSON.parse(s); } catch { console.log('  (unparseable response)'); process.exit(0); }
            const rows = Array.isArray(list) ? list : (list.objects ?? list.publishers ?? []);
            if (!rows.length) { console.log('  no trusted publisher is configured'); process.exit(1); }
            for (const r of rows) console.log('  ' + JSON.stringify(r));
          });
        " || die "no trusted publisher is configured for $PKG_NAME"
        ok "the registry reports a trusted publisher"
    else
        warn "could not read the trust list (are you still logged in?)"
    fi
fi

# ------------------------------------------ 6. retire the bootstrap credential
if step 6; then
    say "6. Retire the bootstrap token"

    if ! gh secret list --repo "$GH_REPO" 2>/dev/null | grep -q "^$BOOTSTRAP_SECRET"; then
        ok "$BOOTSTRAP_SECRET is already gone from $GH_REPO"
    elif [ "$CHECK_ONLY" = 1 ]; then
        warn "--check: $BOOTSTRAP_SECRET still exists and would be offered for deletion"
    else
        info "OIDC is configured, so the token is now a credential with no job and"
        info "an indefinite life. Deleting the secret also disarms the workflow's"
        info "bootstrap path, which is the point: it can never authenticate again."
        if confirm "Delete the $BOOTSTRAP_SECRET secret from $GH_REPO?"; then
            gh secret delete "$BOOTSTRAP_SECRET" --repo "$GH_REPO"
            ok "$BOOTSTRAP_SECRET deleted"
        else
            warn "left in place -- remember it outlives this script"
        fi
    fi

    echo
    warn "Deleting the secret does not revoke the token. Revoke it at the npm end too:"
    info "https://www.npmjs.com/settings/~/tokens"
fi

say "Done"
info "From here a release is: bump the version, tag it v<version>, push the tag."
info "The tag triggers release.yml, which publishes over OIDC with provenance"
info "and no credential anywhere in the repository."

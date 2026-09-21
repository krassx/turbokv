#!/usr/bin/env bash
#
# Configure npm trusted publishing (OIDC) for this package, and retire the
# token that predates it.
#
# WHY A SCRIPT AND NOT A README SECTION: the order matters and one of the steps
# is irreversible. A trusted publisher can only be attached to a package that
# ALREADY EXISTS on the registry, so something else has to create it -- and that
# something can no longer be a CI token. npm restricts tokens that bypass 2FA to
# STAGING a publish, and staging cannot bring a package into being; a token run
# fails with E_STAGE_REQUIRED. So the first version is published from a
# developer machine behind an interactive 2FA challenge, carrying binaries taken
# from a green CI run, and every version after it is published by the workflow
# over OIDC with provenance.
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
        info "A trusted publisher attaches to an EXISTING package, so some other"
        info "credential has to create this one. That credential cannot be a CI"
        info "token any more: npm restricts tokens that bypass 2FA to STAGING a"
        info "publish, and staging cannot bring a package into being. A token run"
        info "gets E_STAGE_REQUIRED -- 'this token can only publish to a staging"
        info "area, and \"$PKG_NAME\" does not exist yet'."
        echo
        info "So the first publish happens HERE, behind the interactive 2FA the"
        info "registry is asking for. The binaries do not: the tarball carries"
        info "prebuilds for six platform targets and this machine can build one,"
        info "so they are taken from a green release run, where each was built"
        info "on its own platform and ran the suite there."
        echo
        warn "This first version publishes WITHOUT provenance -- that attestation"
        warn "is signed from CI's OIDC identity, which a local publish has no way"
        warn "to present. Every release after this one is published by the"
        warn "workflow over OIDC and carries it."
        echo

        if [ "$CHECK_ONLY" = 1 ]; then
            warn "--check: would assemble prebuilds from CI and publish from here"
        else
            # THE BINARIES MUST COME FROM THE COMMIT BEING PUBLISHED, so the run
            # is chosen by head SHA and not by recency or by conclusion. Picking
            # "the newest successful run" would be wrong twice over: a release
            # run legitimately concludes FAILURE whenever its publish step has
            # nothing to do, and the newest run that did succeed could be from
            # any commit at all -- which is how a tarball ends up carrying
            # binaries that no longer match the source beside them.
            HEAD_SHA="$(git rev-parse HEAD)"
            RUN_ID="$(gh run list --repo "$GH_REPO" --workflow "$WORKFLOW_FILE" --limit 30 \
                --json databaseId,headSha --jq "[.[] | select(.headSha == \"$HEAD_SHA\")][0].databaseId // empty")"
            [ -n "$RUN_ID" ] || die "no $WORKFLOW_FILE run for $(git rev-parse --short HEAD), the commit being published.
Build one first:  gh workflow run $WORKFLOW_FILE --ref \$(git rev-parse --abbrev-ref HEAD)
With publish left off it builds and verifies every prebuild without publishing."

            # The tarball takes its JavaScript from this working tree and its
            # binaries from that run. If the tree has moved, they are not the
            # same package.
            git diff --quiet && git diff --cached --quiet \
                || die "the working tree has uncommitted changes -- the tarball would mix them with binaries built from $(git rev-parse --short HEAD)"

            info "taking prebuilds from run $RUN_ID, built at $(git rev-parse --short HEAD)"

            ART_DIR="$(mktemp -d)"
            gh run download "$RUN_ID" --repo "$GH_REPO" --dir "$ART_DIR" --pattern 'prebuild-*' \
                || die "could not download the prebuild artifacts (they expire after 7 days)"
            rm -rf prebuilds && mkdir -p prebuilds
            for d in "$ART_DIR"/prebuild-*/; do cp -R "$d"* prebuilds/; done
            rm -rf "$ART_DIR"

            COUNT="$(find prebuilds -name '*.node' | wc -l | tr -d ' ')"
            # The same count the workflow enforces before it publishes. A missing
            # platform is worse than no publish: that platform silently falls back
            # to a source build, and on Bun gets nothing at all.
            [ "$COUNT" -eq 6 ] || die "expected 6 prebuilds, assembled $COUNT"
            find prebuilds -name '*.node' | sort | sed 's/^/    /'
            ok "six prebuilds assembled"

            # Proves the tarball's binary is the one that gets loaded, not a
            # local build left over in build/ -- the check the workflow runs on
            # every platform, run here on this one.
            rm -rf build
            node -e "
              const p = require('node-gyp-build').path('.');
              if (!p.includes('prebuilds')) { console.error('resolved ' + p + ', not a prebuild'); process.exit(1); }
              if (typeof require('node-gyp-build')('.').create !== 'function') { console.error('addon has no create()'); process.exit(1); }
              console.log('    loaded ' + p);
            " || die "the assembled prebuild does not load"
            ok "it loads, and it is the prebuild that loaded"

            npm pack --dry-run
            if confirm "Publish $PKG_NAME@$PKG_VERSION from here? (npm publishes cannot be taken back)"; then
                npm publish --access public || die "the publish failed"
                PKG_EXISTS=1
                ok "$PKG_NAME@$PKG_VERSION is published"
            else
                die "stopping: the trusted publisher cannot be configured until the package exists"
            fi
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
        info "OIDC is configured, so the token is a credential with no job and an"
        info "indefinite life. It never had one to lose, in fact: npm refuses a"
        info "2FA-bypass token the create, and refuses it trust operations too."
        info "A secret that cannot do anything is still a secret that can leak."
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

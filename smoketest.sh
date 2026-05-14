#!/bin/bash

# This script will run a smoke test (fork OBS project -> upload tweaked dash
# package -> monitor build -> download results) of either the obo CLI or the
# gitlab runner.
#
# OBS credentials are passed via the environment variables:
# - OBS_SERVER
# - OBS_USER
# - OBS_PASSWORD
# with the same meaning as for the CLI/runner (as in README.md).
#
# The OBS project defaults to the apertis target project (containing its dash
# fork) but can be changed via --base-project. Similarly, the package and its
# source can be changed via --package or --apt-repo+--apt-dist+--apt-component,
# respectively.
#
# With all this in mind, if you stick with the default OBS project + package,
# testing the CLI just involves setting the OBS env vars and doing:
#
# ./smoketest.sh --target obo-cli
#
# and then optionally passing the customization flags as needed.
#
# For the GitLab runner (--target obs-gitlab-runner), it's a bit more involved.
# You also need:
#
# - A scratch GitLab repository to store the CI pipelines that will interact
#   with the runner (passed via --gitlab-repo).
# - A GitLab runner token (passed via $GITLAB_RUNNER_TOKEN).
# - Two runner tags:
#   - One for running basic shell commands (--gitlab-shell-runner-tag).
#   - One that's exclusively associated with obs-gitlab-runner (--gitlab-runner-tag).
#
# This will:
# - Start obs-gitlab-runner in the background,
#   connecting to the GitLab server inferred from the given repository URL (can
#   be overridden via --gitlab-server).
# - Push a CI pipeline using the given tags to the GitLab repository and print
#   the running pipeline URL.
# - Monitor the build progress.
# - Retrieve the save artifacts.
#
# You should check the pipeline as seen on GitLab to ensure logs are
# functioning.

set -euo pipefail

BINDIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")/target/debug

BASE_PROJECT=apertis:v2026:target
APT_REPO='https://repositories.apertis.org/apertis'
APT_DIST=v2026
APT_COMPONENT=target
PACKAGE=dash
DEBUG=''
GITLAB_RUNNER_TAG=obs-runner-test
GITLAB_SHELL_RUNNER_TAG=lightweight

CURLAUTH=(
  --variable %OBS_USER
  --variable %OBS_PASSWORD
  --expand-user '{{OBS_USER}}:{{OBS_PASSWORD}}'
)

read -rd '' FIND_PACKAGE <<'EOF' ||:
/^Package:/ && $2 == package { inpkg=1 }
inpkg && /^Files:$/ { infiles=1; next }
infiles && /^ / { print $3 }
infiles && /^[^ ]/ { infiles=0 }
inpkg && /^$/ { inpkg=0 }
EOF

die() {
  echo "$@" >&2
  exit 1
}

usage() {
  echo "usage: $0 <flags>"
  echo "  --target obo-cli|obs-gitlab-runner"
  echo "    The target of the smoke test. Required."
  echo "  --debug"
  echo "    Enable debug logging."
  echo "  --base-project[=$BASE_PROJECT]"
  echo "    The OBS project to fork and build the given package within."
  echo "  --apt-repo[=$APT_REPO], --apt-dist[=$APT_DIST], --apt-component[=$APT_COMPONENT]"
  echo "    The APT repository / dist / component to use to obtain the given source package."
  echo "  --package[=$PACKAGE]"
  echo "    The source package to build."
  echo "  --gitlab-repo"
  echo "    Use the given GitLab repository for testing. It will be WIPED in the process."
  echo "    (The OBS runner tag / token should already be set up on this repository.)"
  echo "  --gitlab-server"
  echo "    The server URL for the gitlab repository / runner."
  echo "    If not given, inferred from --gitlab-repo."
  echo "  --gitlab-runner-tag"
  echo "    The GitLab runner tag that \$GITLAB_RUNNER_TOKEN is associated with."
  echo "  --gitlab-shell-runner-tag"
  echo "    The GitLab runner tag to use for shell commands."
}

OPTS=$(getopt \
  -o ht: \
  --long help,target:,debug,base-project:,package:,apt-repo:,apt-dist:,apt-component:,gitlab-repo:,gitlab-server:,gitlab-runner-tag:,gitlab-shell-runner-tag: \
  -- "$@")

eval set -- "$OPTS"

while true; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    -t|--target) TARGET="$2"; shift 2 ;;
    --debug) DEBUG=1; shift ;;
    --base-project) BASE_PROJECT="$2"; shift 2 ;;
    --package) PACKAGE="$2"; shift 2 ;;
    --apt-repo) APT_REPO="$2"; shift 2 ;;
    --apt-dist) APT_DIST="$2"; shift 2 ;;
    --apt-component) APT_COMPONENT="$2"; shift 2 ;;
    --gitlab-server) GITLAB_SERVER="$2"; shift 2 ;;
    --gitlab-repo) GITLAB_REPO="$2"; shift 2 ;;
    --gitlab-runner-tag) GITLAB_RUNNER_TAG="$2"; shift 2 ;;
    --gitlab-shell-runner-tag) GITLAB_SHELL_RUNNER_TAG="$2"; shift 2 ;;
    --) shift; break ;;
    *) break ;;
  esac
done

[[ "$#" -eq 0 ]] || die 'This does not take positional arguments'
[[ -n "${TARGET:-}" ]] || die '--target must be specified'

case "$TARGET" in
  obo-cli) ;;
  obs-gitlab-runner)
    [[ -n "${GITLAB_REPO:-}" ]] || die '--gitlab-repo must be specified'
    ;;
  *) die "Invalid target: $TARGET" ;;
esac

needvars=(OBS_SERVER OBS_USER OBS_PASSWORD)
[[ "$TARGET" == "obs-gitlab-runner" ]] && needvars+=(GITLAB_RUNNER_TOKEN)
for var in "${needvars[@]}"; do
  [ -n "${!var:-}" ] || die "\$$var must be set"
done

needtools=(curl gawk jq)
[[ "$TARGET" == "obs-gitlab-runner" ]] && needtools+=(git glab)
for tool in "${needtools[@]}"; do
  command -v "$tool" >/dev/null || die "$tool must be installed"
done

cargo build -p "$TARGET"

MY_PROJECT="home:$OBS_USER:branches:obo-testing"

if ! curl -sf "${CURLAUTH[@]}" "$OBS_SERVER/source/$BASE_PROJECT" >/dev/null; then
  die "Failed to retrieve base project $BASE_PROJECT, are your credentials correct?"
fi

if curl -sf "${CURLAUTH[@]}" "$OBS_SERVER/source/$MY_PROJECT" >/dev/null; then
  echo "Going to delete existing project $MY_PROJECT!"
  for i in {1..3}; do
    echo $((4 - i))...
    sleep 1
  done
  curl -f -X DELETE "${CURLAUTH[@]}" "$OBS_SERVER/source/$MY_PROJECT"
fi

tmp=$(mktemp -d)
cleanup_tmp() { rm -rf "$tmp"; }
trap cleanup_tmp EXIT
cd "$tmp"

echo "Fetching source package URLs for $PACKAGE..."
SOURCEFILES=($(curl -fL "$APT_REPO/dists/$APT_DIST/$APT_COMPONENT/source/Sources" \
  | gawk -v package="$PACKAGE" "$FIND_PACKAGE"))

[[ "${#SOURCEFILES[@]}" -gt 0 ]] || die "Failed to find $PACKAGE in $APT_REPO"

SOURCEFETCH=(-fLZ)
for file in "${SOURCEFILES[@]}"; do
  SOURCEFETCH+=(-O "$APT_REPO/pool/$APT_COMPONENT/${PACKAGE:0:1}/$PACKAGE/$file")
done

case "$TARGET" in
  obo-cli)
    echo "Downloading ${#SOURCEFILES[@]} files..."
    curl "${SOURCEFETCH[@]}"

    # Modify the dsc slightly to force a new file upload.
    echo >> *.dsc

    [[ -n "$DEBUG" ]] && export OBO_LOG=obo_core=debug,obo_cli=debug

    export OBO="$BINDIR/obo"

    # Takes a single argument, a space-delimited command line where the first
    # "argument" is a logging prefix, and runs the command line while prepending
    # that prefix to the printed logs. Shell quoting in the command line doesn't
    # work correctly (i.e. the arguments & prefix are split on spaces,
    # regardless of whether or not they're quoted), but this is given simple
    # command lines from the obo cli, so that should be fine.
    read -rd '' CLI_MONITOR_WRAPPER <<'EOF' ||:
set -euo pipefail

# Force expand the single argument into multiple.
set -- $1

prefix="$1"
shift

mkdir -p "$prefix"
cd "$prefix"
(set -x; "$OBO" $@ 2>&1) | gawk -v prefix="$prefix" '{ print prefix ": " $0 }'
echo "$prefix: done"
EOF

    echo '===== Uploading'
    set -x
    "$OBO" dput --branch-to "$MY_PROJECT" "$BASE_PROJECT" *.dsc
    "$OBO" generate-monitor --download-build-results-to results/
    set +x

    cleanup_cli() {
      set +e
      (set -x; "$OBO" prune)
      cleanup_tmp
    }
    trap cleanup_cli EXIT

    echo '===== Generated monitor json:'
    jq < obs-monitor.json

    echo '===== Monitoring'
    parallel=$(jq -r '.entries | length' < obs-monitor.json)
    jq -r '.entries[] | "\(.repo)/\(.arch) \(.commands.monitor)"' < obs-monitor.json \
      | xargs -P "$parallel" -d $'\n' -n1 bash -c "$CLI_MONITOR_WRAPPER" -
    jq -r '.entries[] | "\(.repo)/\(.arch) \(.commands.download_binaries)"' < obs-monitor.json \
      | xargs -d $'\n' -n1 bash -c "$CLI_MONITOR_WRAPPER" -

    echo '===== Build results'
    ls -lR
    find . -name '*.log' -exec tail -n5 '{}' +
    ;;

  obs-gitlab-runner)
    if [[ -z "${GITLAB_SERVER:-}" ]]; then
      if [[ "$GITLAB_REPO" =~ @([^:/]+): || "$GITLAB_REPO" =~ ^[^:/]+://([^/]+)/ ]]; then
        GITLAB_SERVER="https://${BASH_REMATCH[1]}"
      else
        die 'Failed to infer --gitlab-server from --gitlab-repo'
      fi

      echo "Inferred GitLab server: $GITLAB_SERVER"
    fi

    echo "Cloning $GITLAB_REPO..."
    git clone "$GITLAB_REPO" repo
    cd repo

    branch=$(git symbolic-ref --short HEAD)
    echo "On branch: $branch"

    # Make sure glab is actually working in this repo.
    glab repo view >/dev/null

    glab_vars=$(glab variable list -F json | jq '.[].key')
    for var in OBS_SERVER OBS_USER OBS_PASSWORD; do
      if [[ "$var" == "OBS_PASSWORD" ]]; then
        # The password should be set as a masked variable.
        glab_flags=-rm
      else
        glab_flags=-r
      fi

      if jq -e 'select(. == $var)' --arg var "$var" >/dev/null <<<"$glab_vars"; then
        glab variable update "$glab_flags" "$var" "${!var}"
      else
        glab variable set "$glab_flags" "$var" "${!var}"
      fi
    done

    for file in "${SOURCEFILES[@]}"; do
      if [[ "$file" == *.dsc ]]; then
        dsc_filename="$file"
        break
      fi
    done

    cat <<EOF > .gitlab-ci.yml
stages:
  - sources
  - upload
  - obs
  - cleanup

sources:
  stage: sources
  tags:
    - $GITLAB_SHELL_RUNNER_TAG
  image: quay.io/curl/curl
  script:
    - curl ${SOURCEFETCH[@]@Q}
  artifacts:
    paths: $(jq -n '$ARGS.positional' --args "${SOURCEFILES[@]}")

upload:
  stage: upload
  tags:
    - $GITLAB_RUNNER_TAG
  needs: [sources]
  script:
    - dput --branch-to ${MY_PROJECT@Q} ${BASE_PROJECT@Q} ${dsc_filename@Q}
    - generate-monitor ${GITLAB_RUNNER_TAG@Q} --download-build-results-to results/
  after_script:
    - prune --ignore-missing-build-info --only-if-job-unsuccessful
  artifacts:
    paths:
      - build-info.*
      - obs.yml

obs:
  stage: obs
  needs: [upload]
  trigger:
    strategy: depend
    include:
      - artifact: obs.yml
        job: upload

cleanup:
  stage: cleanup
  tags:
    - $GITLAB_RUNNER_TAG
  needs:
    - upload
    - obs
  script:
    - prune
  rules:
    - when: always
EOF

    if ! git diff --exit-code; then
      git add .
      git commit -m 'Test commit'
      git push -o ci.skip
    fi

    [[ -n "$DEBUG" ]] && export OBS_RUNNER_LOG=obo_core=debug,obs_gitlab_runner=debug
    GITLAB_URL="$GITLAB_SERVER" \
      GITLAB_TOKEN="$GITLAB_RUNNER_TOKEN" \
      "$BINDIR/obs-gitlab-runner" &
    runner=$!

    cleanup_runner() {
      set +e
      kill -KILL $runner
      # Clean up the more sensitive OBS_PASSWORD variable.
      glab variable delete OBS_PASSWORD
      cleanup_tmp
    }
    trap cleanup_runner EXIT

    sleep 1
    kill -0 $runner 2>/dev/null || die 'obs-gitlab-runner startup failed'

    glab ci run -w

    echo 'Waiting for pipeline completion...'
    # GLAB_NO_PROMPT will avoid making it interactive:
    # https://gitlab.com/gitlab-org/cli/-/work_items/8171#note_3098682165
    # >/dev/null is because this uses ANSI codes to overwrite the build status
    # and thus can overwrite logs from the runner instead. (You can just watch
    # the build status in the UI anyway.)
    GLAB_NO_PROMPT=1 glab ci status -l >/dev/null

    echo 'Downloading artifacts...'
    glab job artifact "$branch" upload

    while read -r repo_arch; do
      echo "Downloading artifacts from $repo_arch..."
      glab job artifact main "obs-$repo_arch" -p "$repo_arch"
    done <<< $(jq -r '.enabled_repos[] | "\(.repo)-\(.arch)"' < build-info.json)

    echo '===== Build results'
    ls -lR
    find . -name '*.log' -exec tail -n5 '{}' +
    ;;
esac

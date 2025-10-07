# obs-gitlab-runner

This is a custom [GitLab Runner](https://docs.gitlab.com/runner/) implementation
providing a shell-like command language for starting, monitoring, and cleaning up
builds on [OBS](https://build.opensuse.org/), specifically targeting Debian
packages.

## Usage

For information on OBS authentication and the commands supported, see [the
project-wide README](../README.md).

### Sending Jobs

In order to send commands in a job to the runner, set the job's `tags:` to
include the tag you used during [the deployment](#deployment), e.g.:

```yaml
my-job:
  tags:
    - obs-runner  # <-- set to run on runners tagged "obs-runner"
  stage: some-stage
  script:
    # [...]
```

This will run all the commands inside `before_script`, `script`, and
`after_script` with the runner.

### Supported Syntax

A subset of shell syntax is supported for commands:

- Commands are split at spaces, but parts can be quoted, just like in the shell:
  ```bash
  some-command this-is-argument-1 "this entire string is argument 2"
  ```
- Variable substitution is supported, as well as the `${VARIABLE:-DEFAULT}`
  syntax to use the value `DEFAULT` if `$VARIABLE` is unset:
  ```bash
  some-command $a_variable ${a_variable_with_default:-this is used if unset}
  ```
  The variables are sourced from the pipeline-wide and job-specific variables
  set.
  - A significant departure from shell argument parsing is that **variable
    contents are auto-quoted, thus spaces inside a variable do not split
    arguments**. For example, given `MYVAR=a b c`, this:
    ```bash
    some-command $MYVAR
    ```
    is interpreted as:
    ```bash
    some-command 'a b c'
    ```
    *not*:
    ```bash
    some-command a b c
    ```
    There is no way to use a variable without auto-quoting its contents.

## Deployment

### Registering the Runner

In order to use the runner, you must first register it with your GitLab
instance. This requires the use of a registration token, which can be obtained
via the following steps:

- Enter the GitLab admin area.
- Navigate to Overview -> Runners.
- Click "Register an instance runner".
- Copy the registration token within.

(Per-group/-project registration tokens can also be retrieved from the CI/CD
settings of the group or project.)

With this token, you can now register the runner via the [GitLab
API](https://docs.gitlab.com/ee/api/runners.html#register-a-new-runner).

Example using curl:

```bash
curl --request POST "https://$GITLAB_SERVER_URL/api/v4/runners"  \
  --form description='OBS runner' \
  --form run_untagged=false \
  --form tag_list=obs-runner \
  --form token="$REGISTRATION_TOKEN"
```

httpie:

```bash
http --form POST "https://$GITLAB_SERVER_URL/api/v4/runners" \
  description='OBS runner' \
  run_untagged=false \
  tag_list=obs-runner \
  token="$REGISTRATION_TOKEN"
```

**It is critical that you set `run_untagged=false`,** otherwise this runner
will be used for *all* jobs that don't explicitly set a tag, rather than just
the jobs explicitly targeting the runner.

This API call will return a JSON object containing a `token` key, whose value
is a _runner token_ that is used by the runner to connect to GitLab.

### Docker

Docker images are built on every commit, available at
`ghcr.io/collabora/obs-gitlab-runner:main`. The entry point takes two arguments:

- The GitLab server URL.
- The runner token acquired previously.

Simple example usage via the Docker CLI:

```bash
$ docker run --rm -it ghcr.io/collabora/obs-gitlab-runner:main \
    "$GITLAB_SERVER_URL" "$GITLAB_RUNNER_TOKEN"
```

In addition, you can instead opt to set the `GITLAB_URL` and `GITLAB_TOKEN`
environment variables:

```bash
$ docker run --rm -it \
    -e GITLAB_URL="$GITLAB_SERVER_URL" \
    -e GITLAB_TOKEN="$GITLAB_RUNNER_TOKEN" \
    ghcr.io/collabora/obs-gitlab-runner:main
```

### Kubernetes

A [Helm](https://helm.sh/) chart has been provided in the `chart/` directory,
installable via:

```bash
$ helm install \
    --set-string gitlab.url="$GITLAB_SERVER_URL" \
    --set-string gitlab.token="$GITLAB_RUNNER_TOKEN" \
    obs-gitlab-runner chart
```

Upgrades can skip setting `gitlab.token` to re-use the previously set value:

```bash
$ helm upgrade \
    --set-string gitlab.url="$GITLAB_SERVER_URL" \
    obs-gitlab-runner chart
```

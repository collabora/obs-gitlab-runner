# OBS GitLab Runner

This is a custom [GitLab Runner](https://docs.gitlab.com/runner/) implementation
exposing a custom command language for starting, monitoring, and cleaning up
builds on [OBS](https://build.opensuse.org/), specifically targeting Debian
packages.

## Usage

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

### Required Environment

In order to connect to OBS, three variables must be set (generally within the
"CI/CD" section of the settings):

- `OBS_SERVER`: The URL of the OBS instance, e.g. `https://obs.somewhere.com/`.
- `OBS_USER`: The username used to authenticate with OBS (any commits created
  will also be under this user).
- `OBS_PASSWORD`: The password used to authenticate the above username. Although
  there are no places where this value should be logged, **for safety purposes,
  it is highly recommended to mark this variable as *Masked*.**.

### Commands

#### `upload`

```bash
upload PROJECT DSC_FILE
  [--branch-to BRANCHED_PROJECT]
  [--build-info-out BUILD_INFO_FILE=build-info.yml]
  [--rebuild-if-unchanged]
```

This will upload the given .dsc file, as well as any files referenced by it, to
OBS. If any previous .dsc files are present, they, and all files referenced
within, will be removed.

Metadata information on the uploaded revision, such as the revision number,
project name, and package name, will be saved into the file specified by
`--build-info-out` (default is `build-info.yml`). This file is **required** by
the `generate-monitor` and `cleanup` steps. Do note that, if `--branch-to` is
given, the file will be written *immediately* after the branch takes place (i.e.
before the upload); that way, if the upload fails, the branched project can still
be cleaned up.

##### `--branch-to BRANCHED_PROJECT`

Before starting an upload,
[branch](https://openbuildservice.org/help/manuals/obs-user-guide/art.obs.bg.html#sec.obsbg.uc.branchprj)
the package to a new project, named with the value passed to the argument. Any
uploads will now go to the branched project, and `generate-monitor` / `cleanup`
will both used the branched project / package. This is particularly useful to run
testing builds on MRs; you can create an OBS branch named after the MR's Git
branch, and then builds can take place there without interfering with your main
projects.

##### `--build-info-out BUILD_INFO_FILE=build-info.yml`

Changes the filename that the build info will be written to.

##### `--rebuild-if-unchanged`

By default, if none of the files to be uploaded are new or modified from their
previous versions, this will skip any upload, and thus no build will take place.
`--rebuild-if-unchanged` will modify this behavior to explicitly trigger a
rebuild when no actual upload takes place.

Note that, if `--branch-to` was specified, this will, in practice, never be
triggered: due to the way metadata files are handled, right after a branching
operation, there will *always* be a change to upload.

#### `generate-monitor`

```bash
generate-monitor
  [--mixin MONITOR_JOB_MIXIN='']
  [--build-info BUILD_INFO_FILE=build-info.yml]
  [--pipeline-out PIPELINE_FILE=obs.yml]
  [--job-prefix MONITOR_JOB_PREFIX=obs]
  [--build-results-dir BUILD_RESULTS_DIR=results]
  [--build-log-out BUILD_LOG_FILE=build.log]
```

Generates a [child
pipeline](https://docs.gitlab.com/ee/ci/pipelines/parent_child_pipelines.html)
for the purpose of monitoring builds that were started from an
[`upload`](#upload) command. An individual monitoring job will be generated for
each repository / architecture combination in the project, with a name of
`MONITOR_JOB_PREFIX-REPOSITORY-ARCH` (e.g. `obs-default-aarch64`). Because this
command needs to read the generated build info file, this should generally be in
the same job as the invocation of [`upload`](#upload). The generated pipeline
file can be included in your main pipeline following the standard GitLab
mechanisms:

```yaml
obs:
  needs:
    - job-that-generated-the-pipeline-file
  trigger:
    strategy: depend
    include:
      - artifact: obs.yml
        job: job-that-generated-the-pipeline-file
```

The generated jobs will run commands that need to be run by the OBS runner, but
the runner cannot actually determine what [tags](#sending-jobs) should be used.
Therefore, you will generally have to use [`--mixin`](#--mixin) to add extra
YAML that will set the tags, e.g.:

```yaml
upload-and-generate:
  tags:
    - my-runner-tag
  variables:
    JOB_MIXIN: |
      tags:
        - my-runner-tag
  script:
    - upload [...]
    - generate-package --mixin $JOB_MIXIN
  # [...]
```

In addition, if the parent job that invokes the nested yaml (the `obs` job in
the above example) has any rules to [avoid duplicate
pipelines](https://gitlab.com/gitlab-org/gitlab/-/issues/299409), those rules
should be added to the mixin as well, otherwise you may get errors claiming the
["downstream pipeline can not be
created"](https://gitlab.com/gitlab-org/gitlab/-/issues/276179).

After each monitoring job completes, it will save the build artifacts into the
`BUILD_RESULTS_DIR` directory, and the build log will be saved to
`BUILD_LOG_FILE`.

##### `--mixin MONITOR_JOB_MIXIN=''`

Takes a string containing some YAML that will be merged into each generated job.
This will generally be used to set the runner tags of the job

##### `--build-info BUILD_INFO_FILE=build-info.yml`

Specifies the name of the build info file to read. In particular, if a different
build info filename was used with `upload` via
[`--build-info-out`](#--build-info-out), then `--build-info` should be used here
to specify the same filename.

##### `--pipeline-out PIPELINE_FILE=obs.yml`

Changes the filename of the child pipeline YAML.

##### `--job-prefix MONITOR_JOB_PREFIX=obs`

Changes the prefix that will be prepended to each generated job
(`MONITOR_JOB_PREFIX-REPOSITORY-ARCH`).

##### `--build-results-dir BUILD_RESULTS_DIR=results`

Changes the directory each monitoring job will place the build results from OBS
into.

##### `--build-log-out BUILD_LOG_FILE=build.log`

Changes the filename each monitoring job will save the build log into.

#### `cleanup`

```bash
cleanup
  [--build-info BUILD_INFO_FILE=build-info.yml]
  [--ignore-missing-build-info]
  [--only-if-job-unsuccessful]
```

If a branch occurred, cleans up the branched package and, if now empty, project,
using the information from the build info file. (If no branching occurred, this
does nothing.)

##### `--build-info BUILD_INFO_FILE=build-info.yml`

Specifies the name of the build info file to read. In particular, if a different
build info filename was used with `upload` via
[`--build-info-out`](#--build-info-out), then `--build-info` should be used here
to specify the same filename.

##### `--ignore-missing-build-info`

Don't return an error if the build info file is missing; instead, do nothing.
This is primarily useful if `cleanup` is used inside of `after_script`, as it's
possible for the command generating the build info to fail before the build info
is written.

##### `--only-if-job-unsuccessful`

Only run cleanup if a previous command in the same job failed. This is primarily
useful if `cleanup` is used inside of `after_script`, to only remove the branched
project/package if e.g. the upload failed.

## Deployment

TODO

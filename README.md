# OBS Build Orchestrator

This repo contains tools for starting, monitoring, and cleaning up builds on
[OBS](https://build.opensuse.org/), specifically targeting Debian packages.
It's usable in two forms:

- [obo-cli](obo-cli/README.md), a standalone CLI.
- [obs-gitlab-runner](obs-gitlab-runner/README.md), a custom [GitLab
  Runner](https://docs.gitlab.com/runner/).

## Usage

### Required Environment

In order to connect to OBS, three environment variables must be set:

- `OBS_SERVER`: The URL of the OBS instance, e.g. `https://obs.somewhere.com/`.
- `OBS_USER`: The username used to authenticate with OBS (any commits created
  will also be under this user).
- `OBS_PASSWORD`: The password used to authenticate the above username. Although
  there are no places where this value should be logged, **for safety purposes,
  it is highly recommended to mark this variable as *Masked*.**.

For obs-gitlab-runner, these should generally be configured within the "CI/CD"
section of the repository / group settings. **For safety purposes, it is highly
recommended to mark the `OBS_PASSWORD` variable as *Masked*.**. (It should not
be logged anywhere regardless, but that will provide insulation against
mistakes.)

For obo-cli, you can additionally use the `--obs-server`, `--obs-user`, and
`--obs-password` options, but care should be taken to avoid accidentally saving
the values into shell history or other tenuous locations.

### Flag syntax

Any flag arguments shown below can also explicitly take a true/false value, e.g.
`--rebuild-if-unchanged`, `--rebuild-if-unchanged=true`, and
`--rebuild-if-unchanged=false`. This is primarily useful to conditionally set
the value for a flag; you can set `SOME_VARIABLE=true/false` in your CI
pipeline, then use that variable in a flag value as `--flag=$SOME_VARIABLE`.

### Commands

#### `dput`

```bash
dput PROJECT DSC_FILE
  [--branch-to BRANCHED_PROJECT]
  [--build-info-out BUILD_INFO_FILE=build-info.json]
  [--rebuild-if-unchanged]
```

This will upload the given .dsc file, as well as any files referenced by it, to
OBS. If any previous .dsc files are present, they, and all files referenced
within, will be removed.

Metadata information on the uploaded revision, such as the revision number,
project name, and package name, will be saved into the file specified by
`--build-info-out` (default is `build-info.json`). This file is **required** by
the `generate-monitor` and `prune` steps. Do note that, if `--branch-to` is
given, the file will be written *immediately* after the branch takes place (i.e.
before the upload); that way, if the upload fails, the branched project can still
be cleaned up.

##### `--branch-to BRANCHED_PROJECT`

Before starting an upload,
[branch](https://openbuildservice.org/help/manuals/obs-user-guide/art-obs-bg#sec-obsbg-uc-branchprj)
the package to a new project, named with the value passed to the argument. Any
uploads will now go to the branched project, and `generate-monitor` / `prune`
will both used the branched project / package. This is particularly useful to run
testing builds on MRs; you can create an OBS branch named after the MR's Git
branch, and then builds can take place there without interfering with your main
projects.

##### `--build-info-out BUILD_INFO_FILE=build-info.json`

Changes the filename that the build info will be written to.

##### `--rebuild-if-unchanged`

By default, if none of the files to be uploaded are new or modified from their
previous versions, this will skip any upload, and thus no build will take place.
`--rebuild-if-unchanged` will modify this behavior to explicitly trigger a
rebuild when no actual upload takes place.

Note that, if `--branch-to` was specified, this will, in practice, never be
triggered: due to the way metadata files are handled, right after a branching
operation, there will *always* be a change to upload.

#### `generate-monitor` (*obs-gitlab-runner version*)

```bash
generate-monitor RUNNER_TAG
  [--rules RULES]
  [--download-build-results-to BUILD_RESULTS_DIR]
  [--build-info BUILD_INFO_FILE=build-info.json]
  [--pipeline-out PIPELINE_FILE=obs.yml]
  [--job-prefix MONITOR_JOB_PREFIX=obs]
  [--job-timeout MONITOR_JOB_TIMEOUT]
  [--artifact-expiration ARTIFACT_EXPIRATION='3 days']
  [--build-log-out BUILD_LOG_FILE=build.log]
```

Generates a [child
pipeline](https://docs.gitlab.com/ee/ci/pipelines/parent_child_pipelines.html)
for the purpose of monitoring builds that were started from a [`dput`](#dput)
command. An individual monitoring job will be generated for each repository /
architecture combination in the project, with a name of
`MONITOR_JOB_PREFIX-REPOSITORY-ARCH` (e.g. `obs-default-aarch64`). Because this
command needs to read the generated build info file, this should generally be in
the same job as the invocation of [`dput`](#dput). The generated pipeline
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

`RUNNER_TAG` should be the OBS runner's tag; this will be used to run the
generated monitoring jobs on the correct runner. (Unfortunately, the runner
cannot see its own tags, so it is unable fill this in by itself.)

If the parent job that invokes the nested yaml (the `obs` job in the above
example) has any rules to [avoid duplicate
pipelines](https://gitlab.com/gitlab-org/gitlab/-/issues/299409), those rules
should be added to the generated job via [`--rules`](#--rules), otherwise you
may get errors claiming the ["downstream pipeline can not be
created"](https://gitlab.com/gitlab-org/gitlab/-/issues/276179).

After each monitoring job completes, it will save the build log to
`BUILD_LOG_FILE`. In addition, if `--download-build-results-to` is given, the
build artifacts will be saved to the `BUILD_RESULTS_DIR`. These artifacts will
all automatically be uploaded to GitLab.

##### `--rules RULES`

Takes a string containing a YAML sequence of mappings to use as
[rules](https://docs.gitlab.com/ee/ci/yaml/#rules) on the generated jobs.

```yaml
dput-and-generate:
  variables:
    RULES: |
      - if: $$VAR == 1
        when: always
      - when: never
  script:
    - dput [...]
    - generate-package my-tag --rules $RULES
  # [...]
```

##### `--download-build-results-to BUILD_RESULTS_DIR`

After a monitoring job completes, download the build results from OBS to the
given `BUILD_RESULTS_DIR`, and upload it as a GitLab build artifact..

##### `--build-info BUILD_INFO_FILE=build-info.json`

Specifies the name of the build info file to read. In particular, if a different
build info filename was used with `dput` via
[`--build-info-out`](#--build-info-out), then `--build-info` should be used here
to specify the same filename.

##### `--pipeline-out PIPELINE_FILE=obs.yml`

Changes the filename of the child pipeline YAML.

##### `--job-prefix MONITOR_JOB_PREFIX=obs`

Changes the prefix that will be prepended to each generated job
(`MONITOR_JOB_PREFIX-REPOSITORY-ARCH`).

##### `--job-timeout MONITOR_JOB_TIMEOUT`

Changes the timeout for each generated job, using the [job `timeout`
setting](https://docs.gitlab.com/ee/ci/yaml/#timeout). If not passed, the
timeout will not be set.

##### `--artifact-expiration ARTIFACT_EXPIRATION='3 days'`

Changes the expiration of the build results & logs.

##### `--build-log-out BUILD_LOG_FILE=build.log`

Changes the filename each monitoring job will save the build log into.

#### `generate-monitor` (*obo-cli version*)

```bash
generate-monitor
  [--download-build-results-to BUILD_RESULTS_DIR]
  [--build-info BUILD_INFO_FILE=build-info.json]
  [--monitor-out MONITOR_OUT=obs-monitor.json]
  [--build-log-out BUILD_LOG_FILE=build.log]
```

Generates a JSON file `MONITOR_OUT` structured as:

```json5
{
  "entries": [
    {
      "repo": "REPO",
      "arch": "ARCH",
      "commands": {
        "monitor": "monitor [...]",
        "download-binaries": "download-binaries [...]",
      }
    },
    // ...
  ]
}
```

`entries` contains a list of OBS repository + architecture combinations, along
with the subcommands to run to monitor a build and download the results (to be
used as `obo THE_SUBCOMMAND`).

##### `--download-build-results-to BUILD_RESULTS_DIR`

Fills in `entries[*].commands.download-binaries` with a command that will
download the build results from OBS to the given `BUILD_RESULTS_DIR`. If this
option is not given, `commands.download-binaries` will be `null`.

##### `--build-info BUILD_INFO_FILE=build-info.json`

Specifies the name of the build info file to read. In particular, if a different
build info filename was used with `dput` via
[`--build-info-out`](#--build-info-out), then `--build-info` should be used here
to specify the same filename.

##### `--build-log-out BUILD_LOG_FILE=build.log`

Changes the filename each subcommand in `entries[*].commands.monitor` will save
the build log into.

#### `prune`

```bash
prune
  [--build-info BUILD_INFO_FILE=build-info.json]
  [--ignore-missing-build-info]
  [--only-if-job-unsuccessful]
```

If a branch occurred, deletes the branched package and, if now empty, project,
using the information from the build info file. (If no branching occurred, this
does nothing.)

##### `--build-info BUILD_INFO_FILE=build-info.json`

Specifies the name of the build info file to read. In particular, if a different
build info filename was used with `dput` via
[`--build-info-out`](#--build-info-out), then `--build-info` should be used here
to specify the same filename.

##### `--ignore-missing-build-info`

Don't return an error if the build info file is missing; instead, do nothing.
This is primarily useful if `prune` is used inside of `after_script`, as it's
possible for the command generating the build info to fail before the build info
is written.

##### `--only-if-job-unsuccessful`

Only run the prune if a previous command in the same job failed. This is
primarily useful if `prune` is used inside of `after_script`, to only remove the
branched project/package if e.g. the upload failed.

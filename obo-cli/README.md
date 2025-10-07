# obo-cli

This is a CLI for starting, monitoring, and cleaning up builds on
[OBS](https://build.opensuse.org/), specifically targeting Debian packages.

## Usage

For information on OBS authentication and the commands supported, see [the
project-wide README](../README.md).

Docker images are built on every commit, available at
`ghcr.io/collabora/obo-cli:main`. The entry point directly takes the
subcommands, e.g:

```
docker run --rm -it -v $PWD:/work -w /work ghcr.io/collabora/obo-cli:main prune
```

will mount the current directory as `/work` and then run the `prune` command
from within.

This is a collection of release utils for [Apache Flink](https://flink.apache.org/) connectors.

# Integration

The scripts assume that they are integrated into a connector repo as a submodule into the connector repo
under `tools/releasing/shared`.

# Usage

Some scripts rely on environment variables to be set.  
These are checked at the start of each script.  
Any instance of `${some_variable}` in this document refers to an environment variable that is used by the respective
script.

## check_environment.sh

Runs some pre-release checks for the current environment, for example that all required programs are available.  
This should be run once at the start of the release process.

## release_snapshot_branch.sh

Creates (and pushes!) a new snapshot branch for the current commit.  
The branch name is automatically determined from the version in the pom.  
This script should be called when work on a new major/minor version of the connector has started.

## update_branch_version.sh

Updates the version in the poms of the current branch to `${NEW_VERSION}`.

## stage_source_release.sh

Creates a source release from the current branch and pushes it via `svn`
to [dist.apache.org](https://dist.apache.org/repos/dist/dev/flink).  
The version is automatically determined from the version in the pom.  
The created `svn` directory will contain a `-rc${RC_NUM}` suffix.

## stage_jars.sh

Creates the jars from the current branch and deploys them to [repository.apache.org](https://repository.apache.org).  
The version will be suffixed with the Flink minor version, extracted from`${FLINK_VERSION}`, to indicate the supported Flink version.  
If a particular version of a connector supports multiple Flink versions then this script should be called multiple
times.

## release_source_release.sh

Copies the source release from the [SVN release directory](https://dist.apache.org/repops/dist/dev/flink) to the
[SVN release directory](https://dist.apache.org/repops/dist/release/flink) on [dist.apache.org](https://dist.apache.org).

For safety purposes this script does not automatically determine the project and version from the current directory/branch/tag.

```
PROJECT=flink-connector-elasticsearch VERSION=3.0.0 RC_NUM=2 ./release_source_release.sh
```

## release_git_tag.sh

Creates a release tag for the current branch and pushes it to GitHub.
The tag will be suffixed with `-rc${RC_NUM}`, if `${RC_NUM}` was set.  
This script should only be used _after_ the `-SNAPSHOT` version suffix was removd via `update_branch_version.sh`.

## update_japicmp_configuration.sh

Sets the japicmp reference version in the pom of the current branch to `${NEW_VERSION}`, enables compatibility checks
for `@PublicEvolving` when used on snapshot branches an clears the list of exclusions.  
This should be called after a release on the associated snapshot branch. If it was a minor release it should
additionally be called on the `main` branch.

# Common workflow

1. run `release_snapshot_branch.sh`
2. do some development work on the created snapshot branch
3. checkout a specific commit to create a release from
4. run `check_environment.sh`
5. run `update_branch_version.sh`
6. run `stage_source_release.sh`
7. run `stage_jars.sh` (once for each supported Flink version)
8. run `release_git_tag.sh` (with `RC_NUM`)
9. vote on release
10. finalize release or cancel and go back to step 2
11. run `release_source_release.sh`
12. run `release_git_tag.sh` (without `RC_NUM`)
13. run `update_japicmp_configuration.sh` (on snapshot branch, and maybe `main`)

# Script naming conventions

| Prefix  | Meaning                                                                |
|---------|------------------------------------------------------------------------|
| check   | Verifies conditions without making any changes.                        |
| update  | Applies modifications locally to the current branch.                   |
| stage   | Publishes an artifact to an intermediate location for voting purposes. |
| release | Publishes an artifact to a user-facing location.                       |

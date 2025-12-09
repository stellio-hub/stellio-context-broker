# Contributing to Stellio

Thanks for checking out the Stellio Project. We're excited to hear and learn from you. We've put together the following
guidelines to help you figure out where you can best be helpful. 

This file covers contribution best practices. For more information on how you can contribute to Stellio, please have a look at [the development guide](docs/files/development_guide.md).


## Ground rules & expectations

Before we get started, here are a few things we expect from you (and that you should expect from others):

-   Be kind and thoughtful in your conversations around this project. We all come from different backgrounds and
    projects, which means we likely have different perspectives on "how open source is done." Try to listen to others
    rather than convince them that your way is correct.
-   This project is released with a [Contributor Code of Conduct](./CODE_OF_CONDUCT.md). By participating in this
    project, you agree to abide by its terms.
-   If you open a pull request, you must sign the
    [Individual Contributor License Agreement](https://fiware.github.io/contribution-requirements/individual-cla.pdf) by
    stating in a comment _"I have read the CLA Document and I hereby sign the CLA"_
-   Please ensure that your contribution passes all tests. If there are test failures, you will need to address them
    before we can merge your contribution.
-   When adding content, please consider if it is widely valuable. Please don't add references or links to things you or
    your employer have created as others will do so if they appreciate it.

## How to contribute

If you'd like to contribute, start by searching through the [issues](https://github.com/stellio-hub/stellio-context-broker/issues) and
[pull requests](https://github.com/stellio-hub/stellio-context-broker/pulls) to see whether someone else has raised a similar idea or
question.

If you don't see your idea listed, and you think it fits into the goals of this guide, do one of the following:

-   **If your contribution is minor,** such as a typo fix, open a pull request.
-   **If your contribution is major,** such as a new guide, start by opening an issue first. That way, other people can
    weigh in on the discussion before you do any work.

### Pull Request protocol

As explained in ([FIWARE Contribution Requirements](https://fiware-requirements.readthedocs.io/en/latest))
contributions are done using a pull request (PR). The detailed "protocol" used in such PR is described below:

* Direct commits to master or develop branches (even single-line modifications) are not allowed. Every modification has to come as a PR
* In case the PR is implementing/fixing a numbered issue, the issue number has to be referenced in the subject of the PR at creation time
* Anybody is welcome to provide comments to the PR (either direct comments or using the review feature offered by GitHub)
* Use *code line comments* instead of *general comments*, for traceability reasons (see comments lifecycle below)
* Comments lifecycle
    * Comment is created, initiating a *comment thread*
    * New comments can be added as responses to the original one, starting a discussion
    * After discussion, the comment thread ends in one of the following ways:
        * `Fixed in <commit hash>` in case the discussion involves a fix in the PR branch (which commit hash is
          included as reference)
        * `NTC`, if finally nothing needs to be done (NTC = Nothing To Change)
* PR can be merged when the following conditions are met:
    * All comment threads are closed
    * All the participants in the discussion have provided a `LGTM` general comment (LGTM = Looks good to me)
* Self-merging is not allowed (except in rare and justified circumstances)

Some additional remarks to take into account when contributing with new PRs:

* PR must include not only code contributions, but their corresponding pieces of documentation (new or modifications to existing one) and tests
* PR modifications must pass full regression based on existing test in addition to whichever new test added due to the new functionality
* PR should be of an appropriated size that makes review achievable. Too large PRs could be closed with a "please, redo the work in smaller pieces" without any further discussing

## Community

Discussions about the Open Source Guides take place on this repository's
[Issues](https://github.com/stellio-hub/stellio-context-broker/issues) and [Pull Requests](https://github.com/stellio-hub/stellio-context-broker/pulls)
sections. Anybody is welcome to join these conversations.

Wherever possible, do not take these conversations to private channels, including contacting the maintainers directly.
Keeping communication public means everybody can benefit and learn from the conversation.

## Overview

Being an Open Source project, everyone can contribute, provided that you respect the following points:

-   Before contributing any code, the author must make sure all the tests work (see below how to launch the tests).
-   Developed code must adhere to the syntax guidelines enforced by the linters.
-   Code must be developed following the branching model.
-   For any new feature added, unit tests must be provided, following the example of the ones already created.

In order to start contributing:

1. Fork this repository clicking on the "Fork" button on the upper-right area of the page.

2. Clone your just forked repository:

```bash
git clone https://github.com/stellio-hub/stellio-context-broker.git
```

3. Add the main stellio-context-broker repository as a remote to your forked repository (use any name for your remote
   repository, it does not have to be stellio-context-broker, although we will use it in the next steps):

```bash
git remote add stellio-context-broker https://github.com/stellio-hub/stellio-context-broker.git
```

Before starting your contribution, remember to synchronize the `develop` branch in your forked repository with the
`develop` branch in the main stellio-context-broker repository, by following this steps

1. Change to your local `develop` branch (in case you are not in it already):

```bash
git checkout develop
```

2. Fetch the remote changes:

```bash
git fetch stellio-context-broker
```

3. Merge them:

```bash
git rebase stellio-context-broker/develop
```

Contributions following these guidelines will be added to the `develop` branch, and released in the next version. The
release process is explained in the _Releasing_ section below.

## Branching model

There are one special branch in the repository:

-   `master`: contains the tagged and released versions
-   `develop`: contains the development code. New features and bugfixes are always merged to `develop`.

In order to start developing a new feature or refactoring, a new branch should be created with one of the following
names:

-   `feature/<featureDescription>`
-   `fix/<fixDescription>`
-   `chore/<choreDescription>`

depending on the kind of work.

This branch must be created from the current version of the `develop` branch. Once the new functionality has been
completed, a Pull Request will be created from the feature branch to `develop`. Remember to check both the linters, and
the tests before creating the Pull Request.

Bugfixes work the same way as other tasks, except for the branch name, that should be called `fix/<bugName>`.

In order to contribute to the repository, these same scheme should be replicated in the forked repositories, so the new
features or fixes should all come from the current version of `develop` and end up in `develop` again.

All the `feature/*`, `fix/*` and `chore/*` branches are temporary, and should be removed once they have been merged.

## Changelog

The project contains a changelog that is automatically created from the description of the Pull Requests that have been
merged into develop, thanks to the [Release Drafter GitHub action](https://github.com/marketplace/actions/release-drafter).

## Releasing

The process of making a release simply consists in creating the release in GitHub and providing the new tag name.

## Version numbers

The version number will change for each release, according to the following rules:

-   All version numbers will always follow the common pattern: `X.Y.Z`
-   _X_ will change only when there are changes in the release breaking backwards compatibility, or when there are very
    important changes in the feature set of the component. If X changes, Y is set to 0.
-   _Y_ will change every time a new version is released. If only Y changes, it means some new features or bugfixes have
    been released, but the component is just an improved version of the current major release.
-   _Z_ will be reserved for bugfixes inside the releases.

## Bugfix in releases

When a bug is found affecting a release, a branch will be created from the `master` branch. As a part of
the patch, the release version will be increased in its last number (Z). The patch then will be merged (via PR) to the
`master` branch, and a new version will be released.

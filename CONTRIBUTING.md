Thanks in advance for contributing to SpikeAlerts. Please follow these conventions when submitting an issue or pull request.

## Recommended Reading

If this is your first open source project, check out this [guide](https://github.com/readme/guides/first-oss-contribution).

## Version Control Conventions

Here is a recommended way to get setup:
1. Fork this project
2. Clone your new fork, `git clone git@github.com:GithubUser/SpikeAlerts.git`
3. `cd SpikeAlerts`
4. Add the SpikeAlerts repository as an upstream repository: `git remote add upstream git@github.com:SpikeAlerts/SpikeAlerts.git`
5. Create a new branch `git checkout -b your-branch` for your contribution
6. Write code, open a PR from your branch when you're ready
7. If you need to rebase your fork's PR branch onto main to resolve conflicts: `git fetch upstream`, `git rebase upstream/main` and force push to Github `git push --force origin your-branch`

## Changelog Conventions

<!--What warrants a changelog entry?

- Any change that affects the public API, visual appearance or user security *must* have a changelog entry
- Any performance improvement or bugfix *should* have a changelog entry
- Any contribution from a community member *may* have a changelog entry, no matter how small
- Any documentation related changes *should not* have a changelog entry
- Any regression change introduced and fixed within the same release *should not* have a changelog entry
- Any internal refactoring, technical debt reduction, render test, unit test or benchmark related change *should not* have a changelog entry-->

How to add your changelog?

- Edit the [`CHANGELOG.md`](CHANGELOG.md) file directly, inserting a new entry at the top of the appropriate list
- Any changelog entry should be descriptive and concise; it should explain the change to a reader without context

## Attribution

This how-to was initially adapted from MapLibre's [Contributing Documentation](https://github.com/maplibre/maplibre-gl-js/blob/main/CONTRIBUTING.md).

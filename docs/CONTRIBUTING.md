# Contributing to TensorBase

# How to contribute

# Contribution guide

TensorBase is a community-driven open source project and we welcome any contributor. Contributions to the TensorBase project are expected to adhere to our [Code of Conduct](/specs/CODE_OF_CONDUCT.md).

This document outlines some conventions about development workflow, commit message formatting, contact points and other resources to make it easier to get your contribution accepted. You can also join us in our Discussions, Discord server, Slack channel or Wechat group for help with any issues.

<!-- TOC -->

- [How to contribute](#how-to-contribute)
- [Contribution guide](#contribution-guide)
    - [Before you get started](#before-you-get-started)
        - [Sign the CLA or use DCO](#sign-the-cla-or-using-dco)
        - [Setup your development environment](#setup-your-development-environment)
    - [Your First Contribution](#your-first-contribution)
    - [Contribution Workflow](#contribution-workflow)
    - [Code review](#code-review)
        - [Style reference](#style-reference)

<!-- /TOC -->

## Before you get started

### Sign the CLA or using DCO

You should explicitly to make sure you adhere to the community's requirements. Currently, it is open to accept two ways : [CLA](https://en.wikipedia.org/wiki/Contributor_License_Agreement) or [DCO(Developer Certificate of Origin)](https://developercertificate.org/).

for CLA: you just do agree the CLA once. Just follow the bot's instruction. Many open source projects are using this way.

for DCO: you sign off your commits every time. This is verbose. But if some contributor really only accept this option, it is still OK. The commit message must contain a `Signed-off-by` line for DCO. Use option `git commit -s` to sign off your commits.

### Setup your development environment

TensorBase is written in Rust. Before you start contributing code, you need to set up your Rust development environment.


## Your First Contribution

All set to contribute? You can start by finding an existing issue with the [good first issue](https://github.com/tensorbase/tensorbase/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) or [help-wanted](https://github.com/tensorbase/tensorbase/issues?q=is%3Aissue+is%3Aopen+label%3Ahelp-wanted) label. These issues are well suited for new contributors.

## Contribution workflow

To contribute to the TensorBase code base, please follow the workflow as defined in this section.

1. Create a topic branch from where you want to base your work. This is usually main.
2. Make commits of logical units and add test case if the change fixes a bug or adds new functionality.
3. Run tests and make sure all the tests are passed.
4. Run ```cargo fmt``` before commit to enforce an unified code style.
5. Make sure your commit messages are in the proper format (TBD).
6. Push your changes to a topic branch in your fork of the repository.
7. Submit a pull request.
8. If you want core committers to help you easier, select ["Allow edits from maintainers" in the UI of your pull request](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/allowing-changes-to-a-pull-request-branch-created-from-a-fork).

Thanks for your contributions!

## Code review

If your pull request (PR) is opened, it will be assigned to reviewers. Those reviewers will do a thorough code review, looking at correctness, bugs, opportunities for improvement, documentation and comments, and style.

To address review comments, you should commit the changes to the same branch of the PR on your fork.

### Code Style

Keeping a consistent style for sources is very important for an open source project like TensorBase. TensorBase now requires all contributors should run ```cargo fmt``` before commit.

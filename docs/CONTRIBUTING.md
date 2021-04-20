# Contributing to TensorBase

# How to contribute

# Contribution Guide

TensorBase is a community-driven open source project and we welcome any contributor. Contributions to the TensorBase project are expected to adhere to our [Code of Conduct](/specs/CODE_OF_CONDUCT.md).

This document outlines some conventions about development workflow, commit message formatting, contact points and other resources to make it easier to get your contribution accepted. You can also join us in our Discussions, Discord server, Slack channel or Wechat group for help with any issues.

<!-- TOC -->

- [How to contribute](#how-to-contribute)
- [Contribution Guide](#contribution-guide)
    - [Before you get started](#before-you-get-started)
        - [Setting up your development environment](#setting-up-your-development-environment)
    - [Your First Contribution](#your-first-contribution)
    - [Contribution Workflow](#contribution-workflow)
    - [Get a code review](#get-a-code-review)
        - [Style reference](#style-reference)

<!-- /TOC -->

## Before you get started

### Setting up your development environment

TensorBase is written in Rust. Before you start contributing code, you need to set up your Rust development environment.


## Your First Contribution

All set to contribute? You can start by finding an existing issue with the [help-wanted]() label. These issues are well suited for new contributors.

## Contribution Workflow

To contribute to the TensorBase code base, please follow the workflow as defined in this section.

1. Create a topic branch from where you want to base your work. This is usually main.
2. Make commits of logical units and add test case if the change fixes a bug or adds new functionality.
3. Run tests and make sure all the tests are passed.
4. Make sure your commit messages are in the proper format (see below).
5. Push your changes to a topic branch in your fork of the repository.
6. Submit a pull request.

Thanks for your contributions!

## Get a code review

If your pull request (PR) is opened, it will be assigned to reviewers. Those reviewers will do a thorough code review, looking at correctness, bugs, opportunities for improvement, documentation and comments, and style.

To address review comments, you should commit the changes to the same branch of the PR on your fork.

### Style reference

Keeping a consistent style for code, code comments, commit messages, and pull requests is very important for a project like TensorBase. 

Now TensorBase uses the tool [*rustfmt*](/rustfmt.toml) to gate the coding style. If you are using IDE like vscode, the style is guanrateened automatically.

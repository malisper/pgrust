# Aretta onboarding

This `.aristo/` directory configures how Aretta verifies this repository.

## What Aretta does

Aretta continuously checks your default branch against a library of formal
correctness properties and opens pull requests (like the one that added this
directory) whenever there's something worth your attention. Your code only ever
runs inside an isolated sandbox.

## Working model

1. Every merge to your default branch is mirrored into a sandbox and re-verified.
2. Findings reach you as pull requests and issues on this repository — nothing is
   published without a human reviewing it first.
3. You stay in control: edit `config.toml`, close a finding, or reply on a pull
   request and an Aretta operator will pick it up.

## Next steps

- Fill in the `TODO` fields in `config.toml`.
- Read the onboarding guide linked from the welcome pull request.

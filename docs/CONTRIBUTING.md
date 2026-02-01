# Contributing to Druid-DataFusion Bridge

Thank you for your interest in contributing to the Druid-DataFusion Bridge project! We welcome contributions from the community, whether it's bug reports, feature requests, or pull requests.

## Code of Conduct

Please read and adhere to our [Code of Conduct](CODE_OF_CONDUCT.md) to ensure a welcoming and inclusive environment for all contributors.

## Getting Started

### Prerequisites

- Rust 1.70 or later
- Cargo
- Git

### Building from Source

```bash
git clone https://github.com/AndreaBozzo/druid-datafusion-bridge.git
cd druid-datafusion-bridge
cargo build
cargo test
```

## How to Contribute

### Reporting Bugs

Before submitting a bug report, please:

1. **Check existing issues** - Search the issue tracker to see if the bug has already been reported
2. **Verify reproducibility** - Ensure you can consistently reproduce the issue
3. **Gather information** - Include:
   - Rust version (`rustc --version`)
   - Operating system and version
   - Steps to reproduce
   - Expected behavior
   - Actual behavior
   - Any relevant error messages or logs

**Submit the bug report** as a GitHub issue with the `bug` label.

### Suggesting Features

Feature requests are welcome! Please:

1. **Check existing issues** - Ensure the feature hasn't been suggested already
2. **Provide context** - Explain:
   - What problem does this feature solve?
   - Use cases and examples
   - Any alternative approaches you've considered
3. **Label appropriately** - Use the `enhancement` label

### Submitting Pull Requests

1. **Fork the repository** and create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Follow the code style guidelines (see below)
   - Add tests for new functionality
   - Update documentation as needed
   - Ensure all tests pass: `cargo test`
   - Run clippy: `cargo clippy`
   - Format code: `cargo fmt`

3. **Commit with clear messages**:
   ```bash
   git commit -m "feat: add support for X" # or fix:, docs:, test:, etc.
   ```
   We loosely follow [Conventional Commits](https://www.conventionalcommits.org/).

4. **Push and open a Pull Request**:
   ```bash
   git push origin feature/your-feature-name
   ```

5. **Describe your PR**:
   - Reference any related issues: `Closes #123`
   - Explain what the PR does and why
   - Note any breaking changes
   - Include screenshots or hex dumps for binary format changes

### Pull Request Review

- All PRs must pass CI checks (tests, clippy, formatting)
- At least one maintainer review is required
- Please be responsive to review feedback
- It's okay if review takes time - we want to ensure quality

## Code Style

### Rust Guidelines

- Follow [Rust naming conventions](https://rust-lang.github.io/api-guidelines/naming.html)
- Use `cargo fmt` to format code automatically
- Use `cargo clippy` to catch common mistakes
- Write documentation comments (`///`) for public APIs
- Include examples in documentation when helpful

### File Organization

- `src/column/` - Column encoding/compression modules
- `src/segment/` - Segment-level structures (metadata, smoosh format)
- `src/datafusion_ext/` - DataFusion integration
- `src/compression/` - Compression algorithms
- `tests/` - Integration tests and fixtures

### Example Module Structure

```rust
//! Brief description of the module.
//!
//! More detailed explanation if needed.

use crate::error::Result;

/// Detailed documentation with examples.
///
/// # Examples
///
/// ```
/// # use druid_datafusion_bridge::module::Type;
/// let x = Type::new();
/// ```
pub struct Type {
    // fields
}
```

## Testing

- **Unit tests** - Add tests in the same file using `#[test]`
- **Integration tests** - Place in `tests/` directory with real data
- **Test fixtures** - Add binary test data to `tests/fixtures/`

Run tests with:
```bash
cargo test                    # All tests
cargo test --test wikipedia_segment_test  # Specific integration test
cargo test -- --nocapture    # Show println! output
```

## Documentation

- Update `README.md` for major changes
- Add rustdoc comments for public APIs
- Document binary formats in comments or docs/
- Include hex dump examples for complex formats

## Commit Message Format

We encourage following the [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>: <subject>

<body>

<footer>
```

**Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

**Examples**:
- `feat: add support for compressed longs`
- `fix: correct offset calculation in GenericIndexed`
- `docs: add hex dump analysis of index.drd`
- `test: add integration test for Wikipedia segment`

## Questions?

- Check the [README](../README.md) for project overview
- Review existing issues and PRs for context
- Open a discussion issue if you have questions

## License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project.

Thank you for contributing! 🎉

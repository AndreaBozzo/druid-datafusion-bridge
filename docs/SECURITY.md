# Security Policy

## Reporting Security Issues

**Please do not open public GitHub issues for security vulnerabilities.**

If you discover a security vulnerability in Druid-DataFusion Bridge, please report it by emailing the maintainers directly. Include:

- Description of the vulnerability
- Steps to reproduce (if applicable)
- Potential impact
- Any suggested fixes (optional)

We will acknowledge receipt of your report and work with you to address the issue confidentially before any public disclosure.

## Security Considerations

When working with this project, please note:

### Binary Format Parsing

This project parses binary data structures from Druid segments. While we aim to handle malformed input gracefully, be cautious when processing untrusted segment files. Use this library in a sandboxed environment if parsing untrusted data.

### Memory Safety

This project is written in Rust, which provides memory safety guarantees. However, we use `unsafe` code only where necessary for memory-mapped file access. All unsafe usage should be reviewed carefully.

### Dependencies

We maintain a minimal set of dependencies. Check `Cargo.toml` for the full dependency tree and their security advisories using:

```bash
cargo audit
```

## Supported Versions

Security fixes will be provided for:

- The latest stable release
- The development branch (main/master)

## Policy Updates

This security policy may be updated over time. Please check back regularly for changes.

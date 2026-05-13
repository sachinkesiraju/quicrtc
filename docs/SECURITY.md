# Security policy

## Supported versions

Pre-1.0 releases receive security fixes on a best-effort basis. The
most recent tagged minor version is the only one guaranteed to
receive patches.

| Version | Status |
| ------- | ------ |
| 0.1.x   | Supported |
| < 0.1   | Not supported |

## Reporting a vulnerability

Do not file a public issue. Open a private security advisory:
https://github.com/sachinkesiraju/quicrtc/security/advisories/new

Include:

- A description of the issue and its impact.
- Reproduction steps (a minimal program is ideal).
- The version (git SHA or release tag) you tested against.
- Whether you'd like attribution in the fix announcement.

## What to expect

- **Acknowledgement within 5 business days** that we've received
  the report.
- **Assessment within 14 days** of severity (CVSS or qualitative)
  and whether a fix is feasible.
- **Coordinated disclosure**: we'll work with you on a timeline.
  Default is 90 days from initial report to public disclosure;
  earlier is fine if a fix is ready, later is fine if the issue is
  complex or low-severity.
- A public security advisory + CHANGELOG note on release of the
  fix, crediting the reporter (unless they prefer anonymity).

## Scope

In scope:

- The Go library (`server/`, `client/`, `session/`, `wire/`,
  `feed/`, etc.).
- The TypeScript SDK (`ts-sdk/`).
- The wire protocol itself (see [`spec.md`](spec.md)).

Out of scope:

- Vulnerabilities in upstream dependencies (`quic-go`,
  `webtransport-go`, pion). Report those to the respective
  projects.
- Misuse of the library (e.g., disabling TLS verification in
  production, leaking auth slugs).
- Network-level attacks not specific to quicrtc (UDP floods,
  amplification, etc.).

## What we won't do

- Pay a bug bounty. quicrtc is an unfunded open-source project.
- Disclose your contact information without permission.
- Sit on a serious issue indefinitely. If 90 days pass without a
  fix and the issue is high-severity, we'll disclose publicly with
  mitigations.

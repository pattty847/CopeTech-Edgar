# Security policy

Please report suspected vulnerabilities privately through GitHub Security Advisories for
this repository. Do not open a public issue containing exploit details, credentials,
private deployment information, or personal data.

Supported security work currently targets the unreleased `0.2.x` line.

The project treats SEC responses and filing documents as untrusted remote input:

- response bodies are byte-bounded;
- JSON endpoints require valid JSON;
- XML parsers reject entity declarations and external entities;
- CIKs, accessions, tickers, and archive document paths are validated;
- service authentication fails closed when credentials are unconfigured.

Never include API secrets, demo keys, AWS credentials, private-key paths, deployment IPs,
or private hostnames in reports, fixtures, logs, or commits.

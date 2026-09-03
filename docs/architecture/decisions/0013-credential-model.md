# ADR-0013: Credentials stay inside adapters as opaque, redacted values

- **Status:** Accepted
- **Date:** 2026-09-04
- **Decision owners:** InvariantQL maintainers

## Context

The domain must never hold credentials (ADR-0002), yet storage adapters need
keys, tokens, or identity objects; native sources need passwords; and
libraries such as delta-rs, pyiceberg, and Spark need the same secrets again
to read the same locations. The legacy library read service-principal
secrets from environment variables implicitly and wrote SAS tokens into the
caller's `SparkSession` configuration.

## Decision drivers

- Secrets must not appear in plans, fingerprints, explain output, `repr`,
  logs, or exception messages (FF-12).
- Provider SDK credential objects should be usable directly.
- Engines that read a location themselves need the secrets at the edge.
- The library must not alter a `SparkSession` it did not create.

## Decision

Adapters accept provider credentials at construction: plain values (account
keys, SAS tokens, passwords), provider credential objects (for example an
`azure.identity` token credential), or connection strings. They hold them
privately and expose them only as a domain `SecretOptions` mapping, whose
values are redacted when read through the mapping interface and revealed only
through an explicit `reveal()` call by another adapter. Constructing a
`SecretOptions` registers each value with the redaction service, which scrubs
exception messages and diagnostics.

`Storage.native_options()` returns the canonical, provider-neutral secret
vocabulary (for example `account_key`, `sas_token`, `aws_access_key_id`) so
that Delta, Iceberg, and Spark adapters translate rather than re-collect
credentials. `NativeRelation` carries non-secret options and a `SecretOptions`
separately.

Adapters read environment variables only when the caller supplies nothing and
the underlying SDK does so by documented convention; the behaviour is stated in
the adapter's docstring.

The Spark engine never modifies the session it is given. The explicit helper
`SparkEngine.apply_storage_credentials(storage)` copies a storage adapter's
credentials into Hadoop configuration and returns the keys it set.

## Alternatives considered

### A global credential resolver keyed by `CredentialRef`

Cleanly separates configuration from code and suits hosted deployments, but
adds a registry the first users do not need and delays failures to execution
time. `CredentialRef` remains as a label so this can be added later.

### Passing secrets through plan or source options

Simplest to implement and the legacy behaviour; it leaks secrets into
serialised plans, explain output, and logs.

## Consequences and trade-offs

### Benefits

- Plans, fingerprints, and explain output are safe to share and cache.
- Provider errors echoing secrets are scrubbed by exact match and by pattern.

### Costs and risks

- Redaction by pattern is heuristic; the exact-match registry is the reliable
  layer and only covers values the library has seen.
- `SecretOptions` cannot be pickled or serialised, by design.

## Connascence and cohesion

The canonical option vocabulary is static connascence of name between storage
adapters and the adapters that consume it; it replaces connascence of provider
SDK types. Credential handling stays cohesive inside each adapter.

## Fitness functions

- [FF-12: secret non-disclosure](../fitness-functions.md#ff-12-secret-non-disclosure)
- [FF-14: explicit staging and collection](../fitness-functions.md#ff-14-explicit-staging-and-collection)

## Revisit when

- Deployments need credentials resolved at execution time from a vault.
- A provider requires a credential shape that the canonical vocabulary cannot
  express without leaking provider types.

"""FF-12: secrets never appear in errors, reprs, or diagnostics."""

from __future__ import annotations

import pickle

import pytest

from invariantql.domain import CredentialRef, SecretOptions
from invariantql.domain.redaction import (
    REDACTED,
    clear_secrets,
    redact,
    redact_exception,
    redact_mapping,
    register_secret,
)

pytestmark = pytest.mark.architecture

SEEDED = [
    "https://acct.blob.core.windows.net/c/f.csv?sv=2020&sig=SuperSecretSig123",
    "postgresql://user:hunter2pass@db:5432/app",
    "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature",
    "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=abcDEF123==;EndpointSuffix=core.windows.net",
    "{'password': 'p@ss w0rd', 'user': 'me'}",
    "sas_token=sp=r&st=2024&sig=xyz",
]


@pytest.mark.parametrize("text", SEEDED)
def test_patterns_are_scrubbed(text: str) -> None:
    out = redact(text)
    for secret in (
        "SuperSecretSig123",
        "hunter2pass",
        "eyJhbGciOiJIUzI1NiJ9",
        "abcDEF123==",
        "p@ss w0rd",
        "sig=xyz",
    ):
        assert secret not in out, out
    assert REDACTED in out


def test_registered_values_are_scrubbed_everywhere() -> None:
    clear_secrets()
    register_secret("Z9-very-secret-value")
    try:
        assert (
            redact("connection failed: Z9-very-secret-value rejected")
            == f"connection failed: {REDACTED} rejected"
        )
        assert "Z9-very-secret-value" not in redact_exception(
            RuntimeError("bad Z9-very-secret-value")
        )
    finally:
        clear_secrets()


def test_secret_options_never_reveal_by_accident() -> None:
    secrets = SecretOptions(
        {"password": "topsecret-pw-1", "user": "alice"}, ref=CredentialRef("db")
    )
    assert "topsecret-pw-1" not in repr(secrets)
    assert "topsecret-pw-1" not in str(secrets)
    assert secrets["password"] == REDACTED
    assert dict(secrets) == {"password": REDACTED, "user": REDACTED}
    assert secrets.reveal() == {"password": "topsecret-pw-1", "user": "alice"}
    assert redact("error: topsecret-pw-1") == f"error: {REDACTED}"
    with pytest.raises(TypeError):
        pickle.dumps(secrets)
    assert secrets != SecretOptions({"password": "topsecret-pw-1", "user": "alice"})


def test_redact_mapping_masks_secret_keys_and_scrubs_values() -> None:
    out = redact_mapping(
        {"account_key": "k", "url": "https://u:pw@h/", "nested": {"token": "t"}, "n": 1}
    )
    assert out["account_key"] == REDACTED
    assert "pw" not in out["url"]
    assert out["nested"]["token"] == REDACTED
    assert out["n"] == 1

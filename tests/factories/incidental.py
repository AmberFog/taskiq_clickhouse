"""Deterministic incidental text for typed test-data factories."""

from uuid import UUID

from faker import Faker


_FAKER_SEED = 31_036


def identifier(prefix: str, sequence: int) -> str:
    """Return reproducible valid text without shared Faker random state."""
    faker = Faker(locale="en_US")
    faker.seed_instance(_FAKER_SEED + sequence)
    suffix = faker.lexify(text="????????").lower()
    return f"{prefix}-{suffix}"


def payload(prefix: str, sequence: int) -> bytes:
    """Return reproducible opaque bytes whose exact content is incidental."""
    return identifier(prefix, sequence).encode()


def uuid4(sequence: int) -> UUID:
    """Return a reproducible RFC 4122 version-4 identity."""
    return UUID(int=sequence + 1, version=4)

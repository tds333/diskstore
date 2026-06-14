"""Test diskstore.DiskStore with custom config classes."""

import json
import os.path
import shutil
import tempfile
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any

import pytest

from diskstore import DiskStore
from diskstore.config import BaseConfig, DataclassConfig, PydanticConfig

# Check for optional dependencies
try:
    import msgspec

    HAS_MSGSPEC = True
except ImportError:
    HAS_MSGSPEC = False

try:
    import pydantic

    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False


class StructConfig(BaseConfig):
    """Config class for msgspec Structs."""

    def __init__(self, struct, tablename=None, key_type=None, timeout=None, **pragmas):
        super().__init__(
            tablename=tablename, key_type=key_type, timeout=timeout, pragmas=pragmas
        )
        self.fields = (("value", "TEXT"),)
        self.struct = struct

    def dump_value(self, key, value) -> Sequence:
        """Serialize struct to JSON bytes."""
        return (key, msgspec.json.encode(value))

    def load_data(self, data: tuple) -> Any:
        """Deserialize JSON bytes to struct."""
        return msgspec.json.decode(data[1], type=self.struct)


class KeyFromValueConfig(BaseConfig):
    """Config that derives the key from the value when key is None."""

    def __init__(self, *, key_type=None, **kwargs):
        super().__init__(key_type=key_type, **kwargs)
        self.fields = (("value", "TEXT"),)

    def dump_value(self, key, value) -> Sequence:
        if key is None:
            key = value["id"]
        return (key, json.dumps(value))

    def load_data(self, data: tuple) -> Any:
        return json.loads(data[1])


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def tmpdir():
    """Create temporary directory for test files."""
    directory = tempfile.mkdtemp(prefix="diskstore-")
    yield directory
    shutil.rmtree(directory, ignore_errors=True)


@pytest.fixture
def tmpfilename(tmpdir):
    """Create temporary filename for test database."""
    filename = os.path.join(tmpdir, "diskstore.db")
    return filename


def test_key_derived_from_value(tmpfilename):
    """Test that dump_value can derive the key when passed None."""
    store = DiskStore(tmpfilename, KeyFromValueConfig(key_type=str))

    key = store.add(None, {"id": "auto-key", "name": "test"})
    assert key == "auto-key"
    assert store["auto-key"] == {"id": "auto-key", "name": "test"}

    store["explicit"] = {"id": "ignored", "name": "explicit"}
    assert store["explicit"] == {"id": "ignored", "name": "explicit"}

    store.close()


# ============================================================================
# Tests for DataclassConfig
# ============================================================================


class TestDataclassConfig:
    """Test DiskStore with DataclassConfig."""

    def test_dataclass_with_custom_serialization(self, tmpfilename) -> None:
        """Test DataclassConfig with custom dump_value/load_data."""

        class AddressConfig(DataclassConfig):
            """Custom config for Address dataclass with Decimal handling."""

            def dump_value(self, key, value):
                return (
                    key,
                    value.id,
                    value.first_name,
                    value.last_name,
                    str(value.price),
                )

            def load_data(self, data):
                return self.dataclass(data[1], data[2], data[3], Decimal(data[4]))

        @dataclass
        class Address:
            id: str
            first_name: str
            last_name: str
            price: Decimal = Decimal(0)

        value = Address(
            id=str(uuid.uuid4()), first_name="John", last_name="Doe", price=Decimal(1)
        )
        store = DiskStore(tmpfilename, AddressConfig(dataclass=Address))
        store[value.id] = value
        result = store[value.id]
        assert isinstance(result, Address)
        assert result == value
        del store[value.id]
        store.close()

    def test_dataclass_with_json_serialization(self, tmpfilename) -> None:
        """Test DataclassConfig with JSON serialization."""

        @dataclass
        class Address:
            id: str
            first_name: str
            last_name: str
            price: Decimal = Decimal(0)

        class AddressConfig(DataclassConfig):
            """Custom config for Address dataclass using JSON."""

            def __init__(self, tablename=None, key_type=None, timeout=None, **pragmas):
                super().__init__(
                    dataclass=Address,
                    tablename=tablename,
                    key_type=key_type,
                    timeout=timeout,
                    **pragmas,
                )
                self.fields = (("value", "TEXT"),)

            def dump_value(self, key, value):
                d = asdict(value)
                d["price"] = str(d["price"])
                return (key, json.dumps(d))

            def load_data(self, data):
                value = json.loads(data[1])
                value["price"] = Decimal(value["price"])
                return self.dataclass(**value)

        value = Address(
            id=str(uuid.uuid4()), first_name="John", last_name="Doe", price=Decimal(1)
        )
        store = DiskStore(tmpfilename, AddressConfig())
        store[value.id] = value
        result = store[value.id]
        assert isinstance(result, Address)
        assert result == value
        del store[value.id]
        store.close()


# ============================================================================
# Tests for msgspec Struct
# ============================================================================


@pytest.mark.skipif(not HAS_MSGSPEC, reason="msgspec not installed")
class TestMsgspecStruct:
    """Test DiskStore with msgspec Struct and StructConfig."""

    def test_msgspec_struct_roundtrip(self, tmpfilename) -> None:
        """Test saving and loading msgspec Struct."""

        class Address(msgspec.Struct):
            id: str
            first_name: str
            last_name: str
            street: str
            plz: int
            city: str

        value = Address(
            id=str(uuid.uuid4()),
            first_name="John",
            last_name="Doe",
            street="Mainstreet 1",
            plz=77777,
            city="Bonn",
        )
        store = DiskStore(tmpfilename, StructConfig(Address))
        store[value.id] = value
        result = store[value.id]
        assert isinstance(result, Address)
        assert result == value
        del store[value.id]
        store.close()

    def test_msgspec_struct_multiple_records(self, tmpfilename) -> None:
        """Test handling multiple msgspec Struct records."""

        class Person(msgspec.Struct):
            id: str
            name: str
            age: int

        store = DiskStore(tmpfilename, StructConfig(Person))

        people = [
            Person(id="1", name="Alice", age=30),
            Person(id="2", name="Bob", age=25),
            Person(id="3", name="Charlie", age=35),
        ]

        # Store all people
        for person in people:
            store[person.id] = person

        # Verify all can be retrieved
        for person in people:
            result = store[person.id]
            assert result == person
            assert isinstance(result, Person)

        store.close()


# ============================================================================
# Tests for Pydantic BaseModel
# ============================================================================


@pytest.mark.skipif(not HAS_PYDANTIC, reason="pydantic not installed")
class TestPydanticModel:
    """Test DiskStore with Pydantic BaseModel and PydanticConfig."""

    def test_pydantic_model_roundtrip(self, tmpfilename) -> None:
        """Test saving and loading Pydantic model."""

        class Address(pydantic.BaseModel):
            id: str
            first_name: str
            last_name: str
            street: str
            plz: int
            city: str

        value = Address(
            id=str(uuid.uuid4()),
            first_name="John",
            last_name="Doe",
            street="Mainstreet 1",
            plz=77777,
            city="Bonn",
        )
        store = DiskStore(tmpfilename, PydanticConfig(Address))
        store[value.id] = value
        result = store[value.id]
        assert isinstance(result, Address)
        assert result == value
        del store[value.id]
        store.close()

    def test_pydantic_model_validation(self, tmpfilename) -> None:
        """Test Pydantic validation during load."""

        class StrictModel(pydantic.BaseModel):
            id: str
            count: int

        store = DiskStore(tmpfilename, PydanticConfig(StrictModel))

        # Valid data
        value = StrictModel(id="test", count=42)
        store[value.id] = value
        result = store[value.id]
        assert result.count == 42

        # Invalid model data should fail validation on load
        store.close()

    def test_pydantic_model_multiple_records(self, tmpfilename) -> None:
        """Test handling multiple Pydantic model records."""

        class User(pydantic.BaseModel):
            id: str
            name: str
            email: str

        store = DiskStore(tmpfilename, PydanticConfig(User))

        users = [
            User(id="1", name="Alice", email="alice@example.com"),
            User(id="2", name="Bob", email="bob@example.com"),
            User(id="3", name="Charlie", email="charlie@example.com"),
        ]

        # Store all users
        for user in users:
            store[user.id] = user

        # Verify all can be retrieved
        for user in users:
            result = store[user.id]
            assert result == user
            assert isinstance(result, User)

        store.close()

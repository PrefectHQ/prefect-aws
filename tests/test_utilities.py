import pytest

from prefect_aws.utilities import hash_collection


class TestHashCollection:
    def test_simple_dict(self):
        simple_dict = {"key1": "value1", "key2": "value2"}
        assert hash_collection(simple_dict) == hash_collection(
            simple_dict
        ), "Simple dictionary hashing failed"

    def test_nested_dict(self):
        nested_dict = {"key1": {"subkey1": "subvalue1"}, "key2": "value2"}
        assert hash_collection(nested_dict) == hash_collection(
            nested_dict
        ), "Nested dictionary hashing failed"

    def test_complex_structure(self):
        complex_structure = {
            "key1": [1, 2, 3],
            "key2": {"subkey1": {"subsubkey1": "value"}},
        }
        assert hash_collection(complex_structure) == hash_collection(
            complex_structure
        ), "Complex structure hashing failed"

    def test_unhashable_structure(self):
        typically_unhashable_structure = dict(key=dict(subkey=[1, 2, 3]))
        with pytest.raises(TypeError):
            hash(typically_unhashable_structure)
        assert hash_collection(typically_unhashable_structure) == hash_collection(
            typically_unhashable_structure
        ), "Unhashable structure hashing failed after transformation"

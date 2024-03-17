"""Utilities for working with AWS services."""

from prefect.utilities.collections import visit_collection


def hash_collection(collection) -> int:
    """Use visit_collection to transform and hash a collection.

    Args:
        collection (Any): The collection to hash.

    Returns:
        int: The hash of the transformed collection.

    Example:
        ```python
        from prefect_aws.utilities import hash_collection

        hash_collection({"a": 1, "b": 2})
        ```

    """

    def make_hashable(item):
        """Make an item hashable by converting it to a tuple."""
        if isinstance(item, dict):
            return tuple(sorted((k, make_hashable(v)) for k, v in item.items()))
        elif isinstance(item, list):
            return tuple(make_hashable(v) for v in item)
        return item

    hashable_collection = visit_collection(
        collection, visit_fn=make_hashable, return_data=True
    )
    return hash(hashable_collection)

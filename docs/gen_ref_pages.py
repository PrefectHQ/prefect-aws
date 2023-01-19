"""
Copies README.md to index.md. Also discovers all blocks and
generates a list of them in the docs under the Blocks Catalog heading.
"""

from pathlib import Path
from textwrap import dedent

import mkdocs_gen_files
from prefect.blocks.core import Block
from prefect.utilities.dispatch import get_registry_for_type
from prefect.utilities.importtools import to_qualified_name

COLLECTION_SLUG = "prefect_aws"


def find_module_blocks():
    blocks = get_registry_for_type(Block)
    collection_blocks = [
        block
        for block in blocks.values()
        if to_qualified_name(block).startswith(COLLECTION_SLUG)
    ]
    module_blocks = {}
    for block in collection_blocks:
        block_name = block.__name__
        module_nesting = tuple(to_qualified_name(block).split(".")[1:-1])
        if module_nesting not in module_blocks:
            module_blocks[module_nesting] = []
        module_blocks[module_nesting].append(block_name)
    return module_blocks


def insert_blocks_catalog(generated_file):
    module_blocks = find_module_blocks()
    if len(module_blocks) == 0:
        return
    generated_file.write("## Blocks Catalog\n")
    generated_file.write(
        dedent(
            f"""
            Below is a list of Blocks available for registration in
            `prefect-aws`.

            To register blocks in this module to
            [view and edit them](https://orion-docs.prefect.io/ui/blocks/)
            on Prefect Cloud:
            ```bash
            prefect block register -m {COLLECTION_SLUG}
            ```
            """
        )
    )
    generated_file.write(
        "Note, to use the `load` method on Blocks, you must already have a block document "  # noqa
        "[saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) "  # noqa
        "or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).\n"
    )
    for module_nesting, block_names in module_blocks.items():
        module_path = " ".join(module_nesting)
        module_title = module_path.replace("_", " ").title()
        generated_file.write(f"### {module_title} Module\n")
        for block_name in block_names:
            generated_file.write(
                f"- **[{block_name}][{COLLECTION_SLUG}.{module_path}.{block_name}]**\n"
            )
        generated_file.write(
            dedent(
                f"""
                To load the {block_name}:
                ```python
                from prefect import flow
                from {COLLECTION_SLUG}.{module_path} import {block_name}

                @flow
                def my_flow():
                    my_block = {block_name}.load("MY_BLOCK_NAME")

                my_flow()
                ```
                """
            )
        )


readme_path = Path("README.md")
docs_index_path = Path("index.md")

with open(readme_path, "r") as readme:
    with mkdocs_gen_files.open(docs_index_path, "w") as generated_file:
        for line in readme:
            if line.startswith("Visit the full docs [here]("):
                continue  # prevent linking to itself
            if line.startswith("## Resources"):
                insert_blocks_catalog(generated_file)
            generated_file.write(line)

    mkdocs_gen_files.set_edit_path(Path(docs_index_path), readme_path)

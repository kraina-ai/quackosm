"""
OpenStreetMap extracts tree.

This module contains function for displaying available extracts in the tree form.
"""

from typing import Callable

import pandas as pd
from rich.tree import Tree

from quackosm.osm_extracts.extract import OsmExtractSource


def get_available_extracts_as_rich_tree(
    source_enum: OsmExtractSource,
    osm_extract_source_index_functions: dict[OsmExtractSource, Callable[[], pd.DataFrame]],
    use_full_names: bool = False,
) -> Tree:
    """Transform available OSM extracts into a tree from the Rich library."""
    if source_enum == OsmExtractSource.any:
        root = Tree("All extracts")
        for other_source_enum, get_index_function in osm_extract_source_index_functions.items():
            branch_id = other_source_enum.value
            branch = root.add(branch_id)
            create_rich_tree_branch(branch_id, branch, get_index_function(), use_full_names)
    else:
        root_id = source_enum.value
        root = Tree(root_id)
        create_rich_tree_branch(
            root_id, root, osm_extract_source_index_functions[source_enum](), use_full_names
        )

    return root


def create_rich_tree_branch(
    parent_id: str, tree: Tree, index: pd.DataFrame, use_full_names: bool, depth: int = 0
) -> None:
    """
    Iterate OSM extracts recursively and create tree branches.

    Args:
        parent_id (str): Current id of the extract used to find children.
        tree (Tree): Tree object from Rich library.
        index (pd.DataFrame): List of available OSM extracts.
        use_full_names (bool): Whether to display full name, or short name of the extract.
            Full name contains all parents of the extract.
        depth (int, optional): Depth of the recursion. Defaults to 0.
    """
    matching_children = index[index.parent == parent_id].sort_values(by="name")
    for matching_child in matching_children.to_dict(orient="records"):
        name = matching_child["file_name"] if use_full_names else matching_child["name"]
        url = matching_child["url"]
        area = human_format(matching_child["area"])
        branch = tree.add(f":globe_with_meridians: [link={url}]{name}[/link] ({area} km\u00b2)")

        create_rich_tree_branch(matching_child["id"], branch, index, use_full_names, depth + 1)

    if depth == 0:
        loose_parents = sorted(set(index["parent"]).difference(index["id"]).difference([parent_id]))
        for loose_parent in loose_parents:
            branch = tree.add(loose_parent)
            create_rich_tree_branch(loose_parent, branch, index, use_full_names, depth + 1)


def human_format(num: float) -> str:
    """Change big number into a human readable format."""
    num = float(f"{num:.3g}")
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return "{}{}".format(
        f"{num:f}".rstrip(".0"),
        ["", "K", "M", "B"][magnitude],
    )

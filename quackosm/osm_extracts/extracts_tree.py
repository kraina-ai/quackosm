"""
OpenStreetMap extracts tree.

This module contains function for displaying available extracts in the tree form.
"""

from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from rich.tree import Tree


def _create_branch_rich(parent_id: str, tree: "Tree", index: pd.DataFrame, depth: int = 0) -> None:
    matching_children = index[index.parent == parent_id].sort_values(by="name")
    for matching_child in matching_children.to_dict(orient="records"):
        branch = tree.add(f":globe_with_meridians: {matching_child['name']}")
        _create_branch_rich(matching_child["id"], branch, index, depth + 1)

    if depth == 0:
        loose_parents = set(index["parent"]).difference(index["id"]).difference([parent_id])
        for loose_parent in loose_parents:
            branch = tree.add(loose_parent)
            _create_branch_rich(loose_parent, branch, index, depth + 1)

# check https://stackoverflow.com/questions/20242479/printing-a-tree-data-structure-in-python
def _create_branch_dict(
    parent_id: str, tree_dict: dict[str, Any], index: pd.DataFrame, depth: int = 0
) -> None:
    matching_children = index[index.parent == parent_id].sort_values(by="name")
    for matching_child in matching_children.to_dict(orient="records"):
        branch: dict[str, Any] = {}
        _create_branch_dict(matching_child["id"], branch, index, depth + 1)
        tree_dict[matching_child["name"]] = branch

    if depth == 0:
        loose_parents = set(index["parent"]).difference(index["id"]).difference([parent_id])
        for loose_parent in loose_parents:
            branch = {}
            _create_branch_dict(loose_parent, branch, index, depth + 1)
            tree_dict[loose_parent] = branch

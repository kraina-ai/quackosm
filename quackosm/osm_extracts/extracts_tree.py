"""
OpenStreetMap extracts tree.

This module contains function for displaying available extracts in the tree form.
"""

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from rich.tree import Tree


def _create_branch_rich(parent_id: str, tree: "Tree", index: pd.DataFrame, depth: int = 0) -> None:
    matching_children = index[index.parent == parent_id].sort_values(by="name")
    for matching_child in matching_children.to_dict(orient="records"):
        branch = tree.add(f":globe_with_meridians: {matching_child['name']}")
        _create_branch_rich(matching_child["id"], branch, index, depth + 1)

    if depth == 0:
        loose_parents = sorted(set(index["parent"]).difference(index["id"]).difference([parent_id]))
        for loose_parent in loose_parents:
            branch = tree.add(loose_parent)
            _create_branch_rich(loose_parent, branch, index, depth + 1)


# based on https://stackoverflow.com/questions/20242479/printing-a-tree-data-structure-in-python
def _print_branch_python(
    parent_id: str,
    index: pd.DataFrame,
    last: bool = True,
    header: str = "",
    depth: int = 0,
) -> None:
    elbow = "└── " if depth > 0 else ""
    pipe = "│   "
    tee = "├── "
    blank = "    " if depth > 0 else ""

    parent_id_matching = index.id == parent_id

    print(
        header
        + (elbow if last else tee)
        + (
            f"🌐 {index.loc[parent_id_matching, 'name'].iloc[0]}"
            if parent_id_matching.any()
            else parent_id
        )
    )

    loose_parents: list[str] = []

    if depth == 0:
        loose_parents = sorted(set(index["parent"]).difference(index["id"]).difference([parent_id]))

    matching_children = (
        index[index.parent == parent_id].sort_values(by="name").to_dict(orient="records")
    )
    for i, matching_child in enumerate(matching_children):
        _print_branch_python(
            parent_id=matching_child["id"],
            index=index,
            header=header + (blank if last else pipe),
            last=i == len(matching_children) - 1 if not loose_parents else False,
            depth=depth + 1,
        )

    if depth == 0:
        for i, loose_parent in enumerate(loose_parents):
            _print_branch_python(
                parent_id=loose_parent,
                index=index,
                header=header + (blank if last else pipe),
                last=i == len(loose_parents) - 1,
                depth=depth + 1,
            )

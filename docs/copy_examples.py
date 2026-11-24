"""Examples copying utility function."""

from pathlib import Path

import mkdocs_gen_files
import nbformat

EXAMPLES_DIRECTORY_PATH = Path("examples")

RUN_IN_COLAB_CELL_MARKDOWN = """
<a target="_blank" href="https://colab.research.google.com/github/kraina-ai/quackosm/blob/main/{relative_file_path}">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a>
"""


def write_file(file_path: Path) -> None:
    """
    Copies file from examples directory into mkdocs scope.

    Args:
        file_path (Path): Current file path.
    """
    root_path = file_path.relative_to(".")
    print(f"Copying {root_path} file to {root_path}")
    with root_path.open("r") as src, mkdocs_gen_files.open(root_path, "w") as dst:
        if root_path.suffix != ".ipynb":
            dst.write(src.read())
            return

        nb = nbformat.read(src, as_version=4)
        for i, cell in enumerate(nb["cells"]):
            # Cell installing the libraries in the Google Colab
            if "colab" in cell["metadata"].get("tags", []):
                nb["cells"][i] = nbformat.v4.new_markdown_cell(
                    RUN_IN_COLAB_CELL_MARKDOWN.format(relative_file_path=root_path)
                )
                break
        else:
            print(f"No cell tagged `colab` in the {root_path} notebook.")

        nbformat.write(nb, dst)


banned_directories = ["cache", "files", "example_files", "__pycache__", "lightning_logs"]
for i in EXAMPLES_DIRECTORY_PATH.glob("**/*"):
    if i.is_file() and i.suffix == ".ipynb":
        should_copy = True
        for banned_directory in banned_directories:
            if banned_directory in i.parts:
                should_copy = False
                break

        if should_copy:
            write_file(i)

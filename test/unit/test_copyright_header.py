#
# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#

import re
from pathlib import Path

# For distributed open source and proprietary code, we must include
# a copyright header in source every file:
_copyright_header_re = re.compile(
    r"Copyright \d{4} Amazon\.com, Inc\. or its affiliates\. All Rights Reserved\.", re.IGNORECASE
)


def _check_file(filename: Path) -> None:
    with open(filename) as infile:
        lines_read = 0
        for line in infile:
            if _copyright_header_re.search(line):
                return  # success
            lines_read += 1
            if lines_read > 10:
                raise Exception(
                    f"Could not find a valid Amazon.com copyright header in the top of {filename}."
                    " Please add one."
                )
        else:
            # __init__.py files are usually empty, this is to catch that.
            raise Exception(
                f"Could not find a valid Amazon.com copyright header in the top of {filename}."
                " Please add one."
            )


def test_copyright_headers():
    """Verifies every .py file has an Amazon copyright header."""
    root_project_dir = Path(__file__).parent.parent.parent
    # Choose only a few top level directories to test.
    # That way we don't snag any virtual envs a developer might create, at the risk of missing
    # some top level .py files.
    top_level_dirs = ["src", "test", "scripts"]
    file_count = 0
    for top_level_dir in top_level_dirs:
        for path in Path(root_project_dir / top_level_dir).glob("**/*.py"):
            if "_version.py" not in str(path):
                print(path)
                _check_file(path)
                file_count += 1

    print(f"test_copyright_headers checked {file_count} files successfully.")

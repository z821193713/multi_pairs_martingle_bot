
"""
Utility functions for trader.
"""


import json
from pathlib import Path
from decimal import Decimal, ROUND_DOWN, ROUND_UP


def _get_trader_dir(temp_name: str):
    """
    Get path where trader is running in.
    """
    cwd = Path.cwd()
    temp_path = cwd.joinpath(temp_name)

    if temp_path.exists():
        return cwd, temp_path

    if not temp_path.exists():
        temp_path.mkdir()

    return cwd, temp_path


TRADER_DIR, TEMP_DIR = _get_trader_dir("trader")


def get_file_path(filename: str):
    """
    Get path for temp file with filename.
    """
    return TEMP_DIR.joinpath(filename)


def get_folder_path(folder_name: str):
    """
    Get path for temp folder with folder name.
    """
    folder_path = TEMP_DIR.joinpath(folder_name)
    if not folder_path.exists():
        folder_path.mkdir()
    return folder_path

def load_json(filename: str):
    """
    Load data from json file in temp path.
    """
    filepath = get_file_path(filename)

    if filepath.exists():
        with open(filepath, mode="r", encoding="UTF-8") as f:
            data = json.load(f)
        return data
    else:
        save_json(filename, {})
        return {}


def save_json(filename: str, data: dict):
    """
    Save data into json file in temp path.
    """
    filepath = get_file_path(filename)
    with open(filepath, mode="w+", encoding="UTF-8") as f:
        json.dump(
            data,
            f,
            indent=4,
            ensure_ascii=False
        )


def round_to(value: float, target: float) -> Decimal:
    """
    Round price to price tick value.
    """
    value = Decimal(str(value))
    target = Decimal(str(target).rstrip("0"))
    return value.quantize(target)


def floor_to(value: float, target: float) -> Decimal:
    """
    Similar to math.floor function, but to target float number.
    """
    value = Decimal(str(value))
    target = Decimal(str(target).rstrip("0"))
    return value.quantize(target, rounding=ROUND_DOWN)
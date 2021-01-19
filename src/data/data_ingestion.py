"""Runs data processing scripts to turn raw data from (../raw) into
cleaned data ready to be analyzed (saved in ../interim).
"""

import json
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import find_dotenv, load_dotenv

PROJECT_DIR = Path(__file__).resolve().parents[2]

FOLDER_RAW = os.path.join(PROJECT_DIR, "data/raw")
FOLDER_INTERIM = os.path.join(PROJECT_DIR, "data/interim")


def main():
    fit_profiles_to_mongoimport()
    fit_cards_to_mongoimport()
    # get_sample_profile()


def profile_to_line(name: str, data: pd.DataFrame, index: int) -> str:
    return (
        json.dumps({"description": data[name], "id": index, "name": name})
        + "\n"
    )


def fit_profiles_to_mongoimport():
    filename = "extended_properties.json"

    data = json.load(open(os.path.join(FOLDER_RAW, filename)))

    lines = [
        profile_to_line(profile_name, data, index)
        for index, profile_name in enumerate(data)
    ]

    with open(os.path.join(FOLDER_INTERIM, filename), "w") as file_out:
        file_out.writelines(lines)


def card_to_line(card_id: int, card: pd.DataFrame) -> str:
    card["id"] = card_id
    return json.dumps(card) + "\n"


def fit_cards_to_mongoimport():
    filename = "tarot-images.json"

    data = json.load(open(os.path.join(FOLDER_RAW, filename)))
    cards = data["cards"]

    lines = [card_to_line(card_id, card) for card_id, card in enumerate(cards)]

    with open(os.path.join(FOLDER_INTERIM, filename), "w") as file_out:
        file_out.writelines(lines)


def get_score(d: dict) -> float:
    return sum(d.values())


def get_sample_profile():
    filename = "extended_properties_mongoimport.json"

    data = pd.read_json(os.path.join(FOLDER_RAW, filename), lines=True)
    data["score"] = data["description"].map(get_score)

    print(data.sort_values("score", ascending=False).head())
    print(data.describe())

    data.head(50).to_json(
        os.path.join(FOLDER_INTERIM, filename), orient="records", indent=2
    )


if __name__ == "__main__":
    LOG_FMT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=LOG_FMT)

    load_dotenv(find_dotenv())

    main()

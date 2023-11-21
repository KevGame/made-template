import sys
import os
import pandas as pd
import sqlalchemy as sa
from kaggle.api.kaggle_api_extended import KaggleApi

def download_dataset_csv(identifier: str) -> None:
    # Setup kaggle API from the environment variables
    kaggle_api = KaggleApi()

    try:
        # Authenticate with the api and download the raw dataset (csv) to the data folder. If needed overwrite files with the same name
        kaggle_api.authenticate()
        kaggle_api.dataset_download_files(dataset=identifier, path="data", force=True, unzip=True)
    except:
        # One problem of kaggles api is, if the authentication fails they raise a exception from urllib rather than a custom one
        # For this reason, all exceptions are treated together.
        print("Problems downloading the dataset. Check whether the API credentials are correct.", file=sys.stderr)

def load_dataset_csv(data_name: str, seperator: str, charset: str) -> pd.DataFrame:
    # Load csv file in data (which was previously downloaded from kaggle)
    csv_data = pd.read_csv("data/" + data_name, sep=seperator, encoding=charset)
    
    return csv_data

def delete_dataset_csv(data_name: str) -> None:
    # Delete the specific file in the data folder
    os.remove("data/" + data_name)

def main():
    # - - - - - First dataset: Top games on Twitch 2016 - 2023 (https://www.kaggle.com/datasets/rankirsh/evolution-of-top-games-on-twitch) - - - - -
    csv_names = ["Twitch_game_data.csv", "Twitch_global_data.csv"]
    download_dataset_csv("rankirsh/evolution-of-top-games-on-twitch")
    csv_data = load_dataset_csv(csv_names[0], ",", "iso-8859-1")

    delete_dataset_csv(csv_names[0])
    delete_dataset_csv(csv_names[1])

    # - - - - - Second dataset: Popular Video Games (https://www.kaggle.com/datasets/matheusfonsecachaves/popular-video-games) - - - - -
    csv_names = ["backloggd_games.csv"]
    download_dataset_csv("matheusfonsecachaves/popular-video-games")
    csv_data = load_dataset_csv(csv_names[0], ",", "")

    delete_dataset_csv(csv_names[0])
    
if __name__ == "__main__":
    main()

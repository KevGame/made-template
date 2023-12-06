import sys
import os
import pandas as pd
import sqlalchemy as sa
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile

DATA_DIR_NAME = 'data/'
DATA_FILE_NAME = 'data.sqlite'

def download_dataset_csv(identifier: str, csv_name: str) -> None:
    # Setup kaggle API from the environment variables
    kaggle_api = KaggleApi()

    try:
        # Authenticate with the api and download the raw dataset (csv) to the data folder. If needed overwrite files with the same name
        kaggle_api.authenticate()
        kaggle_api.dataset_download_file(dataset=identifier, file_name=csv_name, path=DATA_DIR_NAME, force=True)
        
        # Extract zip file from data
        with zipfile.ZipFile(f'{DATA_DIR_NAME}{csv_name}.zip', 'r') as data_zip:
            data_zip.extractall(DATA_DIR_NAME)
        
        # Delete the zip file afterwards
        os.remove(f"{DATA_DIR_NAME}{csv_name}.zip")
    except:
        # One problem of kaggles api is, if the authentication fails they raise a exception from urllib rather than a custom one
        # For this reason, all exceptions are treated together.
        print("Problems downloading the dataset. Check if your API credentials are correct.", file=sys.stderr)

def correct_datasource_1(data_name: str) -> None:
    # Replacement dict for wrong character representation
    replacements = {
        '<U+00E9>': 'é',
        'Ã©': 'é'
    }
    
    # Open csv file as iso
    with open(f'{DATA_DIR_NAME}{data_name}', 'r', encoding='ISO-8859-1') as file:
        # Read content of file
        content = file.read()

        # Replace all occurences according to the dict
        for old, new in replacements.items():
            content = content.replace(old, new)
        
    # Write new converted content in csv file with utf-8 encoding
    with open(f"{DATA_DIR_NAME}{data_name}", 'w', encoding='utf-8') as file:
        file.write(content)

def load_dataset_csv(data_name: str, seperator: str) -> pd.DataFrame:
    # Load csv file in data (which was previously downloaded from kaggle)
    csv_data = pd.read_csv(f'{DATA_DIR_NAME}{data_name}', sep=seperator, encoding='utf-8')

    return csv_data

def store_data(csv_data: pd.DataFrame, table_name: str) -> None:
    # Create sqlite engine and store des changed csv data in the /data/ folder as SQLite database
    engine = sa.create_engine(f'sqlite:///{DATA_DIR_NAME}{DATA_FILE_NAME}?charset=utf8')
    csv_data.to_sql(table_name, engine, index=False, if_exists='replace')

def delete_dataset_csv(data_name: str) -> None:
    # Delete the specific file in the data folder
    os.remove(f"{DATA_DIR_NAME}{data_name}")

def prep_datasource_1(csv_data: pd.DataFrame) -> pd.DataFrame:
    # Drop unnecessary columns: Rank, Streamers, Avg_viewer_ratio
    csv_data.drop(['Rank', 'Streamers', 'Avg_viewer_ratio'], axis=1, inplace=True)

    # Drop categories that are not games
    irl = ['Sports', 'ASMR', 'Pools, Hot Tubs, and Beaches', 'Talk Shows & Podcasts', 'Travel & Outdoors', 'Special Events', 'Science & Technology', 'Fitness & Health', 'Just Chatting']
    creative = ['Music', 'Art', 'Software & Game Development', 'Makers & Crafting', 'Food & Drink']
    csv_data = csv_data[~csv_data["Game"].isin(irl + creative)]

    return csv_data

def prep_datasource_2(csv_data: pd.DataFrame) -> pd.DataFrame:
    # Drop unnecessary columns: Team, Rating, Number of Reviews, Summary, Reviews, Backlogs, Wishlist
    csv_data.drop(['Unnamed: 0', 'Developers', 'Summary', 'Rating', 'Playing', 'Backlogs', 'Wishlist', 'Lists', 'Reviews'], axis=1, inplace=True)


    return csv_data

def prep_combine_datasources(csv_data_twitch: pd.DataFrame, csv_data_games: pd.DataFrame) -> pd.DataFrame:
    # Merge datasets on game title to get game specific information 
    csv_data = csv_data_twitch.merge(csv_data_games, left_on='Game', right_on='Title', how='inner')

    return csv_data

def main():
    print("- - - - - Starting data pipeline - - - - -")

    # - - - - - First dataset: Top games on Twitch 2016 - 2023 (https://www.kaggle.com/datasets/rankirsh/evolution-of-top-games-on-twitch) - - - - -
    print("- - - - - Start pipeline for datasource 1: Top games on Twitch 2016 - 2023 - - - - -")
    print("Datasource 1: Download and load")
    download_dataset_csv('rankirsh/evolution-of-top-games-on-twitch', 'Twitch_game_data.csv')
    correct_datasource_1('Twitch_game_data.csv')
    csv_data_twitch = load_dataset_csv('Twitch_game_data.csv', ',')

    print("Datasource 1: Prepare")
    csv_data_twitch = prep_datasource_1(csv_data_twitch)

    print("Datasource 1: Store")
    store_data(csv_data_twitch, 'twitch_games')

    print("Datasource 1: Delete raw csv(s)")
    delete_dataset_csv('Twitch_game_data.csv')

    # - - - - - Second dataset: Popular Video Games (https://www.kaggle.com/datasets/matheusfonsecachaves/popular-video-games) - - - - -
    print("- - - - - Start pipeline for datasource 2: Popular Video Games - - - - -")
    print("Datasource 2: Download and load")
    download_dataset_csv('matheusfonsecachaves/popular-video-games', 'backloggd_games.csv')
    csv_data_games = load_dataset_csv('backloggd_games.csv', ',')

    print("Datasource 2: Prepare")
    csv_data_games = prep_datasource_2(csv_data_games)

    print("Datasource 2: Store")
    store_data(csv_data_games, 'popular_video_games')

    print("Datasource 2: Delete raw csv(s)")
    delete_dataset_csv('backloggd_games.csv')

    # - - - - - Combine datasets for later analysis - - - - -
    print("- - - - - Final: Combine datasources - - - - -")
    print("Combine datasources")
    csv_data = prep_combine_datasources(csv_data_twitch, csv_data_games)
    store_data(csv_data, 'merged')
    
    print("- - - - - Finished data pipeline - - - - -")

if __name__ == "__main__":
    main()

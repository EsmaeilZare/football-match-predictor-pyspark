from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

import pandas as pd
import os
import sqlite3
from utils import clean_name, clean_value, clean_date

base_url = "https://www.transfermarkt.com/kaka/marktwertverlauf/spieler/"


def retrieve_players_from_dataset(dataset_path):
    # establishing a connection to our dataset
    db_conn = sqlite3.connect(dataset_path)
    # loading player data of our dataset
    players = pd.read_sql("SELECT * FROM Player;", db_conn)
    # add 2 columns for player id and its value history in Transfermarkt website
    players_count = len(players)
    players.insert(2, "tm_player_id", [0 for _ in range(players_count)], True)
    players.insert(3, "value_history", ["_" for _ in range(len(players))], True)
    return players


def find_tm_player_id(player: dict, tm_players: pd.DataFrame):
    tm_player_id = -1
    name = clean_name(player["player_name"])
    birth_date = player["birthday"]
    # find all tm players with the same name
    matched_tm_players = tm_players.loc[(tm_players["name"] == name)]
    count = len(matched_tm_players)

    for j in range(count):
        if matched_tm_players["birth_data"].iloc[j] == birth_date:
            # find tm player with the same birthday
            print(f"{name} who was born on {birth_date} was matched")
            tm_player_id = matched_tm_players["id"].iloc[j]
        elif j == count - 1:
            # since birthday might be incorrect we select last matched tm player
            print(
                f"{name} who was born on {birth_date} was matched with a different birthdate"
            )
            tm_player_id = matched_tm_players["id"].iloc[j]

    return tm_player_id


def get_player_value_market_history(
    web_driver: webdriver, tab_number: int, tm_player_id: int
):
    global base_url
    market_history = []
    try:
        web_driver.switch_to.window(f"{tab_number}")
        web_driver.get(f"{base_url}{tm_player_id}")
        value_history_chart = web_driver.find_element(
            by=By.CLASS_NAME, value="highcharts-markers"
        )
        value_change_points = value_history_chart.find_elements(
            by=By.TAG_NAME, value="image"
        )
        for value_change_point in value_change_points:
            try:
                action = ActionChains(web_driver)
                action.move_to_element(value_change_point).perform()
                details = web_driver.find_elements(
                    by=By.XPATH, value='//div[@class="highcharts-tooltip"]/span/b'
                )
                if len(details) != 4:
                    continue
                change_point_info = {
                    "date": details[0].text,
                    "value": clean_value(details[1].text),
                    "club": details[2].text,
                    "age": details[3].text,
                }
                date = clean_date(change_point_info["date"])
                if date is not None and change_point_info["value"] > 0:
                    market_history.append(change_point_info)

            except WebDriverException:
                raise
    except NoSuchElementException:
        print(f"There is no market value history for tm_player_id --> {tm_player_id}")
    finally:
        # remove duplicated change_points
        unique_value_history = [dict(t) for t in {tuple(d.items()) for d in market_history}]
        return unique_value_history


def initialize_web_driver():
    global base_url

    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.maximize_window()

    driver.get(base_url)

    while True:
        accept_cookies = input(
            "Please accept all cookies of transfermarkt.com on google chrome and the press y\n"
        )
        if accept_cookies.upper() == "Y":
            break

    # Create 8 tabs
    for x in range(8):
        driver.execute_script(f"window.open('about:blank', '{x}');")

    return driver


def main():
    global base_url
    driver = None
    players = None
    tm_players = pd.read_csv("../datasets/tm_players.csv")
    try:
        # since execution of this function is time and resource consuming
        # we designed it to be able to pick up its results from where it left and continue
        if os.path.exists("../datasets/players.csv"):
            players = pd.read_csv("../datasets/players.csv")
        else:
            players = retrieve_players_from_dataset("../datasets/ESDB.sqlite")

        start_index = int(input("Where do you want to start? \n"))
        end_index = int(input("Where do you want to finish? \n"))
        if start_index > len(players):
            print("Start index is out of range!!!!")
            return
        if end_index > len(players):
            end_index = len(players)

        while start_index < end_index:
            if driver is None:
                driver = initialize_web_driver()
            try:
                player_dict = (players.iloc[start_index]).to_dict()
                tm_player_id = player_dict["tm_player_id"]
                value_history = player_dict["value_history"]
                if tm_player_id == 0:
                    # if tm_player is None it means that we have not set it in our data frame
                    tm_player_id = find_tm_player_id(player_dict, tm_players)
                    players.loc[start_index, "tm_player_id"] = tm_player_id
                if tm_player_id > 0 and value_history == "_":
                    print(
                        "requesting and getting information of player_id --> {} at index: {}".format(
                            tm_player_id, start_index + 1
                        )
                    )

                    players.loc[start_index, "value_history"] = str(
                        get_player_value_market_history(
                            driver, (start_index % 8), tm_player_id
                        )
                    )

                if start_index % 10 == 0:
                    print(
                        f"we have just saved a backup of progress till index ==> {start_index}"
                    )
                    players.to_csv("../dataset/players.csv", index=False)
                start_index += 1

            except WebDriverException:
                driver = None

    except Exception as e:
        print("error ---> ", str(e))
    finally:
        players.to_csv("../dataset/players.csv", index=False)


if __name__ == "__main__":
    main()

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

import pandas as pd
import winsound

base_url = "https://www.transfermarkt.com/kaka/marktwertverlauf/spieler/"


def write_csv(player_list):
    players_df = pd.DataFrame.from_records(player_list)
    players_df.to_csv("tm_players.csv")


def get_player_value_market_history(
    web_driver: webdriver, tab_number: int, player_id: int
):
    global base_url
    market_history = []
    try:
        web_driver.switch_to.window(f"{tab_number}")
        web_driver.get(f"{base_url}{player_id}")
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
                market_history.append(
                    {
                        "date": details[0].text,
                        "value": details[1].text,
                        "club": details[2].text,
                        "age": details[3].text,
                    }
                )
            except WebDriverException:
                raise
    except NoSuchElementException:
        print(f"There is no market value history for player_id --> {player_id}")
    finally:
        return market_history


def initialize_web_driver():
    global base_url

    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.maximize_window()

    driver.get(base_url)

    frequency = 2500  # Set Frequency To 2500 Hertz
    duration = 1000  # Set Duration To 1000 ms == 1 second
    winsound.Beep(frequency, duration)
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
    try:
        players = pd.read_csv("players.csv")
        players_count = len(players)
        players.insert(3, "value_history", [[] for _ in range(players_count)], True)
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
                tm_player_id = players.loc[start_index, "tm_player_id"]
                if tm_player_id != 0:
                    print(
                        "requesting and getting information of player_id --> {}".format(
                            tm_player_id
                        )
                    )
                    players.loc[start_index, "value_history"] = str(
                        get_player_value_market_history(
                            driver, (tm_player_id % 8), tm_player_id
                        )
                    )
                start_index += 1

            except WebDriverException:
                driver = None

    except Exception as e:
        print("error ---> ", str(e))
    finally:
        players.to_csv("players_value_history.csv", index=False)


if __name__ == "__main__":
    main()

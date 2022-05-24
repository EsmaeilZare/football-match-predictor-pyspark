from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

from threading import Thread
from queue import Queue
import pandas as pd
import os.path
import winsound

players = []
base_url = "https://www.transfermarkt.com/kaka/marktwertverlauf/spieler/"


def write_csv(player_list):
    players_df = pd.DataFrame.from_records(player_list)
    players_df.to_csv("tm_players.csv")


def get_player_value_market_history(web_driver: webdriver, tab_number: int, base_url: str, player_id: int):
    global players
    player, market_history = None, []
    try:
        web_driver.switch_to.window(f"{tab_number}")
        web_driver.get(f'{base_url}{player_id}')
        value_history_chart = web_driver.find_element(by=By.CLASS_NAME, value='highcharts-markers')
        value_change_points = value_history_chart.find_elements(by=By.TAG_NAME, value="image")
        for value_change_point in value_change_points:
            try:
                action = ActionChains(web_driver)
                action.move_to_element(value_change_point).perform()
                details = web_driver.find_elements(by=By.XPATH, value='//div[@class="highcharts-tooltip"]/span/b')
                if len(details) != 4:
                    continue
                market_history.append(
                    {
                        "date": details[0].text,
                        "value": details[1].text,
                        "club": details[2].text,
                        "age": details[3].text
                    }
                )
            except WebDriverException:
                raise

        name = web_driver.find_element(by=By.XPATH, value="//h1[@class='data-header__headline-wrapper']")
        birth_data = web_driver.find_element(by=By.CSS_SELECTOR, value="span[itemprop='birthDate']")
        nationality = web_driver.find_element(by=By.CSS_SELECTOR, value="span[itemprop='nationality']")
        height = web_driver.find_element(by=By.CSS_SELECTOR, value="span[itemprop='height']")
        player = {
            "id": player_id,
            "name": name.text,
            "birth_data": birth_data.text,
            "nationality": nationality.text,
            "height": height.text,
            "market_history": market_history
        }

        print("------->", player)
    except NoSuchElementException:
        print(f"There is no market value history for player_id --> {player_id}")
    finally:
        if player is not None:
            players.append(player)


class RequestWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            web_driver, tab_number, base_url, player_id, = self.queue.get()
            try:
                get_player_value_market_history(web_driver, tab_number, base_url, player_id)
            finally:
                self.queue.task_done()


def initialize_web_driver():
    global base_url

    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.maximize_window()

    driver.get(base_url)

    frequency = 2500  # Set Frequency To 2500 Hertz
    duration = 1000  # Set Duration To 1000 ms == 1 second
    winsound.Beep(frequency, duration)
    while True:
        accept_cookies = input("Please accept all cookies of transfermarkt.com on google chrome and the press y\n")
        if accept_cookies.upper() == "Y":
            break

    # Create 8 tabs
    for x in range(8):
        driver.execute_script(f"window.open('about:blank', '{x}');")

    return driver


def main():
    global players
    global base_url
    driver = None
    try:
        tm_player_id = 1
        if os.path.exists('tm_players.csv'):
            players_df = pd.read_csv("tm_players.csv")
            players = players_df.to_dict('records')
            tm_player_id = players[-1]["id"] + 1
        players_count = int(input("How many players do you want to retrieve? \n"))
        end_tm_player_id = tm_player_id + players_count

        while tm_player_id < end_tm_player_id:
            if driver is None:
                driver = initialize_web_driver()
            try:
                print(
                    "requesting and getting information of player_id --> {}".format(tm_player_id)
                )
                get_player_value_market_history(driver, (tm_player_id % 8), base_url, tm_player_id)
                tm_player_id += 1

            except WebDriverException:
                driver = None

    except Exception as e:
        print("error ---> ", str(e))
    finally:
        write_csv(players)


if __name__ == "__main__":
    main()

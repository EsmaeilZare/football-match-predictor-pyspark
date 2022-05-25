import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from queue import Queue
from threading import Thread

players = []
base_url = "https://www.transfermarkt.com/kaka/marktwertverlauf/spieler/"
headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
}


def write_csv(player_list):
    players_df = pd.DataFrame.from_records(player_list)
    players_df.to_csv("static_tm_players.csv", index=False)


def get_player_data(player_id: int):
    global players
    global base_url
    player = None
    try:
        page_tree = requests.get(f"{base_url}{player_id}", headers=headers)
        page_soup = BeautifulSoup(page_tree.content, "html.parser")
        market_value = page_soup.find(id="market-value")
        if market_value:
            name = page_soup.find("h1", class_="data-header__headline-wrapper")
            birth_data = page_soup.find("span", itemprop="birthDate")
            nationality = page_soup.find("span", itemprop="nationality")
            if all((name, birth_data, nationality)):
                player = {
                    "id": player_id,
                    "name": " ".join(name.text.split()),
                    "birth_data": " ".join(birth_data.text.split()),
                    "nationality": " ".join(nationality.text.split()),
                    "market_history": [],
                }
                print(f"player with id: {player_id} has been retrieved!\n")
    except Exception as e:
        print(
            f"During retrieving data of player with id: {player_id} following error has occurred \n {str(e)}"
        )
    finally:
        if player is not None:
            players.append(player)
            if player_id % 1000 == 0:
                write_csv(players)


class GetPlayerWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            player_id = self.queue.get()
            try:
                get_player_data(player_id)
            finally:
                self.queue.task_done()


def main():
    global players
    global base_url
    try:
        if os.path.exists("static_tm_players.csv"):
            players_df = pd.read_csv("static_tm_players.csv")
            players = players_df.to_dict("records")
            start_tm_player_id = players[-1]["id"] + 1
        else:
            start_tm_player_id = int(input("Please set the starting id of players? \n"))
        players_count = int(input("How many players do you want to retrieve? \n"))
        end_tm_player_id = start_tm_player_id + players_count

        # Create a queue to communicate with the worker threads
        queue = Queue()
        # Create 8 worker threads
        for x in range(8):
            worker = GetPlayerWorker(queue)
            # Setting daemon to True will let the main thread exit even though the workers are blocking
            worker.daemon = True
            worker.start()

        for tm_player_id in range(start_tm_player_id, end_tm_player_id):
            queue.put(tm_player_id)

        # Causes the main thread to wait for the queue to finish processing all the tasks
        queue.join()

    except Exception as e:
        print("error ---> ", str(e))
    finally:
        write_csv(players)


if __name__ == "__main__":
    main()

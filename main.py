import colorlog
from concurrent.futures import ThreadPoolExecutor
from crab_dbg import dbg
from queue import Queue
import requests
import threading
from threading import Event
import time
from typing import Dict
import yaml


def load_config(config_file="config.yaml"):
    try:
        with open(config_file, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        LOGGER.error("Error loading configuration file: " + str(e))
        exit(1)


# Global variables
# LOGGER setup
LOGGER = colorlog.getLogger()
LOGGER.setLevel(colorlog.DEBUG)
handler = colorlog.StreamHandler()
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt=None,
    reset=True,
    log_colors={
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "red,bg_white",
    },
    secondary_log_colors={},
    style="%",
)
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
CONFIG = load_config()
price_queue = Queue()
trade_queue = Queue()
log_queue = Queue()
shutdown_event = Event()


def get_jupiter_price() -> Dict[str, float] | None:
    """
    Retrieve the DEX price from the Jupiter API for SOL to USDC conversion.
    Returns:
        A dict contains the buy and sell price in USDC per SOL or None on failure.
    """
    url = "https://api.jup.ag/price/v2"

    params = {
        "ids": "So11111111111111111111111111111111111111112",  # SOL
        # "vsToken": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        "showExtraInfo": "true",
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise HTTPError for non-200 status codes

        data = response.json()
        sol_mint = params["ids"]
        sol_data = data["data"][sol_mint]
        quoted = sol_data["extraInfo"]["quotedPrice"]

        return {"buy": float(quoted["buyPrice"]), "sell": float(quoted["sellPrice"])}
    except (requests.exceptions.RequestException, KeyError, ValueError) as e:
        LOGGER.error(e)
        return None


def get_bybit_price() -> Dict[str, float] | None:
    """
    Retrieve the CEX price from the ByBit API for SOL to USDC conversion.
    Returns:
        A dict contains the buy and sell price in USDC per SOL or None on failure.
    """
    url = "https://api.bybit.com/v5/market/tickers"
    params = {"category": "spot", "symbol": "SOLUSDC"}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Automatically checks for HTTP errors

        data = response.json()
        ticker = data["result"]["list"][0]  # Access first item in ticker list

        return {"buy": float(ticker["bid1Price"]), "sell": float(ticker["ask1Price"])}

    except (
        requests.exceptions.RequestException,
        KeyError,
        IndexError,
        ValueError,
    ) as e:
        LOGGER.error(e)
        return None


def fetch_price() -> None:
    """
    Monitor jupiter and bybit, once we get the latest price, send it to price_queue.
    :return: None
    """
    while not shutdown_event.is_set():
        # We need to get prices from DEX and CEX simultaneously as prices may change fast.
        with ThreadPoolExecutor() as executor:
            jupiter_future = executor.submit(get_jupiter_price)
            bybit_future = executor.submit(get_bybit_price)
            jupiter_price = jupiter_future.result()
            bybit_price = bybit_future.result()

        LOGGER.info("jupiter: %s" % jupiter_price)
        LOGGER.info("bybit: %s" % bybit_price)

        price_queue.put(
            {
                "jupiter": jupiter_price,
                "bybit": bybit_price,
            }
        )


def price_analysis() -> None:
    """
    Analysis the received price, decide if we can make profit.
    If cannot, then do nothing; if we can, then put the trading info into trade queue.
    :return:
    """
    while not shutdown_event.is_set():
        price = price_queue.get(block=True)
        LOGGER.error(price)


def main():
    global shutdown_event

    components = [
        threading.Thread(target=fetch_price, daemon=True),
        threading.Thread(target=price_analysis, daemon=True),
    ]
    try:
        for component in components:
            component.start()

        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown_event.set()
        for component in components:
            component.join(timeout=5)
        LOGGER.warning("Program shut down due to keyboard interruption")


if __name__ == "__main__":
    main()

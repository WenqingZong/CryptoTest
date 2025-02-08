import colorlog
from concurrent.futures import ThreadPoolExecutor
from crab_dbg import dbg
import requests
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
        dbg(sol_data["extraInfo"])
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


def main():
    # We need to get prices from DEX and CEX simultaneously as prices may change fast.
    with ThreadPoolExecutor() as executor:
        jupiter_future = executor.submit(get_jupiter_price)
        bybit_future = executor.submit(get_bybit_price)
        jupiter_price = jupiter_future.result()
        bybit_price = bybit_future.result()
    LOGGER.info(jupiter_price)
    LOGGER.info(bybit_price)


if __name__ == "__main__":
    main()

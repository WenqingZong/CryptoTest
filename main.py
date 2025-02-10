import aiohttp
from aiohttp import ClientTimeout
import asyncio
import colorlog

# from crab_dbg import dbg
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import signal
from typing import Coroutine, List
from threading import Event
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
price_queue = asyncio.Queue()
trade_queue = asyncio.Queue()
log_queue = asyncio.Queue()
shutdown_event = Event()


@dataclass
class MarketPrice:
    """
    Ask and bid price get from DEX/CEX API
    """

    def __init__(self, buy: float, sell: float):
        self._buy = buy
        self._sell = sell
        self._timestamp = datetime.now()

    # 大坑，交易所的卖是我的买，交易所的买是我的卖！！！！
    def user_buy(self) -> float:
        return self._sell

    def user_sell(self) -> float:
        return self._buy

    def __str__(self):
        return "Bid (User Sell): %f, Ask (User Buy): %f at timestamp %s" % (
            self._buy,
            self._sell,
            self._timestamp,
        )


@dataclass
class ArbitrageOpportunity:
    """
    A pair of DEX and CEX prices. We may find arbitrage opportunity from here.
    """

    def __init__(self, jupiter: MarketPrice, bybit: MarketPrice, coin_name: str):
        self._jupiter = jupiter
        self._bybit = bybit
        self._coin_name = coin_name

    def get_jupiter(self) -> MarketPrice:
        return self._jupiter

    def get_bybit(self) -> MarketPrice:
        return self._bybit

    def get_coin_name(self) -> str:
        return self._coin_name

    def __str__(self):
        return "%s Jupiter: %s, Bybit: %s" % (
            self._coin_name,
            self._jupiter,
            self._bybit,
        )


class TradeType(Enum):
    # We, the user, buy from jupiter and sell at bybit
    JUPITER_TO_BYBIT = 1

    # We, the user, buy from bybit and sell at jupiter
    BYBIT_TO_JUPITER = 2


class TradeResult(Enum):
    NotProcessed = 1
    Finished = 2
    Failed = 3


@dataclass
class Trade:
    """
    Once we believe we can gain profit, issue a Trade object and store its result.
    """

    def __init__(
        self,
        amount: float,
        trade_type: TradeType,
        arbitrage_opportunity: ArbitrageOpportunity,
        expected_profit: float,
    ):
        self._amount = amount
        self._trade_type = trade_type
        self._arbitrage_opportunity = arbitrage_opportunity
        self._expected_profit = expected_profit

        self._trade_result = TradeResult.NotProcessed
        self._actual_profit = 0.0

    def get_trade_type(self) -> TradeType:
        return self._trade_type

    def get_arbitrage_opportunity(self) -> ArbitrageOpportunity:
        return self._arbitrage_opportunity

    def set_trade_result(self, trade_result: TradeResult) -> None:
        self._trade_result = trade_result

    def set_actual_profit(self, actual_profit) -> None:
        self._actual_profit = actual_profit

    def __str__(self):
        coin_name = self._arbitrage_opportunity.get_coin_name()

        if self._trade_type == TradeType.JUPITER_TO_BYBIT:
            user_buy_price = self._arbitrage_opportunity.get_jupiter().user_buy()
            user_sell_price = self._arbitrage_opportunity.get_bybit().user_sell()
            return (
                "Buy %f %s from Jupiter with price %f, sell at Bybit with price %f, expected profit %f USDC"
                % (
                    self._amount,
                    coin_name,
                    user_buy_price,
                    user_sell_price,
                    self._expected_profit,
                )
            )
        else:
            user_buy_price = self._arbitrage_opportunity.get_bybit().user_buy()
            user_sell_price = self._arbitrage_opportunity.get_jupiter().user_sell()
            return (
                "Buy %f %s from Bybit with price %f, sell at Jupiter with price %f, expected profit %f USDC"
                % (
                    self._amount,
                    coin_name,
                    user_buy_price,
                    user_sell_price,
                    self._expected_profit,
                )
            )


async def get_jupiter_price(session: aiohttp.ClientSession) -> MarketPrice | None:
    """
    Retrieve the DEX price from the Jupiter API for SOL to USDC conversion.
    Returns:
        A MarketPrice object contains the buy and sell price in USDC per SOL or None on failure.
    """
    url = "https://api.jup.ag/price/v2"

    params = {
        "ids": "So11111111111111111111111111111111111111112",  # SOL
        # "vsToken": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        "showExtraInfo": "true",
    }
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()

            sol_mint = params["ids"]
            sol_data = data["data"][sol_mint]
            quoted = sol_data["extraInfo"]["quotedPrice"]

            return MarketPrice(float(quoted["buyPrice"]), float(quoted["sellPrice"]))
    except (aiohttp.ClientError, KeyError, ValueError) as e:
        LOGGER.error(f"Jupiter API error: {str(e)}")
        return None
    except Exception as e:
        LOGGER.exception(f"Jupiter API unknown error: {str(e)}")
        return None


async def get_bybit_price(session: aiohttp.ClientSession) -> MarketPrice | None:
    """
    Retrieve the CEX price from the ByBit API for SOL to USDC conversion.
    Returns:
        A MarketPrice object contains the buy and sell price in USDC per SOL or None on failure.
    """
    url = "https://api.bybit.com/v5/market/tickers"
    params = {"category": "spot", "symbol": "SOLUSDC"}

    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()

            ticker = data["result"]["list"][0]
            return MarketPrice(float(ticker["bid1Price"]), float(ticker["ask1Price"]))
    except (aiohttp.ClientError, KeyError, IndexError) as e:
        LOGGER.error(f"Bybit API error: {str(e)}")
        return None
    except Exception as e:
        LOGGER.exception(f"Bybit API unknown error: {str(e)}")
        return None


async def fetch_price(session: aiohttp.ClientSession) -> None:
    """
    Monitor jupiter and bybit, once we get the latest price, send it to price_queue.
    :return: None
    """
    while not shutdown_event.is_set():
        try:
            # Handle two API requests at the same time
            jupiter_price, bybit_price = await asyncio.gather(
                get_jupiter_price(session),
                get_bybit_price(session),
                return_exceptions=True,
            )

            # Exception handling
            if isinstance(jupiter_price, Exception):
                LOGGER.error(f"Jupiter request error: {str(jupiter_price)}")
                jupiter_price = None
            if isinstance(bybit_price, Exception):
                LOGGER.error(f"Bybit request error: {str(bybit_price)}")
                bybit_price = None

            # Add the price to price_queue for further analysis
            await price_queue.put(
                ArbitrageOpportunity(jupiter_price, bybit_price, "SOL")
            )

        except asyncio.CancelledError:
            break
        except Exception as e:
            LOGGER.exception(f"Price fetching loop error: {str(e)}")
        finally:
            await asyncio.sleep(0.1)


async def price_analysis() -> None:
    """
    Analysis the received price, decide if we can make profit via arbitrage.
    If cannot, then do nothing; if we can, then put the trading info into trade queue.
    :return:
    """
    SERVICE_CHARGE_PERCENTAGE = CONFIG["SERVICE_CHARGE_PERCENTAGE"]
    ARBITRAGE_PERCENTAGE_THRESHOLD = CONFIG["ARBITRAGE_PERCENTAGE_THRESHOLD"]
    TRADE_AMOUNT = CONFIG["TRADE_AMOUNT"]

    while not shutdown_event.is_set():
        try:
            price: ArbitrageOpportunity = await price_queue.get()
            LOGGER.debug("Analysing %s" % price)
            jupiter_user_buy = price.get_jupiter().user_buy()
            jupiter_user_sell = price.get_jupiter().user_sell()
            bybit_user_buy = price.get_bybit().user_buy()
            bybit_user_sell = price.get_bybit().user_sell()

            # Trade type 1: Jupiter to Bybit
            cost_type1 = jupiter_user_buy * (1 + SERVICE_CHARGE_PERCENTAGE)
            revenue_type1 = bybit_user_sell * (1 - SERVICE_CHARGE_PERCENTAGE)
            profit_type1 = revenue_type1 - cost_type1
            profit_percent_type1 = (profit_type1 / cost_type1) * 100

            # Trade type 2: Bybit to Jupiter
            cost_type2 = bybit_user_buy * (1 + SERVICE_CHARGE_PERCENTAGE)
            revenue_type2 = jupiter_user_sell * (1 - SERVICE_CHARGE_PERCENTAGE)
            profit_type2 = revenue_type2 - cost_type2
            profit_percent_type2 = (profit_type2 / cost_type2) * 100

            trade = None
            if profit_percent_type1 >= ARBITRAGE_PERCENTAGE_THRESHOLD:
                trade = Trade(
                    TRADE_AMOUNT,
                    TradeType.JUPITER_TO_BYBIT,
                    price,
                    TRADE_AMOUNT * profit_type1,
                )
            elif profit_percent_type2 >= ARBITRAGE_PERCENTAGE_THRESHOLD:
                trade = Trade(
                    TRADE_AMOUNT,
                    TradeType.BYBIT_TO_JUPITER,
                    price,
                    TRADE_AMOUNT * profit_type2,
                )

            if trade is not None:
                await trade_queue.put(trade)
                LOGGER.info("Find a potential arbitrage trade: %s" % trade)
        except asyncio.CancelledError:
            break
        except Exception as e:
            LOGGER.exception(e)


async def actual_trade() -> None:
    """
    Perform the actual trade operation via Jupiter and Bybit API.
    :return: None
    """
    while not shutdown_event.is_set():
        try:
            trade: Trade = await trade_queue.get()
            LOGGER.info("Handling arbitrage %s" % trade)
        except asyncio.CancelledError:
            break
        except Exception as e:
            LOGGER.exception(e)


async def graceful_shutdown(
    signal: signal.Signals, session: aiohttp.ClientSession
) -> None:
    """
    Handle system shutdown signal in a graceful manner
    Return: None
    """
    LOGGER.warning(f"Received signal {signal.name}, handling it...")
    shutdown_event.set()

    await session.close()

    # Cancel all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def main() -> None:
    global shutdown_event

    LOGGER.info("Running async arbitrage bot")
    loop = asyncio.get_running_loop()

    SESSION_TIMEOUT = ClientTimeout(total=5)
    session = aiohttp.ClientSession(timeout=SESSION_TIMEOUT)

    # Register system signal handler
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, lambda: asyncio.create_task(graceful_shutdown(sig, session))
        )

    tasks: List[Coroutine] = [
        fetch_price(session),
        price_analysis(),
        actual_trade(),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        await session.close()
        LOGGER.warning("Bot terminated")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        LOGGER.error("Unexpected exception: %s" % e)

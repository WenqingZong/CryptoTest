import aiohttp
from aiohttp import ClientTimeout
import asyncio
import base58
import base64
import colorlog

from crab_dbg import dbg
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import hmac
import hashlib
import logging
import json
import signal
import os
from pybit.unified_trading import HTTP
from solana.rpc.async_api import AsyncClient
from solders import message
from solders.transaction import VersionedTransaction
from solders.keypair import Keypair
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

# To suppress unwanted logs from dependencies
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("pybit").setLevel(logging.WARNING)
logging.getLogger("solana.rpc.commitment").setLevel(logging.WARNING)

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

    def get_timestamp(self):
        return self._timestamp

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
        self._jupiter_transaction_id = ""
        self._bybit_order_id = ""

    def get_trade_type(self) -> TradeType:
        return self._trade_type

    def get_arbitrage_opportunity(self) -> ArbitrageOpportunity:
        return self._arbitrage_opportunity

    def get_trade_result(self):
        return self._trade_result

    def get_trade_amount(self):
        return self._amount

    def get_expected_profit(self):
        return self._expected_profit

    def get_jupiter_transaction_id(self) -> str:
        return self._jupiter_transaction_id

    def get_bybit_order_id(self) -> str:
        return self._bybit_order_id

    def set_bybit_order_id(self, id: str):
        self._bybit_order_id = id

    def set_jupiter_transaction_id(self, id: str):
        self._jupiter_transaction_id = id

    def set_trade_result(self, trade_result: TradeResult) -> None:
        self._trade_result = trade_result

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
    Retrieve the DEX price from the Jupiter API for COIN1 to COIN2 conversion.
    Returns:
        A MarketPrice object contains the buy and sell price in COIN2 per COIN1 or None on failure.
    """
    COIN1_MINT = CONFIG["COIN1_MINT"]
    COIN2_MINT = CONFIG["COIN2_MINT"]
    url = "https://api.jup.ag/price/v2"

    params = {
        "ids": COIN1_MINT,
        "vsToken": COIN2_MINT,
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
    COIN1_NAME = CONFIG["COIN1_NAME"]
    COIN2_NAME = CONFIG["COIN2_NAME"]

    url = "https://api.bybit.com/v5/market/tickers"
    params = {"category": "spot", "symbol": COIN1_NAME + COIN2_NAME}

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
    COIN1_NAME = CONFIG["COIN1_NAME"]

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
                ArbitrageOpportunity(jupiter_price, bybit_price, COIN1_NAME)
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


async def record_trade_in_json(trade: Trade) -> None:
    """
    Record the given trade activity in json format.
    :param trade: The processed trade object.
    :return: None
    """
    assert (
        trade.get_trade_result() != TradeResult.NotProcessed
    ), "Unprocessed trade object shouldn't be added to json record"
    file_path = "trade_records.json"

    records = []
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                records = json.load(f)
        except Exception:
            pass

    # Convert the trade object into a dictionary.
    # We manually extract fields from the trade and its related objects.
    jupiter = trade.get_arbitrage_opportunity().get_jupiter()
    bybit = trade.get_arbitrage_opportunity().get_bybit()

    trade_record = {
        "amount": trade.get_trade_amount(),
        "trade_type": trade.get_trade_type().name,  # Enum to string
        "coin_name": trade.get_arbitrage_opportunity().get_coin_name(),
        "expected_profit": trade.get_expected_profit(),
        "trade_result": trade.get_trade_result().name,  # Enum to string
        "timestamp": datetime.now().isoformat(),
        "jupiter": {
            "buy": jupiter.user_buy(),
            "sell": jupiter.user_sell(),
            "timestamp": jupiter._timestamp.isoformat(),
        },
        "jupiter_transaction_id": trade.get_jupiter_transaction_id(),
        "bybit": {
            "buy": bybit.user_buy(),
            "sell": bybit.user_sell(),
            "timestamp": bybit.get_timestamp().isoformat(),
        },
        "bybit_order_id": trade.get_bybit_order_id(),
    }

    # Append the new trade record to the list.
    records.append(trade_record)

    # Write the updated list back to the JSON file.
    with open(file_path, "w") as f:
        json.dump(records, f, indent=4)


async def report_trade_to_telegram(trade: Trade) -> None:
    """
    Report the given trade activity to telegram.
    :param trade: The processed trade object
    :return: None
    """
    # Ensure that only processed trades are reported.
    assert (
        trade.get_trade_result() != TradeResult.NotProcessed
    ), "Unprocessed trade object shouldn't be reported to telegram"

    # Retrieve Telegram credentials from environment variables.
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        LOGGER.warning("No telegram bot configured, doing nothing here")
        return

    # Compose the message to send. We use the trade's string representation.
    message = f"Trade Report:\n{trade}"

    # Telegram API endpoint for sending messages.
    telegram_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    # Define the payload parameters.
    params = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }

    # Create an asynchronous HTTP session and send the POST request.
    async with aiohttp.ClientSession() as session:
        async with session.post(telegram_url, data=params) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(
                    f"Failed to send Telegram message: {response.status} - {error_text}"
                )


async def sign_and_send_transaction(swap_tx_base64: str) -> str:
    """
    Broadcast the desired transaction on Solana chain.
    :param swap_tx_base64: swapTransaction attribute get from swap API response
    :return: Jupiter transaction id
    """
    BITGET_WALLET_PRIVATE_KEY = CONFIG["BITGET_WALLET_PRIVATE_KEY"]

    # Load the wallet's private key
    private_key_bytes = base58.b58decode(BITGET_WALLET_PRIVATE_KEY)
    keypair = Keypair.from_bytes(private_key_bytes)

    # Sign the transaction with private key
    raw_transaction = VersionedTransaction.from_bytes(base64.b64decode(swap_tx_base64))
    signature = keypair.sign_message(
        message.to_bytes_versioned(raw_transaction.message)
    )
    signed_txn = VersionedTransaction.populate(raw_transaction.message, [signature])

    # Serialize the signed transaction
    signed_txn_bytes = bytes(signed_txn)

    # Send the transaction to Solana
    async with AsyncClient("https://api.mainnet-beta.solana.com") as client:
        # Send the raw transaction
        send_response = await client.send_raw_transaction(signed_txn_bytes)
        LOGGER.info(
            "Jupiter transaction confirmed with transaction id: %s"
            % send_response.value
        )
        return str(send_response.value)


# Helper function to generate the signature for Bybit v5 API requests.
def generate_signature(
    api_key: str, api_secret: str, timestamp: str, recv_window: str, body: str
) -> str:
    # The signature is computed as: HMAC_SHA256(secret, f"{timestamp}{api_key}{recv_window}{body}")
    message = f"{timestamp}{api_key}{recv_window}{body}"
    signature = hmac.new(
        api_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return signature


async def handle_jupiter_to_bybit_trade(
    trade: Trade, session: aiohttp.ClientSession
) -> None:
    """
    Do the actual trade, we buy from jupiter and sell at bybit.
    :param trade: The desired trade object, we'll also update trade result in this object.
    :return: None
    """
    BITGET_WALLET_ADDRESS = CONFIG["BITGET_WALLET_ADDRESS"]

    COIN1_MINT = CONFIG["COIN1_MINT"]
    COIN1_NAME = CONFIG["COIN1_NAME"]
    COIN1_DECIMAL = CONFIG["COIN1_DECIMAL"]

    COIN2_MINT = CONFIG["COIN2_MINT"]
    COIN2_NAME = CONFIG["COIN2_NAME"]

    BYBIT_API_KEY = CONFIG["BYBIT_API_KEY"]
    BYBIT_API_SECRET = CONFIG["BYBIT_API_SECRET"]

    try:
        amount = trade.get_trade_amount()
        arbitrage_opportunity = trade.get_arbitrage_opportunity()
        amount_int = int(amount * 10**COIN1_DECIMAL)
        quote_params = {
            "inputMint": COIN1_MINT,
            "outputMint": COIN2_MINT,
            "amount": amount_int,
            "slippageBps": 50,
            "swapMode": "ExactIn",
        }

        # Request a quote from Jupiter.
        quote_data = None
        async with session.get(
            "https://api.jup.ag/swap/v1/quote", params=quote_params
        ) as resp:
            if resp.status != 200:
                raise Exception(
                    "Jupiter quote request failed with status %d" % resp.status
                )
            quote_data = await resp.json()

        # Request a swap from Jupiter.
        swap_payload = {
            "quoteResponse": quote_data,
            "userPublicKey": BITGET_WALLET_ADDRESS,
        }
        swap_result = None
        async with session.post(
            "https://api.jup.ag/swap/v1/swap", json=swap_payload
        ) as swap_resp:
            if swap_resp.status != 200:
                raise Exception(
                    "Jupiter swap request failed with status %d" % swap_resp.status
                )
            swap_result = await swap_resp.json()

        # Broadcast this transaction on solana chain
        jupiter_transaction_id = await sign_and_send_transaction(
            swap_result["swapTransaction"]
        )
        trade.set_jupiter_transaction_id(jupiter_transaction_id)

        # Now, place a sell order on Bybit.
        bybit_session = HTTP(
            testnet=False, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET
        )
        bybit_response = bybit_session.place_order(
            category="spot",
            symbol=COIN1_NAME + COIN2_NAME,
            side="Sell",
            orderType="Limit",
            qty=str(amount),
            price=str(arbitrage_opportunity.get_bybit().user_sell()),
            timeInForce="PostOnly",
            isLeverage=0,
            orderFilter="Order",
        )
        if bybit_response["retCode"] != 0:
            raise Exception("Bybit API failed with error %s" % bybit_response["retMsg"])
        bybit_order_id = bybit_response["result"]["orderId"]
        trade.set_bybit_order_id(bybit_order_id)
        LOGGER.info("Bybit transaction confirmed with order id: %s" % bybit_order_id)

        trade.set_trade_result(TradeResult.Finished)
    except Exception as e:
        trade.set_trade_result(TradeResult.Failed)
        LOGGER.error("Trade %s failed due to %s" % (trade, e))
    finally:
        await report_trade_to_telegram(trade)
        await record_trade_in_json(trade)


async def handle_bybit_to_jupiter_trade(
    trade: Trade, session: aiohttp.ClientSession
) -> None:
    """
    Do the actual trade, we buy from bybit and sell at jupiter.
    :param trade: The desired trade object, we'll also update trade result in this object.
    :return: None
    """
    """
    Do the actual trade, we buy from jupiter and sell at bybit.
    :param trade: The desired trade object, we'll also update trade result in this object.
    :return: None
    """
    BITGET_WALLET_ADDRESS = CONFIG["BITGET_WALLET_ADDRESS"]

    COIN1_MINT = CONFIG["COIN1_MINT"]
    COIN1_NAME = CONFIG["COIN1_NAME"]
    COIN1_DECIMAL = CONFIG["COIN1_DECIMAL"]

    COIN2_MINT = CONFIG["COIN2_MINT"]
    COIN2_NAME = CONFIG["COIN2_NAME"]

    BYBIT_API_KEY = CONFIG["BYBIT_API_KEY"]
    BYBIT_API_SECRET = CONFIG["BYBIT_API_SECRET"]

    try:
        amount = trade.get_trade_amount()
        arbitrage_opportunity = trade.get_arbitrage_opportunity()

        # Place a buy order on Bybit.
        bybit_session = HTTP(
            testnet=False, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET
        )
        bybit_response = bybit_session.place_order(
            category="spot",
            symbol=COIN1_NAME + COIN2_NAME,
            side="Buy",
            orderType="Limit",
            qty=str(amount),
            price=str(arbitrage_opportunity.get_bybit().user_buy()),
            timeInForce="PostOnly",
            isLeverage=0,
            orderFilter="Order",
        )
        if bybit_response["retCode"] != 0:
            raise Exception("Bybit API failed with error %s" % bybit_response["retMsg"])
        bybit_order_id = bybit_response["result"]["orderId"]
        trade.set_bybit_order_id(bybit_order_id)
        LOGGER.info("Bybit transaction confirmed with order id: %s" % bybit_order_id)

        # Now, sell on jupiter
        amount_int = int(amount * 10**COIN1_DECIMAL)
        quote_params = {
            "inputMint": COIN2_MINT,
            "outputMint": COIN1_MINT,
            "amount": amount_int,
            "slippageBps": 50,
            "swapMode": "ExactIn",
        }

        # Request a quote from Jupiter.
        quote_data = None
        async with session.get(
            "https://api.jup.ag/swap/v1/quote", params=quote_params
        ) as resp:
            quote_data = await resp.json()
            if resp.status != 200:
                raise Exception(
                    "Jupiter quote request failed with status %d" % resp.status
                )

        # Request a swap from Jupiter.
        swap_payload = {
            "quoteResponse": quote_data,
            "userPublicKey": BITGET_WALLET_ADDRESS,
        }
        swap_result = None
        async with session.post(
            "https://api.jup.ag/swap/v1/swap", json=swap_payload
        ) as swap_resp:
            if swap_resp.status != 200:
                raise Exception(
                    "Jupiter swap request failed with status %d" % swap_resp.status
                )
            swap_result = await swap_resp.json()
        # Broadcast this transaction on solana chain
        jupiter_transaction_id = await sign_and_send_transaction(
            swap_result["swapTransaction"]
        )
        trade.set_jupiter_transaction_id(jupiter_transaction_id)

        trade.set_trade_result(TradeResult.Finished)
    except Exception as e:
        trade.set_trade_result(TradeResult.Failed)
        LOGGER.error("Trade %s failed due to %s" % (trade, e))
    finally:
        await report_trade_to_telegram(trade)
        await record_trade_in_json(trade)


async def actual_trade(session: aiohttp.ClientSession) -> None:
    """
    Perform the actual trade operation via Jupiter and Bybit API.
    :return: None
    """
    while not shutdown_event.is_set():
        try:
            trade: Trade = await trade_queue.get()
            LOGGER.info("Handling arbitrage %s" % trade)
            if trade.get_trade_type() == TradeType.JUPITER_TO_BYBIT:
                await handle_jupiter_to_bybit_trade(trade, session)
            else:
                await handle_bybit_to_jupiter_trade(trade, session)
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
        actual_trade(session),
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

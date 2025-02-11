import aiohttp
from aiohttp import ClientTimeout
import asyncio
import base64
import colorlog

from crab_dbg import dbg
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import hmac
import hashlib
import json
import signal
from solana.rpc.async_api import AsyncClient
from solders.transaction import VersionedTransaction
from solders.keypair import Keypair
import time
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
LOGGER.setLevel(colorlog.INFO)
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

    def get_trade_result(self):
        return self._trade_result

    def get_trade_amount(self):
        return self._amount

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


async def record_trade_in_json(trade: Trade) -> None:
    """
    Record the given trade activity in json format.
    :param trade: The processed trade object.
    :return: None
    """
    assert (
        trade.get_trade_result() != TradeResult.NotProcessed
    ), "Unprocessed trade object shouldn't be added to json record"
    pass


async def report_trade_to_telegram(trade: Trade) -> None:
    """
    Report the given trade activity to telegram.
    :param trade: The processed trade object
    :return:
    """
    assert (
        trade.get_trade_result() != TradeResult.NotProcessed
    ), "Unprocessed trade object shouldn't be reported to telegram"
    pass


async def sign_and_send_transaction(swap_tx_base64: str, keypair: Keypair):
    """
    Given a base64-encoded (unsigned) swap transaction from Jupiter,
    this function:
      1. Decodes and deserializes the transaction (assuming it's a VersionedTransaction),
      2. Signs it with the provided keypair,
      3. Sends it to the network, and
      4. Waits for confirmation.

    Returns the transaction signature.
    """
    # # Create a Solana RPC client
    # client = AsyncClient("https://api.devnet.solana.com")
    #
    # # Decode the base64 transaction into bytes
    # tx_bytes = base64.b64decode(swap_tx_base64)
    #
    # # Deserialize into a VersionedTransaction (Jupiter returns versioned transactions)
    # try:
    #     tx = VersionedTransaction.from_bytes(tx_bytes)
    # except Exception as e:
    #     raise Exception(f"Failed to deserialize the transaction: {e}")
    #
    # # Sign the transaction using your keypair
    # tx.sign([keypair])
    #
    # # Serialize the signed transaction back into bytes
    # signed_tx_bytes = tx.serialize()
    #
    # # Send the raw signed transaction (skip preflight if desired)
    # send_response = client.send_raw_transaction(signed_tx_bytes, skip_preflight=True)
    #
    # if not send_response.get("result"):
    #     raise Exception(f"Failed to send transaction: {send_response}")
    #
    # signature = send_response["result"]
    # print("Transaction signature:", signature)
    #
    # # Wait for confirmation (polling)
    # timeout_sec = 30
    # start_time = time.time()
    # confirmed = False
    # while time.time() - start_time < timeout_sec:
    #     confirmation = client.get_signature_statuses([signature])
    #     status = confirmation.get("result", {}).get("value", [None])[0]
    #     if status is not None and status.get("confirmationStatus") in ("confirmed", "finalized"):
    #         confirmed = True
    #         break
    #     await asyncio.sleep(1)  # Wait a second before polling again
    #
    # if confirmed:
    #     print("Transaction confirmed!")
    # else:
    #     raise Exception("Transaction confirmation timed out.")
    #
    # return signature
    pass


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
    COIN2_DECIMAL = CONFIG["COIN2_DECIMAL"]

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
            dbg(quote_data)

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
            dbg(swap_result)

        # Now, place a sell order on Bybit.
        order_payload = {
            "category": "spot",
            "symbol": COIN1_NAME + COIN2_NAME,
            "side": "Sell",
            "orderType": "Limit",
            "qty": str(amount * 10**COIN2_DECIMAL),
            "price": str(arbitrage_opportunity.get_bybit().user_sell()),
        }
        body = json.dumps(order_payload)

        # Prepare authentication headers.
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"

        signature = generate_signature(
            BYBIT_API_KEY, BYBIT_API_SECRET, timestamp, recv_window, body
        )
        headers = {
            "Content-Type": "application/json",
            "X-BAPI-API-KEY": BYBIT_API_KEY,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "X-BAPI-SIGN": signature,
        }

        async with session.post(
            "https://api.bybit.com/v5/order/create", data=body, headers=headers
        ) as response:
            resp_json = await response.json()
            dbg(resp_json)
            if resp_json.get("retCode") != 0:
                raise Exception(
                    "Bybit API request failed with return code %d"
                    % resp_json.get("retCode")
                )

        trade.set_trade_result(TradeResult.Finished)
    except Exception as e:
        trade.set_trade_result(TradeResult.Failed)
        trade.set_actual_profit(0.0)
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
    pass


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
                await handle_jupiter_to_bybit_trade(trade, session)
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

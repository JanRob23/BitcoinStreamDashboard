import os
import base64
import hmac
import hashlib
import time
import aiohttp
import asyncio
import json
import logging
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ.get('SANDBOX_API_KEY')
api_secret = os.environ.get('SANDBOX_SECRET_KEY')
SIGNATURE_PATH = '/users/self/verify'


def generate_signature():
    timestamp = str(time.time())
    message = f'{timestamp}GET{SIGNATURE_PATH}'
    hmac_key = base64.b64decode(api_secret)
    signature = hmac.new(
        hmac_key,
        message.encode('utf-8'),
        digestmod=hashlib.sha256).digest()
    signature_b64 = base64.b64encode(signature).decode().rstrip('\n')
    return signature_b64, timestamp

async def get_trades(session, product_id="BTC-USD"):
    url = f"https://api.exchange.coinbase.com/products/{product_id}/trades"
    signature, timestamp = generate_signature()

    headers = {
        'CB-ACCESS-KEY': api_key,
        'CB-ACCESS-SIGN': signature,
        'CB-ACCESS-TIMESTAMP': timestamp,
    }

    async with session.get(url, headers=headers) as response:
        return await response.json()

async def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
    )
    product_ids = ["BTC-USD", "BTC-EUR", "BTC-GBP"]

    async with aiohttp.ClientSession() as session:
        with app.get_producer() as producer:
            while True:
                tasks = [get_trades(session, product_id) for product_id in product_ids]
                results = await asyncio.gather(*tasks)

                all_trades = []
                for product_id, trades in zip(product_ids, results):
                    for t in trades:
                        t.update({'product_id': product_id})
                    # get min and max timestamps
                    min_timestamp = min([t['time'] for t in trades])
                    max_timestamp = max([t['time'] for t in trades])
                    # logging.debug("Got response: %s", trades)
                    logging.debug("Got response: %s", min_timestamp, max_timestamp)
                    all_trades.extend(trades)
                    producer.produce(
                        topic=product_id,
                        key="exchange-rates",
                        value=json.dumps(all_trades),
                    )
                logging.info("Produced. Sleeping...")
                await asyncio.sleep(2)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    asyncio.run(main())

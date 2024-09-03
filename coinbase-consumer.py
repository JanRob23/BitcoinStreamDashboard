import logging
from quixstreams import Application
from uuid import uuid4
from datetime import timedelta, datetime
from google.cloud import bigquery
import ast
import json
from google.oauth2 import service_account
import pandas as pd


def initalize_fn(agg):
    return {
            "avg_sale_price": 0,
            "avg_buy_price": 0,
            "sale_count": 0,
            "buy_count": 0,
            "total_sale_size": 0,
            "total_buy_size": 0,
        }

def prepare_rows_for_bigquery(data):
    print("Timeframe of kafka messages: ", data["start"] * 0.001, data["end"] * 0.001)
    rows = []
    for item in data["value"]:
        print("ITEM", item, "\n")
        row = {
            'avg_sale_price': item['avg_sale_price'],
            'avg_buy_price': item['avg_buy_price'],
            'sale_count': item['sale_count'],
            'buy_count': item['buy_count'],
            'total_sale_size': item['total_sale_size'],
            'total_buy_size': item['total_buy_size'],
            'product_id': item['product_id'],
            "start_time": item["start_time"],
            "end_time": item["end_time"]
        }
        rows.append(row)

    # find all rows that have the same start_time 
    start_times = [row["start_time"] for row in rows]
    # check if x occurs more then once in start_times
    same_start_time = [x for x in rows if start_times.count(x["start_time"]) > 1]
    print("Same start time: ", same_start_time)
    # print(rows)
    # print("end data \n \n \n \n ")
    return rows

def initialize_summary_dict():
    return {
            "avg_sale_price": 0,
            "avg_buy_price": 0,
            "sale_count": 0,
            "buy_count": 0,
            "total_sale_size": 0,
            "total_buy_size": 0,
        }

def reduce_groupby(_, data):
    print("len data", len(data))    
    summarized_messages = {}
    for msg in data:
        # summarize the data, it is only one currency
        time_key = pd.to_datetime(msg['time']).floor('10s')
        if time_key not in summarized_messages:
            # create a new summary dict for each time key (here every second)
            summarized_messages[time_key] = {
                "avg_sale_price": 0,
                "avg_buy_price": 0,
                "sale_count": 0,
                "buy_count": 0,
                "total_sale_size": 0,
                "total_buy_size": 0
            }
        if msg["side"] == "sell":
            summarized_messages[time_key]["avg_sale_price"] += float(msg["price"])
            summarized_messages[time_key]["sale_count"] += 1
            summarized_messages[time_key]["total_sale_size"] += float(msg["size"])
        elif msg["side"] == "buy":
            summarized_messages[time_key]["avg_buy_price"] += float(msg["price"])
            summarized_messages[time_key]["buy_count"] += 1
            summarized_messages[time_key]["total_buy_size"] += float(msg["size"])
    aggregated_messages = []
    i = 0
    # calculate the averages and create a serializable list of dicts
    for key, value in summarized_messages.items():
        if value["sale_count"] > 0:
            value["avg_sale_price"] /= value["sale_count"]
        if value["buy_count"] > 0:
            value["avg_buy_price"] /= value["buy_count"]
        # in case there was no sale or buy in this second, take the value from the previous second
        if i > 0:
            if value["avg_sale_price"] == 0:
                value["avg_sale_price"] = aggregated_messages[i-1]["avg_sale_price"]
            if value["avg_buy_price"] == 0:
                value["avg_buy_price"] = aggregated_messages[i-1]["avg_buy_price"]
        time_stamp = key.isoformat()
        value["start_time"] = time_stamp
        value["end_time"] = key + timedelta(seconds=10)
        value["end_time"] = value["end_time"].isoformat()
        value["product_id"] = data[0]["product_id"]
        
        aggregated_messages.append(value)
        i += 1
    
    print(f"Aggregated over timeframe {min(summarized_messages.keys())} to {max(summarized_messages.keys())}")
    print("total time delta is: ", max(summarized_messages.keys()) - min(summarized_messages.keys()))
    print("Length of aggregate: ", len(aggregated_messages))    
    return aggregated_messages
            
    

    
def main():
    #Create BQ credentials object
    credentials = service_account.Credentials.from_service_account_file('coinbase-proj-941cedd1d50d.json')

    # Construct a BigQuery client object.
    bq_client = bigquery.Client(credentials=credentials)

    #Speficy BigQuery table to stream to
    table_id = 'coinbase-proj.coinbase_stream_data.BTC_to_currencies'
    
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        auto_offset_reset="latest",
    )
    app.clear_state()
    # input_topics = ["BTC-USD", "BTC-EUR", "BTC-GBP"] 1724317080
    # for topic in input_topics:
    topic = "BTC-USD"
    input_topic = app.topic(topic)

    sdf = app.dataframe(input_topic)
    # print(sdf)
    # # sdf = sdf.group_into_hourly_batches(...)
    sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=60))
    sdf = sdf.reduce(initializer=initalize_fn, reducer=reduce_groupby)
    sdf = sdf.final()
    print("Currency: ", topic)
    #sdf = sdf.apply(reduce_groupby)
    
    # sdf = sdf.update(lambda msg: logging.debug("Got: %s", msg))
    def write_to_bigquery(data):
        rows_to_insert = prepare_rows_for_bigquery(data)
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        if errors == []:
            # print("Added rows: ", rows_to_insert)
            print(f"New rows have been added to {table_id}")
        else:
            print(f"Encountered errors while inserting rows: {errors}")

    sdf = sdf.apply(write_to_bigquery)

        #sdf = sdf.apply(reduce_fn)
        # sdf = sdf.update(lambda msg: logging.debug("Got"))
    app.run(dataframe=sdf)

    # with app.get_consumer() as consumer:
    #     consumer.subscribe(["BTC-USD"])

    #     while True:
    #         msg = consumer.poll(1)

    #         if msg is None:
    #             print("Waiting...")
    #         elif msg.error() is not None:
    #             raise Exception(msg.error())
    #         else:
    #             key = msg.key().decode("utf8")
    #             value = json.loads(msg.value())
    #             offset = msg.offset()

    #             #print(f"{offset} {key} {value}")
                
    #             summary_per_second = reduce_groupby(value)
    #             consumer.store_offsets(msg)
    #             # if value != "{'message': 'NotFound'}":
    #             #     ##### Stream data into BigQuery table #######
    #             #     rows_to_insert = [value]
    #             #     print((rows_to_insert))
    #             #     errors = bq_client.insert_rows_json(table_id,value) #Make API request

    #             #     if errors==[]:
    #             #         print("New rows added.")
    #             #     else:
    #             #         print("Encountered erros while inserting rows: {}".format(errors))
    #             #         break


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()
    
    

# def reduce_fn(agg: dict, messages: list):
#     print("reducing")
#     aggregated_messages = []
#     summary_dict = initialize_summary_dict()
#     start_time = None
#     # print("messages", messages)
#     sorted_trades = sorted(messages, key=lambda x: datetime.strptime(x['time'], "%Y-%m-%dT%H:%M:%S.%fZ"))
#     for msg in sorted_trades:
#         if start_time is None:
#             start_time = datetime.strptime(msg["time"], "%Y-%m-%dT%H:%M:%S.%fZ") 
#             one_second_later = start_time + timedelta(seconds=1)

#         currency = msg["product_id"]
#         # need to make sure to process all product ids and their timestamps...
#         msg_time = datetime.strptime(msg["time"], "%Y-%m-%dT%H:%M:%S.%fZ")    
#         if msg_time <= one_second_later:
#             if msg["side"] == "sell":
#                 summary_dict["avg_sale_price"] += float(msg["price"])
#                 summary_dict["sale_count"] += 1
#                 summary_dict["total_sale_size"] += float(msg["size"])
#             elif msg["side"] == "buy":
#                 summary_dict["avg_buy_price"] += float(msg["price"])
#                 summary_dict["buy_count"] += 1
#                 summary_dict["total_buy_size"] += float(msg["size"])
#         else:
#             if summary_dict["sale_count"] > 0:
#                 summary_dict["avg_sale_price"] /= summary_dict["sale_count"]
#             if summary_dict["buy_count"] > 0:
#                 summary_dict["avg_buy_price"] /= summary_dict["buy_count"]
#             summary_dict["start_time"] = start_time.isoformat()
#             summary_dict["end_time"] = msg_time.isoformat()
#             summary_dict["product_id"] = currency
#             aggregated_messages.append(summary_dict)
#             start_time = one_second_later
#             one_second_later += timedelta(seconds=1)
#             summary_dict = initialize_summary_dict()
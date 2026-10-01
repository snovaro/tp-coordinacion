import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal import InternalMessageType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_request = {}
        self.eof_received_by_request = {}

    def _handle_sigterm(self, signum, frame):
        logging.info("Aggregation %s received SIGTERM; stopping consumer", ID)
        try:
            self.input_exchange.stop_consuming()
        except Exception as exc:
            logging.debug("Aggregation consumer already stopped: %s", exc)

    def _process_data(self, request_id, fruit, amount, sum_id):
        logging.info("Processing data message")
        fruit_top = self.fruit_top_by_request.setdefault(request_id, [])
        logging.info(f"Adding new value of {fruit} with amount {amount} to request {request_id} from sum id {sum_id}")
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated = fruit_top.pop(i) + fruit_item.FruitItem(
                    fruit, amount
                )
                bisect.insort(fruit_top, updated)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, request_id, sum_id):
        logging.info(f"Received EOF for request {request_id} from sum {sum_id}")
        eof_received = self.eof_received_by_request.get(request_id, 0)
        self.eof_received_by_request[request_id] = eof_received + 1
        if self.eof_received_by_request[request_id] == SUM_AMOUNT:
            logging.info(f"Processing EOF for request {request_id} from aggregation {ID}")
            fruit_chunk = list(self.fruit_top_by_request.pop(request_id, [])[-TOP_SIZE:])
            fruit_chunk.reverse()
            fruit_top = [
                request_id,
                *[
                    (item.fruit, item.amount)
                    for item in fruit_chunk
                ],
            ]
            logging.info(f"Final top fruits for request {request_id} in aggregation {ID}: {fruit_top}")
            self.output_queue.send(message_protocol.internal.serialize(fruit_top))

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == InternalMessageType.DATA:
            self._process_data(*fields[1:])
        elif fields[0] == InternalMessageType.EOF:
            self._process_eof(*fields[1:])
        ack()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            for exchange in (self.input_exchange, self.output_queue):
                try:
                    exchange.close()
                except Exception as exc:
                    logging.warning("Could not close Aggregation exchange connection: %s", exc)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()

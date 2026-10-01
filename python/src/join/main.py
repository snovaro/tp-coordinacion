import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.top_by_request = {}

    def _handle_sigterm(self, signum, frame):
        logging.info("Join received SIGTERM; stopping consumer")
        try:
            self.input_queue.stop_consuming()
        except Exception as exc:
            logging.debug("Join consumer already stopped: %s", exc)

    def _merge_tops(self, current, new_top):
        merged = []
        i = j = 0

        while i < len(current) and j < len(new_top):
            current_fruit_item = fruit_item.FruitItem(
                current[i][0], int(current[i][1])
            )
            new_fruit_item = fruit_item.FruitItem(
                new_top[j][0], int(new_top[j][1])
            )
            if current_fruit_item >= new_fruit_item:
                merged.append(current[i])
                i += 1
            else:
                merged.append(new_top[j])
                j += 1

        merged.extend(current[i:])
        merged.extend(new_top[j:])
        return merged[:TOP_SIZE]

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fields = message_protocol.internal.deserialize(message)
        request_id = fields[0]
        fruits = fields[1:]
        current_top = self.top_by_request.setdefault(request_id, [[], 0])[0]
        self.top_by_request[request_id] = [
            self._merge_tops(current_top, fruits[:TOP_SIZE]),
            self.top_by_request[request_id][1] + 1
            ]

        if self.top_by_request[request_id][1] == AGGREGATION_AMOUNT:
            self.output_queue.send(message_protocol.internal.serialize([
                request_id,
                self.top_by_request[request_id][0]
            ]))
            self.top_by_request.pop(request_id)
        ack()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            for middleware_object in (self.input_queue, self.output_queue):
                try:
                    middleware_object.close()
                except Exception as exc:
                    logging.warning("Could not close Join middleware connection: %s", exc)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()

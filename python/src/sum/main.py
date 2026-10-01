import os
import logging
import threading
import hashlib

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal import InternalMessageType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.amount_by_request = {}
        self.sum_control_main_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}"]
        )
        # Asumo que el SUM_AMOUNT implica que los sum_id van de 0 a n = SUM_AMOUNT - 1
        self.count_by_sum_id = {} # request_id -> [count_sum_0, count_sum_1, ..., count_sum_n]
        self.total_count_by_request = {}
        self.state_lock = threading.Lock()
        self.flushed_requests = set()
        self.listener_ready = threading.Event()
        self.listener_startup_error = None

    def _process_data(self, request_id, fruit, amount):
        logging.info(f"Process data")
        should_send = False
        message_count = 0
        with self.state_lock:
            request_data = self.amount_by_request.setdefault(request_id, [0, {}]) 
            request_data[0] += 1
            message_count = request_data[0]
            request_data[1][fruit] = request_data[1].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            logging.info(f"Updated amount in sum id {ID} for request {request_id}: {request_data[1][fruit].amount}")
            if request_id in self.total_count_by_request:
                should_send = True

        if should_send:
            self.sum_control_main_exchange.send(
                message_protocol.internal.serialize(
                    [InternalMessageType.COUNT, 
                     ID, 
                     request_id, 
                     message_count
                     ])
            )


    def _process_eof_gateway(self, request_id, message_count):
        logging.info(f"Process EOF for request {request_id}")
        self.sum_control_main_exchange.send(
            message_protocol.internal.serialize([
                InternalMessageType.EOF_RECEIVED, 
                request_id, 
                message_count
                ])
        )

    def _process_eof(self, request_id):
        logging.info(f"Process EOF for request {request_id}")
        fruits = {}
        with self.state_lock:
            fruits = self.amount_by_request.pop(request_id, [0, {}])[1]
        logging.info(f"Finalizing request {request_id} with fruits: {fruits} in sum id {ID}")
        for final_fruit_item in fruits.values():
            index = self._aggregation_for(request_id, final_fruit_item.fruit)
            self.data_output_exchanges[index].send(
                message_protocol.internal.serialize(
                    [InternalMessageType.DATA, 
                        request_id, 
                        final_fruit_item.fruit, 
                        final_fruit_item.amount, 
                        ID
                        ]
                    )
                )

        logging.info(f"Broadcasting EOF for request {request_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([
                InternalMessageType.EOF, 
                request_id, 
                ID
                ]))

    def _aggregation_for(self, request_id, fruit):
        key = f"{request_id}:{fruit}".encode("utf-8")
        digest = hashlib.sha256(key).digest()
        return int.from_bytes(digest, "big") % AGGREGATION_AMOUNT


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == InternalMessageType.DATA:
            self._process_data(*fields[1:])
        elif fields[0] == InternalMessageType.EOF:
            self._process_eof_gateway(*fields[1:])
        ack()

    def _receive_eof(self, request_id, message_count):
        with self.state_lock:
            self.total_count_by_request[request_id] = message_count
            count = self.amount_by_request.get(request_id, [0, {}])[0]
        self.sum_control_listener_exchange.send(
            message_protocol.internal.serialize([
                InternalMessageType.COUNT, 
                ID, 
                request_id, 
                count
                ])
        )

    def _receive_count(self, sum_id, request_id, count):
        logging.info(f"Received count from sum {sum_id} for request {request_id}")
        should_send_eof = False
        with self.state_lock:
            if request_id in self.flushed_requests:
                return
            current_counts = self.count_by_sum_id.setdefault(request_id, [0] * SUM_AMOUNT)
            if count > current_counts[sum_id]:
                self.count_by_sum_id[request_id][sum_id] = count

            if request_id not in self.total_count_by_request:
                return
            
            if sum(self.count_by_sum_id[request_id]) == self.total_count_by_request[request_id]:
                self.count_by_sum_id.pop(request_id, None)
                self.total_count_by_request.pop(request_id, None)
                self.flushed_requests.add(request_id)
                should_send_eof = True

        if should_send_eof:
            self._process_eof(request_id)

    def process_sum_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == InternalMessageType.EOF_RECEIVED:
            self._receive_eof(*fields[1:])
        elif fields[0] == InternalMessageType.COUNT:
            self._receive_count(*fields[1:])
        ack()

    def _listen_other_sums(self):
        try:
            self.data_output_exchanges = []
            for i in range(AGGREGATION_AMOUNT):
                data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
                )
                self.data_output_exchanges.append(data_output_exchange)
            
            self.sum_control_listener_exchange= middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}"]
            )
            self.listener_ready.set()

            self.sum_control_listener_exchange.start_consuming(
                self.process_sum_control_message
            )

        except Exception as exc:
            self.listener_startup_error = exc
            self.listener_ready.set()
            raise

    def start(self):
        listener = threading.Thread(
            target=self._listen_other_sums, daemon=True
        )
        listener.start()
        if not self.listener_ready.wait(timeout=10):
            raise RuntimeError("El listener de control no inició a tiempo")
        
        if self.listener_startup_error:
            raise RuntimeError("Falló el inicio del listener de control") from self.listener_startup_error
        
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

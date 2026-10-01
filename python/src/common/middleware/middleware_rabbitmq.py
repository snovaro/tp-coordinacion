import pika
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while initializing the RabbitMQ queue: {e}")

    def start_consuming(self, on_message_callback):
        try:
            self.on_message_callback = on_message_callback
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while starting to consume messages: {e}")

    def _callback(self, ch, method, properties, body):
        def _nack():
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag)
            except pika.exceptions.AMQPConnectionError as e:
                raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
            except Exception as e:
                raise MessageMiddlewareMessageError(f"An error occurred while starting to consume messages: {e}")

        def _ack():
            try:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except pika.exceptions.AMQPConnectionError as e:
                raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
            except Exception as e:
                raise MessageMiddlewareMessageError(f"An error occurred while starting to consume messages: {e}")
        self.delivery_tag = method.delivery_tag
        self.on_message_callback(body, _ack, _nack)


    def stop_consuming(self):
        try:
            if self.connection.is_open and self.channel.is_open:
                self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while stopping the consumption of messages: {e}")

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while sending a message: {e}")

    def close(self):
        try:
            self.channel.close()
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"An error occurred while closing the RabbitMQ connection: {e}")

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)
            result = self.channel.queue_declare(queue='', exclusive=True, auto_delete=True)
            self.queue_name = result.method.queue
            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=routing_key)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while initializing the RabbitMQ exchange: {e}")

    def send(self, message):
        routing_key = self.routing_keys[0]
        try:
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while sending a message: {e}")
    
    def start_consuming(self, on_message_callback):
        try:
            self.on_message_callback = on_message_callback
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while starting to consume messages: {e}")

    def _callback(self, ch, method, properties, body):
        def _nack():
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag)
            except pika.exceptions.AMQPConnectionError as e:
                raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
            except Exception as e:
                raise MessageMiddlewareMessageError(f"An error occurred while starting to consume messages: {e}")

        def _ack():
            try:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except pika.exceptions.AMQPConnectionError as e:
                raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
            except Exception as e:
                raise MessageMiddlewareMessageError(f"An error occurred while starting to consume messages: {e}")
        self.delivery_tag = method.delivery_tag
        self.on_message_callback(body, _ack, _nack)

    def stop_consuming(self):
        try:
            if self.connection.is_open and self.channel.is_open:
                self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self.host}: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while stopping the consumption of messages: {e}")

    def close(self):
        try:
            self.channel.close()
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"An error occurred while closing the RabbitMQ connection: {e}")

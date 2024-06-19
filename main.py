from time import sleep
from pika import ConnectionParameters, BlockingConnection, PlainCredentials
from pika.spec import Basic


class RabbitMQ:
    """A Python class for interacting with RabbitMQ, a message broker."""

    def __init__(self, host: str, port: int, username: str, password: str, heartbeat: int = None) -> None:
        self.__connection = self.__connecting(ConnectionParameters(host=host, port=port, credentials=PlainCredentials(username=username, password=password), heartbeat=heartbeat))

    def __connecting(self, parameters: ConnectionParameters) -> BlockingConnection:
        """Connect to RabbitMQ server."""
        counter = 0
        while True:
            counter += 1
            try:
                connection = BlockingConnection(parameters=parameters)
                self.__channel = connection.channel()
                return connection
            except Exception as e:
                print(e)
                if counter >= 5:
                    print(f'RabbitMQ connection failed counter: {counter}')
                    raise e
                sleep(30)

    def declare_queue(self, names: list) -> None:
        """Declare a queue within the channel context."""
        for _ in names:
            self.__channel.queue_declare(queue=_)

    def listen(self, prefetch_count: int, queue: str, callback) -> None:
        """Begin consuming messages from the designated queue."""
        self.__channel.basic_qos(prefetch_count=prefetch_count)
        self.__channel.basic_consume(queue=queue, on_message_callback=callback)
        self.__channel.start_consuming()

    def accept(self, method: Basic.Deliver) -> None:
        """Accept the message and remove it from the queue."""
        self.__channel.basic_ack(delivery_tag=method.delivery_tag)

    def send(self, queue: str, body: bytes) -> None:
        """Send a message to the designated queue."""
        self.__channel.basic_publish(exchange='', routing_key=queue, body=body)

    def reject(self, method: Basic.Deliver, body: bytes) -> None:
        """Accept the message and requeue it to the end of the same queue."""
        self.send(queue=method.routing_key, body=body)
        self.accept(method=method)

    def close(self) -> None:
        """Close the RabbitMQ server connection."""
        self.__connection.close()

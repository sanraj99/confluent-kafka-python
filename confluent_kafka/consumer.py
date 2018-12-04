from .cimpl import Consumer as _impl


class Consumer(_impl):

    __slots__ = ["key_deserializer", "value_deserializer"]

    def __init__(self, conf, key_deserializer=None, value_deserializer=None,
                 on_commit=None, stats_cb=None, throttle_cb=None, logger=None):
        """
            Create a new Kafka Consumer instance.

            To avoid spontaneous calls from non-Python threads all callbacks will only be served upon
            calling ```client.poll()``` or ```client.flush()``` are called.

            :param dict conf: Configuration properties.
                See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for more information.
            :param func key_deserializer(topic, key): Converts message key bytes to object.
                **note** deserializers are responsible for handling NULL keys
            :param func value_deserializer(topic, value): Converts message value bytes to object.
                **note** deserializers are responsible for handling NULL values
            :param func on_commit(err, [partitions]): Callback used to indicate success or failure
                of an offset commit.
            :param func stats_cb(json_str): Callback for statistics emitted every ``statistics.interval.ms``.
                See https://github.com/edenhill/librdkafka/wiki/Statistics” for more information.
            :param func throttle_cb(confluent_kafka.ThrottleEvent): Callback for throttled request reporting.
            :param logging.handlers logger: Forwards logs from the Kafka client to the provided handler instance.
                Log messages will only be forwarded when ``client.poll()`` or ``producer.flush()`` are called.
        """
        self.key_deserializer = key_deserializer
        self.value_deserializer = value_deserializer

        super(Consumer, self).__init__(conf, on_commit=on_commit)

    def poll(self, timeout=-1.0, key_deserializer=None, value_deserializer=None):
        """
            Consumes a message, triggers callbacks, returns an event.

            The application must check the returned Message object’s Message.error() method to distinguish
            between proper messages (error() returns None), or an event or error (see error().code() for specifics).

            :param float timeout:  Maximum time in seconds to block waiting for message, event or callback.
            :param func key_deserializer(topic, key): Converts message key bytes to object.
                **note** deserializers are responsible for handling NULL keys
            :param func value_deserializer(topic, value): Converts message value bytes to object.
                **note** deserializers are responsible for handling NULL values
            :returns: A confluent_kafka.Message or None on timeout.
            :raises RuntimeError: If called on a closed consumer.
        """

        msg = super(Consumer, self).poll(timeout)

        if not msg or msg.error():
            return msg

        topic = msg.topic()

        # parameter overrides take precedence over instance functions
        if not key_deserializer:
            key_deserializer = self.key_deserializer

        if key_deserializer:
            msg.set_key(key_deserializer(topic, msg.key()))

        if not value_deserializer:
            value_deserializer = self.value_deserializer

        if value_deserializer:
            msg.set_value(value_deserializer(topic, msg.value()))

        return msg

    def consume(self, num_messages=1, timeout=-1):
        """
            Consume messages, calls callbacks and returns a list of messages. (possibly empty on timeout)

            The application must check Message.error() to distinguish between
                proper messages (error() returns None), an event, or an error for each
                Message in the list.

            :param int num_messages: Maximum number of messages to return (default: 1)
            :param float timeout: Maximum time in seconds to block waiting for message, event or callback.
                (default: infinite (-1))
            :returns: A list of Message objects (possibly empty on timeout)
            :rtype: list(Message)
            :raises RuntimeError: if called on a closed consumer.
            :raises KafkaError: in case of internal error.
            :raises ValueError: if num_messages > 1M.
        """

        # Disable consume() method when serializers are in use.
        if self.key_deserializer or self.value_deserializer:
            raise(NotImplementedError, "Batch consumption does not support the use of deserializers")

        return super(Consumer, self).consume(num_messages, timeout)

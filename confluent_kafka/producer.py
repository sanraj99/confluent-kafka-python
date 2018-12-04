from .cimpl import Producer as _impl, PARTITION_UA


class Producer(_impl):

    __slots__ = ["key_serializer", "value_serializer"]

    def __init__(self, conf, key_serializer=None, value_serializer=None,
                 error_cb=None, stats_cb=None, throttle_cb=None, logger=None):
        """
            Create a new Kafka Producer instance.

            To avoid spontaneous calls from non-Python threads all callbacks will only be served upon
                calling ```client.poll()``` or ```client.flush()``` are called.

            :param dict conf: Configuration properties. At a minimum ``bootstrap.servers`` **should** be set.
                See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for more information.
            :param func key_serializer(topic, key): Converts key to bytes.
                **note** serializers are responsible for handling NULL keys
            :param func value_serializer(topic, value): Converts value to bytes.
                **note** serializers are responsible for handling NULL keys
            :param func error_cb(kafka.KafkaError): Callback for generic/global error events.
            :param func stats_cb(json_str): Callback for statistics emitted every ``statistics.interval.ms``.
                See https://github.com/edenhill/librdkafka/wiki/Statistics” for more information.
            :param func throttle_cb(confluent_kafka.ThrottleEvent): Callback for throttled request reporting.
            :param logging.handlers logger: Forwards logs from the Kafka client to the provided handler instance.
                Log messages will only be forwarded when ``client.poll()`` or ``producer.flush()`` are called.
        """

        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

        super(Producer, self).__init__(conf, logger=logger)

    def produce(self, topic, value=None, key=None, partition=PARTITION_UA,
                on_delivery=None, callback=None, timestamp=0, headers=None,
                key_serializer=None, value_serializer=None):
        """
            Produces message to Kafka

            :param str topic: Topic to produce message to.
            :param str|bytes|obj value: Message payload; value_serializer required for non-character values.
            :param str|bytes|obj key: Message key payload; key_serializer required for non-character keys.
            :param int partition: Partition to produce to, else uses the configured built-in partitioner.
                Default value is PARTITION_UA(round-robin over all partitions).
            :param func on_delivery(confluent_kafka.KafkaError, confluent_kafka.Message):
                Delivery report callback to call on successful or failed delivery.
            :param func callback(confluent_kafka.KafkaError, confluent_kafka.Message):
                See on_delivery.
            :param int timestamp: Message timestamp (CreateTime) in milliseconds since epoch UTC
                (requires librdkafka >= v0.9.4, api.version.request=true, and broker >= 0.10.0.0).
                Default value is current time.
            :param dict|list headers: Message headers to set on the message. The header key must be a string while
                the value must be binary, unicode or None. Accepts a list of (key,value) or a dict.
                (Requires librdkafka >= v0.11.4 and broker version >= 0.11.0.0)
            :param func key_serializer(topic, key): Producer key_serializer override;
                Converts message key to bytes. **note** serializers are responsible for handling NULL keys
            :param func value_serializer(topic, value): Producer value_serializer override;
                Converts message value to bytes. **note** serializers are responsible for handling NULL values

        """

        # on_delivery is an alias for callback and take precedence
        if callback and not on_delivery:
            on_delivery = callback

        # parameter overrides take precedence over instance functions
        if not key_serializer:
            key_serializer = self.key_serializer

        if key_serializer:
            key = key_serializer(topic, key)

        if not value_serializer:
            value_serializer = self.value_serializer

        if value_serializer:
            value = value_serializer(topic, value)

        super(Producer, self).produce(topic, value, key, partition, on_delivery=on_delivery,
                                      timestamp=timestamp, headers=headers)

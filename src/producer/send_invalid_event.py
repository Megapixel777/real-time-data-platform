import json

from kafka import KafkaProducer

TOPIC_NAME = "ecommerce-events"


def main() -> None:

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    invalid_event = {
        "event_id": "invalid-test-001",
        "event_type": "order_item_added",
        "order_id": "ORD-INVALID",
        "customer_id": "CUST-TEST",
        "product_id": "PROD-TEST",
        "quantity": -5,
        "unit_price": 100.00,
        "event_timestamp": "2026-08-30T12:00:00+00:00",
    }

    producer.send(
        TOPIC_NAME,
        value=invalid_event,
    )

    producer.flush()
    producer.close()

    print("Invalid event sent successfully")


if __name__ == "__main__":
    main()
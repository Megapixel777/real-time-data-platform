import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


TOPIC_NAME = "ecommerce-events"


def get_timestamp() -> str:
    """Return the current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def create_base_event(
    event_type: str,
    order_id: str,
    customer_id: str,
) -> dict:
    """Create the base structure for an e-commerce event."""

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": None,
        "quantity": None,
        "unit_price": None,
        "event_timestamp": get_timestamp(),
    }


def create_order_events() -> list[dict]:
    """Generate all events associated with one e-commerce order."""

    order_id = f"ORD-{random.randint(1000, 9999)}"
    customer_id = f"CUST-{random.randint(1, 1000)}"

    events = []

    # Order created
    events.append(
        create_base_event(
            "order_created",
            order_id,
            customer_id,
        )
    )

    # Between 1 and 4 products per order
    number_of_items = random.randint(1, 4)

    for _ in range(number_of_items):

        product_id = f"PROD-{random.randint(1, 500)}"
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 500), 2)

        item_event = create_base_event(
            "order_item_added",
            order_id,
            customer_id,
        )

        item_event["product_id"] = product_id
        item_event["quantity"] = quantity
        item_event["unit_price"] = unit_price

        events.append(item_event)

    # Order lifecycle events
    for event_type in [
        "payment_completed",
        "order_shipped",
        "order_delivered",
    ]:

        events.append(
            create_base_event(
                event_type,
                order_id,
                customer_id,
            )
        )

    return events


def main() -> None:

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"Sending events to topic: {TOPIC_NAME}")

    try:

        while True:

            events = create_order_events()

            for event in events:

                producer.send(
                    TOPIC_NAME,
                    value=event,
                )

                print(
                    f"Sent: {event['event_type']} "
                    f"for {event['order_id']}"
                )

                time.sleep(0.5)

            # Ensure all events for this order are sent
            producer.flush()

            print("-" * 50)

            # Wait before generating the next order
            time.sleep(2)

    except KeyboardInterrupt:

        print("\nStopping producer...")

    finally:

        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
from datetime import datetime
from unittest.mock import Mock, patch

from src.producer import event_generator
from src.producer.event_generator import (
    create_base_event,
    create_order_events,
    get_timestamp,
)


def test_get_timestamp():

    timestamp = get_timestamp()

    assert timestamp is not None

    # Verificamos que puede convertirse a datetime
    datetime.fromisoformat(timestamp)


def test_create_base_event():

    event = create_base_event(
        "order_created",
        "ORD-100",
        "CUST-100",
    )

    assert event["event_type"] == "order_created"
    assert event["order_id"] == "ORD-100"
    assert event["customer_id"] == "CUST-100"

    assert event["event_id"] is not None
    assert event["event_timestamp"] is not None

    assert event["product_id"] is None
    assert event["quantity"] is None
    assert event["unit_price"] is None


def test_create_order_events():

    events = create_order_events()

    # Debe haber al menos:
    # 1 order_created
    # 1-4 order_item_added
    # 3 lifecycle events
    assert len(events) >= 5

    # Todos los eventos pertenecen al mismo pedido
    order_ids = {event["order_id"] for event in events}
    assert len(order_ids) == 1

    # Todos los eventos pertenecen al mismo cliente
    customer_ids = {event["customer_id"] for event in events}
    assert len(customer_ids) == 1

    # Debe existir el evento order_created
    event_types = [event["event_type"] for event in events]

    assert "order_created" in event_types
    assert "payment_completed" in event_types
    assert "order_shipped" in event_types
    assert "order_delivered" in event_types

    # Debe haber entre 1 y 4 productos
    item_events = [
        event
        for event in events
        if event["event_type"] == "order_item_added"
    ]

    assert 1 <= len(item_events) <= 4

    # Los productos deben tener información válida
    for event in item_events:

        assert event["product_id"] is not None

        assert event["quantity"] is not None
        assert event["quantity"] > 0

        assert event["unit_price"] is not None
        assert event["unit_price"] > 0


@patch("src.producer.event_generator.time.sleep")
@patch("src.producer.event_generator.KafkaProducer")
@patch("src.producer.event_generator.create_order_events")
def test_main(
    mock_create_order_events,
    mock_kafka_producer,
    mock_sleep,
):

    mock_producer = Mock()
    mock_kafka_producer.return_value = mock_producer

    mock_create_order_events.return_value = [
        {
            "event_id": "event-1",
            "event_type": "order_created",
            "order_id": "ORD-100",
            "customer_id": "CUST-100",
            "product_id": None,
            "quantity": None,
            "unit_price": None,
            "event_timestamp": "2026-01-01T00:00:00+00:00",
        }
    ]

    # Interrumpimos el bucle infinito en el primer sleep
    mock_sleep.side_effect = KeyboardInterrupt

    event_generator.main()

    # Se crea el productor Kafka
    mock_kafka_producer.assert_called_once()

    # Se genera un pedido
    mock_create_order_events.assert_called_once()

    # Se envía el evento
    mock_producer.send.assert_called_once()

    # Al terminar se hace flush y close
    mock_producer.flush.assert_called()
    mock_producer.close.assert_called_once()
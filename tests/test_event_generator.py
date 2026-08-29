from src.producer.event_generator import create_order_events


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
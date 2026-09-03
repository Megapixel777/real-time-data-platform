from unittest.mock import Mock, patch

from src.producer import send_invalid_event


@patch("src.producer.send_invalid_event.KafkaProducer")
def test_send_invalid_event(mock_kafka_producer):

    mock_producer = Mock()
    mock_kafka_producer.return_value = mock_producer

    send_invalid_event.main()

    mock_kafka_producer.assert_called_once()

    mock_producer.send.assert_called_once()

    args, kwargs = mock_producer.send.call_args

    assert args[0] == "ecommerce-events"

    event = kwargs["value"]

    assert event["event_id"] == "invalid-test-001"
    assert event["event_type"] == "order_item_added"
    assert event["order_id"] == "ORD-INVALID"
    assert event["quantity"] == -5

    mock_producer.flush.assert_called_once()
    mock_producer.close.assert_called_once()

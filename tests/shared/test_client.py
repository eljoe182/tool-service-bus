from shared.client import default_client_factory


def test_default_client_factory_delegates_connection_string(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "shared.client.ServiceBusClient.from_connection_string",
        lambda connection_string: calls.append(connection_string) or object(),
    )

    assert default_client_factory("safe-connection") is not None
    assert calls == ["safe-connection"]

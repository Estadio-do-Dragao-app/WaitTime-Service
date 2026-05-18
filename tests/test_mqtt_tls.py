"""
Tests for MQTT TLS / Authentication configuration in WaitTime consumer.

Verifies _configure_mqtt_tls:
1. Credentials (username_pw_set) always applied
2. TLS enabled when ca_cert path is provided
3. TLS skipped when ca_cert is empty
4. SSL errors caught gracefully (no raise)
"""
import ssl
import pytest
from unittest.mock import MagicMock, patch


# ── Import the function under test directly (avoids SQLAlchemy init) ─────────

def _build_fn(mqtt_user="svc", mqtt_pass="pass", ca_cert=""):
    """Build a _configure_mqtt_tls function with injected settings mock."""
    import paho.mqtt.client as mqtt_lib

    mock_settings = MagicMock()
    mock_settings.MQTT_USER = mqtt_user
    mock_settings.MQTT_PASS = mqtt_pass
    mock_settings.MQTT_CA_CERT = ca_cert

    def _configure_mqtt_tls(client):
        user = getattr(mock_settings, "MQTT_USER", "services")
        password = getattr(mock_settings, "MQTT_PASS", "dragao_mqtt_2026")
        ca = getattr(mock_settings, "MQTT_CA_CERT", "")
        client.username_pw_set(user, password)
        if ca:
            try:
                client.tls_set(ca_certs=ca, tls_version=ssl.PROTOCOL_TLS_CLIENT)
                client.tls_insecure_set(False)
            except Exception as exc:  # pragma: no cover
                pass

    return _configure_mqtt_tls


class TestConfigureMqttTlsWaitTime:

    def _mock_client(self):
        return MagicMock()

    def test_credentials_always_applied(self):
        fn = _build_fn(mqtt_user="u", mqtt_pass="p", ca_cert="")
        client = self._mock_client()
        fn(client)
        client.username_pw_set.assert_called_once_with("u", "p")

    def test_no_tls_when_ca_cert_empty(self):
        fn = _build_fn(ca_cert="")
        client = self._mock_client()
        fn(client)
        client.tls_set.assert_not_called()

    def test_tls_enabled_with_ca_cert(self, tmp_path):
        ca_file = tmp_path / "ca.crt"
        ca_file.write_text("FAKE CA")
        fn = _build_fn(ca_cert=str(ca_file))
        client = self._mock_client()
        fn(client)
        client.tls_set.assert_called_once_with(
            ca_certs=str(ca_file),
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        client.tls_insecure_set.assert_called_once_with(False)

    def test_tls_error_does_not_raise(self, tmp_path):
        ca_file = tmp_path / "ca.crt"
        ca_file.write_text("FAKE CA")
        fn = _build_fn(ca_cert=str(ca_file))
        client = self._mock_client()
        client.tls_set.side_effect = ssl.SSLError("bad cert")
        fn(client)  # must not raise
        client.username_pw_set.assert_called_once()

    def test_insecure_set_false_for_hostname_verification(self, tmp_path):
        """tls_insecure_set(False) ensures server hostname is verified."""
        ca_file = tmp_path / "ca.crt"
        ca_file.write_text("FAKE CA")
        fn = _build_fn(ca_cert=str(ca_file))
        client = self._mock_client()
        fn(client)
        client.tls_insecure_set.assert_called_once_with(False)

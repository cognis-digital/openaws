"""Tests for KMSService — CMKs, encrypt/decrypt, GenerateDataKey, aliases."""
import base64
import pytest

from openaws.errors import Conflict, NotFound, ValidationError


def test_create_and_describe_key(app):
    key = app.kms.create_key(description="test key")
    assert key["state"] == "Enabled"
    desc = app.kms.describe_key(key["key_id"])
    assert desc["description"] == "test key"
    assert "key_material_b64" not in desc


def test_list_keys(app):
    k1 = app.kms.create_key()
    k2 = app.kms.create_key()
    keys = app.kms.list_keys()
    ids = [k["key_id"] for k in keys]
    assert k1["key_id"] in ids and k2["key_id"] in ids


def test_encrypt_decrypt_roundtrip(app):
    key = app.kms.create_key()
    plaintext = b"hello secret world"
    enc = app.kms.encrypt(key["key_id"], plaintext)
    assert "ciphertext_blob" in enc
    dec = app.kms.decrypt(enc["ciphertext_blob"])
    recovered = base64.b64decode(dec["plaintext"])
    assert recovered == plaintext


def test_encrypt_with_context(app):
    key = app.kms.create_key()
    ctx = {"service": "s3", "region": "local"}
    enc = app.kms.encrypt(key["key_id"], b"data", encryption_context=ctx)
    dec = app.kms.decrypt(enc["ciphertext_blob"], encryption_context=ctx)
    assert base64.b64decode(dec["plaintext"]) == b"data"


def test_decrypt_wrong_context_fails(app):
    key = app.kms.create_key()
    enc = app.kms.encrypt(key["key_id"], b"data", encryption_context={"k": "v"})
    with pytest.raises(Exception):
        app.kms.decrypt(enc["ciphertext_blob"], encryption_context={"k": "other"})


def test_generate_data_key(app):
    key = app.kms.create_key()
    result = app.kms.generate_data_key(key["key_id"])
    assert "plaintext" in result
    assert "ciphertext_blob" in result
    plaintext_bytes = base64.b64decode(result["plaintext"])
    assert len(plaintext_bytes) == 32  # AES_256


def test_generate_data_key_without_plaintext(app):
    key = app.kms.create_key()
    result = app.kms.generate_data_key_without_plaintext(key["key_id"])
    assert "ciphertext_blob" in result
    assert "plaintext" not in result


def test_generate_data_key_aes128(app):
    key = app.kms.create_key()
    result = app.kms.generate_data_key(key["key_id"], key_spec="AES_128")
    plaintext_bytes = base64.b64decode(result["plaintext"])
    assert len(plaintext_bytes) == 16


def test_disable_enable_key(app):
    key = app.kms.create_key()
    app.kms.disable_key(key["key_id"])
    desc = app.kms.describe_key(key["key_id"])
    assert desc["state"] == "Disabled"
    with pytest.raises(ValidationError):
        app.kms.encrypt(key["key_id"], b"data")
    app.kms.enable_key(key["key_id"])
    desc2 = app.kms.describe_key(key["key_id"])
    assert desc2["state"] == "Enabled"


def test_schedule_key_deletion(app):
    key = app.kms.create_key()
    result = app.kms.schedule_key_deletion(key["key_id"], pending_window_in_days=7)
    assert result["state"] == "PendingDeletion"
    desc = app.kms.describe_key(key["key_id"])
    assert desc["state"] == "PendingDeletion"


def test_key_aliases(app):
    key = app.kms.create_key()
    app.kms.create_alias("alias/my-key", key["key_id"])
    aliases = app.kms.list_aliases()
    assert any(a["alias_name"] == "alias/my-key" for a in aliases)
    app.kms.delete_alias("alias/my-key")
    aliases_after = app.kms.list_aliases()
    assert not any(a["alias_name"] == "alias/my-key" for a in aliases_after)


def test_alias_must_start_with_alias(app):
    key = app.kms.create_key()
    with pytest.raises(ValidationError):
        app.kms.create_alias("my-key", key["key_id"])


def test_duplicate_alias_raises(app):
    key = app.kms.create_key()
    app.kms.create_alias("alias/dup", key["key_id"])
    with pytest.raises(Conflict):
        app.kms.create_alias("alias/dup", key["key_id"])


def test_key_rotation(app):
    key = app.kms.create_key()
    status = app.kms.get_key_rotation_status(key["key_id"])
    assert status["key_rotation_enabled"] is False
    app.kms.enable_key_rotation(key["key_id"])
    assert app.kms.get_key_rotation_status(key["key_id"])["key_rotation_enabled"] is True
    app.kms.disable_key_rotation(key["key_id"])
    assert app.kms.get_key_rotation_status(key["key_id"])["key_rotation_enabled"] is False


def test_describe_nonexistent_key(app):
    with pytest.raises(NotFound):
        app.kms.describe_key("nonexistent-key-id")


def test_kms_http_roundtrip(server):
    import urllib.request, json
    base = server.base_url + "/kms"

    def call(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(base, data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    key = call({"action": "create_key", "description": "http-test"})
    key_id = key["key_id"]

    enc = call({"action": "encrypt", "key_id": key_id, "plaintext": "hello"})
    dec = call({"action": "decrypt", "ciphertext_blob": enc["ciphertext_blob"]})
    recovered = base64.b64decode(dec["plaintext"]).decode()
    assert recovered == "hello"

    keys = call({"action": "list_keys"})
    assert any(k["key_id"] == key_id for k in keys["keys"])

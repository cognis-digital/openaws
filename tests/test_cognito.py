"""Tests for CognitoService — user pools, sign-up, sign-in, JWT tokens."""
import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ---------------------------------------------------------------------------
# User pools
# ---------------------------------------------------------------------------

def test_create_describe_delete_user_pool(app):
    pool = app.cognito.create_user_pool("my-app-pool")
    pool_id = pool["pool_id"]
    assert pool["pool_name"] == "my-app-pool"
    desc = app.cognito.describe_user_pool(pool_id)
    assert desc["pool_name"] == "my-app-pool"
    app.cognito.delete_user_pool(pool_id)
    with pytest.raises(NotFound):
        app.cognito.describe_user_pool(pool_id)


def test_list_user_pools(app):
    app.cognito.create_user_pool("pool-a")
    app.cognito.create_user_pool("pool-b")
    pools = app.cognito.list_user_pools()
    names = [p["pool_name"] for p in pools]
    assert "pool-a" in names and "pool-b" in names


# ---------------------------------------------------------------------------
# Pool clients
# ---------------------------------------------------------------------------

def test_create_describe_list_delete_client(app):
    pool = app.cognito.create_user_pool("p1")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "web-app")
    cid = client["client_id"]
    desc = app.cognito.describe_user_pool_client(pid, cid)
    assert desc["client_name"] == "web-app"
    clients = app.cognito.list_user_pool_clients(pid)
    assert any(c["client_id"] == cid for c in clients)
    app.cognito.delete_user_pool_client(pid, cid)
    with pytest.raises(NotFound):
        app.cognito.describe_user_pool_client(pid, cid)


def test_client_with_secret(app):
    pool = app.cognito.create_user_pool("p-secret")
    client = app.cognito.create_user_pool_client(pool["pool_id"], "native", generate_secret=True)
    assert client["client_secret"] is not None
    assert len(client["client_secret"]) >= 16


# ---------------------------------------------------------------------------
# Sign-up and confirm
# ---------------------------------------------------------------------------

def test_sign_up_confirm_sign_in(app):
    pool = app.cognito.create_user_pool("auth-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]

    signup = app.cognito.sign_up(pid, cid, "alice", "password123!")
    assert signup["user_confirmed"] is False
    code = signup["confirmation_code"]

    app.cognito.confirm_sign_up(pid, cid, "alice", code)

    result = app.cognito.initiate_auth(
        "USER_PASSWORD_AUTH",
        {"USERNAME": "alice", "PASSWORD": "password123!"},
        cid, pool_id=pid,
    )
    auth = result["authentication_result"]
    assert "access_token" in auth
    assert "id_token" in auth
    assert "refresh_token" in auth


def test_sign_up_duplicate_raises(app):
    pool = app.cognito.create_user_pool("dup-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    app.cognito.sign_up(pid, cid, "bob", "pass1!")
    with pytest.raises(Conflict):
        app.cognito.sign_up(pid, cid, "bob", "pass2!")


def test_sign_in_wrong_password_raises(app):
    pool = app.cognito.create_user_pool("pw-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    signup = app.cognito.sign_up(pid, cid, "carol", "rightpass!")
    app.cognito.confirm_sign_up(pid, cid, "carol", signup["confirmation_code"])
    with pytest.raises(ValidationError):
        app.cognito.initiate_auth(
            "USER_PASSWORD_AUTH",
            {"USERNAME": "carol", "PASSWORD": "wrongpass!"},
            cid, pool_id=pid,
        )


def test_sign_in_unconfirmed_raises(app):
    pool = app.cognito.create_user_pool("unc-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    app.cognito.sign_up(pid, cid, "dave", "pass!")
    with pytest.raises(ValidationError):
        app.cognito.initiate_auth(
            "USER_PASSWORD_AUTH",
            {"USERNAME": "dave", "PASSWORD": "pass!"},
            cid, pool_id=pid,
        )


def test_wrong_confirmation_code_raises(app):
    pool = app.cognito.create_user_pool("wp")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    app.cognito.sign_up(pid, cid, "eve", "pass!")
    with pytest.raises(ValidationError):
        app.cognito.confirm_sign_up(pid, cid, "eve", "000000")


# ---------------------------------------------------------------------------
# Admin operations
# ---------------------------------------------------------------------------

def test_admin_create_and_delete_user(app):
    pool = app.cognito.create_user_pool("admin-pool")
    pid = pool["pool_id"]
    result = app.cognito.admin_create_user(pid, "frank", "Temp@123")
    assert result["status"] == "FORCE_CHANGE_PASSWORD"
    user = app.cognito.admin_get_user(pid, "frank")
    assert user["username"] == "frank"
    app.cognito.admin_delete_user(pid, "frank")
    with pytest.raises(NotFound):
        app.cognito.admin_get_user(pid, "frank")


def test_admin_confirm_sign_up(app):
    pool = app.cognito.create_user_pool("ac-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    app.cognito.sign_up(pid, cid, "grace", "pass!")
    app.cognito.admin_confirm_sign_up(pid, "grace")
    # should now be able to sign in
    result = app.cognito.initiate_auth(
        "USER_PASSWORD_AUTH", {"USERNAME": "grace", "PASSWORD": "pass!"}, cid, pool_id=pid
    )
    assert "authentication_result" in result


def test_admin_set_user_password(app):
    pool = app.cognito.create_user_pool("sp-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    app.cognito.sign_up(pid, cid, "henry", "oldpass!")
    app.cognito.admin_confirm_sign_up(pid, "henry")
    app.cognito.admin_set_user_password(pid, "henry", "newpass!", permanent=True)
    result = app.cognito.initiate_auth(
        "USER_PASSWORD_AUTH", {"USERNAME": "henry", "PASSWORD": "newpass!"}, cid, pool_id=pid
    )
    assert "authentication_result" in result


# ---------------------------------------------------------------------------
# get_user + list_users
# ---------------------------------------------------------------------------

def test_get_user_from_token(app):
    pool = app.cognito.create_user_pool("gu-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    signup = app.cognito.sign_up(pid, cid, "ida", "pass!")
    app.cognito.confirm_sign_up(pid, cid, "ida", signup["confirmation_code"])
    result = app.cognito.initiate_auth(
        "USER_PASSWORD_AUTH", {"USERNAME": "ida", "PASSWORD": "pass!"}, cid, pool_id=pid
    )
    access_token = result["authentication_result"]["access_token"]
    user = app.cognito.get_user(access_token)
    assert user["username"] == "ida"


def test_list_users(app):
    pool = app.cognito.create_user_pool("lu-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    for name in ["jim", "kate", "liam"]:
        signup = app.cognito.sign_up(pid, cid, name, "pass!")
        app.cognito.confirm_sign_up(pid, cid, name, signup["confirmation_code"])
    users = app.cognito.list_users(pid)
    usernames = [u["username"] for u in users]
    assert "jim" in usernames and "kate" in usernames and "liam" in usernames


# ---------------------------------------------------------------------------
# Refresh token
# ---------------------------------------------------------------------------

def test_refresh_token(app):
    pool = app.cognito.create_user_pool("rt-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    signup = app.cognito.sign_up(pid, cid, "mike", "pass!")
    app.cognito.confirm_sign_up(pid, cid, "mike", signup["confirmation_code"])
    auth1 = app.cognito.initiate_auth(
        "USER_PASSWORD_AUTH", {"USERNAME": "mike", "PASSWORD": "pass!"}, cid, pool_id=pid
    )
    refresh = auth1["authentication_result"]["refresh_token"]
    auth2 = app.cognito.initiate_auth(
        "REFRESH_TOKEN_AUTH",
        {"REFRESH_TOKEN": refresh},
        cid, pool_id=pid,
    )
    assert "access_token" in auth2["authentication_result"]


# ---------------------------------------------------------------------------
# Global sign out
# ---------------------------------------------------------------------------

def test_global_sign_out(app):
    pool = app.cognito.create_user_pool("so-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    signup = app.cognito.sign_up(pid, cid, "nina", "pass!")
    app.cognito.confirm_sign_up(pid, cid, "nina", signup["confirmation_code"])
    auth = app.cognito.initiate_auth(
        "USER_PASSWORD_AUTH", {"USERNAME": "nina", "PASSWORD": "pass!"}, cid, pool_id=pid
    )
    access_token = auth["authentication_result"]["access_token"]
    result = app.cognito.global_sign_out(access_token)
    assert result["signed_out"] is True


# ---------------------------------------------------------------------------
# Forgot / reset password
# ---------------------------------------------------------------------------

def test_forgot_confirm_password(app):
    pool = app.cognito.create_user_pool("fp-pool")
    pid = pool["pool_id"]
    client = app.cognito.create_user_pool_client(pid, "app")
    cid = client["client_id"]
    signup = app.cognito.sign_up(pid, cid, "oscar", "oldpass!")
    app.cognito.confirm_sign_up(pid, cid, "oscar", signup["confirmation_code"])
    forgot = app.cognito.forgot_password(pid, cid, "oscar")
    code = forgot["reset_code"]
    app.cognito.confirm_forgot_password(pid, cid, "oscar", code, "newpass!")
    # should be able to sign in with new password
    result = app.cognito.initiate_auth(
        "USER_PASSWORD_AUTH", {"USERNAME": "oscar", "PASSWORD": "newpass!"}, cid, pool_id=pid
    )
    assert "authentication_result" in result


# ---------------------------------------------------------------------------
# HTTP round-trip
# ---------------------------------------------------------------------------

def test_cognito_http_roundtrip(server):
    import urllib.request, json
    base = server.base_url + "/cognito"

    def call(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(base, data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    pool = call({"action": "create_user_pool", "pool_name": "http-pool"})
    pid = pool["pool_id"]
    client = call({"action": "create_user_pool_client", "pool_id": pid, "client_name": "app"})
    cid = client["client_id"]

    signup = call({"action": "sign_up", "pool_id": pid, "client_id": cid,
                   "username": "http-user", "password": "Passw0rd!"})
    code = signup["confirmation_code"]

    call({"action": "confirm_sign_up", "pool_id": pid, "client_id": cid,
          "username": "http-user", "confirmation_code": code})

    auth = call({"action": "initiate_auth", "auth_flow": "USER_PASSWORD_AUTH",
                 "auth_parameters": {"USERNAME": "http-user", "PASSWORD": "Passw0rd!"},
                 "client_id": cid, "pool_id": pid})
    assert "access_token" in auth["authentication_result"]

    users = call({"action": "list_users", "pool_id": pid})
    names = [u["username"] for u in users["users"]]
    assert "http-user" in names

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import yaml


class KeycloakError(RuntimeError):
    pass


@dataclass
class KeycloakClient:
    id: str
    client_id: str
    secret: str | None = None


class KeycloakAdmin:
    def __init__(self, base_url: str, realm: str, token: str, verify_tls: bool = True) -> None:
        self.base_url = base_url.rstrip("/")
        self.realm = realm
        self.token = token
        self.ssl_context = ssl.create_default_context() if verify_tls else ssl._create_unverified_context()

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def request(self, method: str, path: str, payload: Any | None = None, expected: tuple[int, ...] = (200, 201, 204)) -> tuple[int, Any, dict[str, str]]:
        headers = {"Authorization": f"Bearer {self.token}", "User-Agent": "Mozilla/5.0"}
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self._url(path), data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, context=self.ssl_context, timeout=30) as resp:
                raw = resp.read().decode("utf-8")
                body: Any = raw
                ctype = resp.headers.get("Content-Type", "")
                if raw and "json" in ctype:
                    body = json.loads(raw)
                if resp.status not in expected:
                    raise KeycloakError(f"{method} {path} returned unexpected status {resp.status}: {raw[:400]}")
                return resp.status, body, dict(resp.headers)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 403:
                raise KeycloakError(
                    f"{method} {path} returned 403 Forbidden. The admin account can authenticate but lacks the required admin roles for realm '{self.realm}'."
                ) from exc
            raise KeycloakError(f"{method} {path} failed with {exc.code}: {body[:400]}") from exc

    def get_realm(self) -> dict[str, Any]:
        _, body, _ = self.request("GET", f"/admin/realms/{self.realm}")
        return body

    def list_groups(self) -> list[dict[str, Any]]:
        _, body, _ = self.request("GET", f"/admin/realms/{self.realm}/groups?briefRepresentation=false&max=500")
        return body

    def ensure_group(self, name: str, display_name: str | None = None) -> dict[str, Any]:
        groups = self.list_groups()
        for group in groups:
            if group.get("name") == name:
                return group
        payload = {"name": name}
        if display_name:
            payload["attributes"] = {"displayName": [display_name]}
        self.request("POST", f"/admin/realms/{self.realm}/groups", payload=payload, expected=(201, 204))
        for group in self.list_groups():
            if group.get("name") == name:
                return group
        raise KeycloakError(f"group '{name}' was not visible after create")

    def list_client_scopes(self) -> list[dict[str, Any]]:
        _, body, _ = self.request("GET", f"/admin/realms/{self.realm}/client-scopes")
        return body

    def get_client_scope_by_name(self, name: str) -> dict[str, Any] | None:
        for scope in self.list_client_scopes():
            if scope.get("name") == name:
                return scope
        return None

    def ensure_client_scope(self, payload: dict[str, Any]) -> dict[str, Any]:
        existing = self.get_client_scope_by_name(payload["name"])
        if existing is None:
            self.request("POST", f"/admin/realms/{self.realm}/client-scopes", payload=payload, expected=(201,))
            existing = self.get_client_scope_by_name(payload["name"])
            if existing is None:
                raise KeycloakError(f"client scope '{payload['name']}' was not visible after create")
        else:
            merged = dict(existing)
            merged.update(payload)
            self.request("PUT", f"/admin/realms/{self.realm}/client-scopes/{existing['id']}", payload=merged)
            existing = self.get_client_scope_by_name(payload["name"])
        return existing

    def list_client_scope_mappers(self, scope_id: str) -> list[dict[str, Any]]:
        _, body, _ = self.request("GET", f"/admin/realms/{self.realm}/client-scopes/{scope_id}/protocol-mappers/models")
        return body

    def ensure_client_scope_mapper(self, scope_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        for mapper in self.list_client_scope_mappers(scope_id):
            if mapper.get("name") == payload["name"]:
                merged = dict(mapper)
                merged.update(payload)
                self.request("PUT", f"/admin/realms/{self.realm}/client-scopes/{scope_id}/protocol-mappers/models/{mapper['id']}", payload=merged)
                return merged
        self.request("POST", f"/admin/realms/{self.realm}/client-scopes/{scope_id}/protocol-mappers/models", payload=payload, expected=(201,))
        for mapper in self.list_client_scope_mappers(scope_id):
            if mapper.get("name") == payload["name"]:
                return mapper
        raise KeycloakError(f"client scope mapper '{payload['name']}' was not visible after create")

    def list_clients(self, client_id: str | None = None) -> list[dict[str, Any]]:
        qs = "?max=500"
        if client_id:
            qs += "&clientId=" + urllib.parse.quote(client_id)
        _, body, _ = self.request("GET", f"/admin/realms/{self.realm}/clients{qs}")
        return body

    def get_client_by_client_id(self, client_id: str) -> dict[str, Any] | None:
        for client in self.list_clients(client_id):
            if client.get("clientId") == client_id:
                return client
        return None

    def ensure_client(self, payload: dict[str, Any]) -> KeycloakClient:
        existing = self.get_client_by_client_id(payload["clientId"])
        if existing is None:
            self.request("POST", f"/admin/realms/{self.realm}/clients", payload=payload, expected=(201,))
            existing = self.get_client_by_client_id(payload["clientId"])
            if existing is None:
                raise KeycloakError(f"client '{payload['clientId']}' was not visible after create")
        else:
            merged = dict(existing)
            merged.update(payload)
            self.request("PUT", f"/admin/realms/{self.realm}/clients/{existing['id']}", payload=merged)
            existing = self.get_client_by_client_id(payload["clientId"])
        secret = None
        if not payload.get("publicClient", False):
            try:
                _, secret_body, _ = self.request("GET", f"/admin/realms/{self.realm}/clients/{existing['id']}/client-secret")
                secret = secret_body.get("value")
            except KeycloakError:
                secret = None
        return KeycloakClient(id=existing["id"], client_id=existing["clientId"], secret=secret)

    def list_client_mappers(self, client_uuid: str) -> list[dict[str, Any]]:
        _, body, _ = self.request("GET", f"/admin/realms/{self.realm}/clients/{client_uuid}/protocol-mappers/models")
        return body

    def ensure_client_mapper(self, client_uuid: str, payload: dict[str, Any]) -> dict[str, Any]:
        for mapper in self.list_client_mappers(client_uuid):
            if mapper.get("name") == payload["name"]:
                merged = dict(mapper)
                merged.update(payload)
                self.request("PUT", f"/admin/realms/{self.realm}/clients/{client_uuid}/protocol-mappers/models/{mapper['id']}", payload=merged)
                return merged
        self.request("POST", f"/admin/realms/{self.realm}/clients/{client_uuid}/protocol-mappers/models", payload=payload, expected=(201,))
        for mapper in self.list_client_mappers(client_uuid):
            if mapper.get("name") == payload["name"]:
                return mapper
        raise KeycloakError(f"client mapper '{payload['name']}' was not visible after create")

    def list_optional_client_scopes(self, client_uuid: str) -> list[dict[str, Any]]:
        _, body, _ = self.request("GET", f"/admin/realms/{self.realm}/clients/{client_uuid}/optional-client-scopes")
        return body

    def ensure_optional_client_scope(self, client_uuid: str, scope_id: str) -> None:
        for scope in self.list_optional_client_scopes(client_uuid):
            if scope.get("id") == scope_id:
                return
        self.request("PUT", f"/admin/realms/{self.realm}/clients/{client_uuid}/optional-client-scopes/{scope_id}", expected=(204,))


def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def admin_token(config: dict[str, Any]) -> str:
    kc = config["keycloak"]
    form = urllib.parse.urlencode(
        {
            "grant_type": "password",
            "client_id": kc.get("admin_client_id", "admin-cli"),
            "username": kc["admin_username"],
            "password": kc["admin_password"],
        }
    ).encode("utf-8")
    url = f"{kc['base_url'].rstrip('/')}/realms/{kc.get('admin_realm', 'master')}/protocol/openid-connect/token"
    ssl_context = ssl.create_default_context() if kc.get("verify_tls", True) else ssl._create_unverified_context()
    req = urllib.request.Request(url, data=form, headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
    req.add_header("User-Agent", "Mozilla/5.0")
    try:
        with urllib.request.urlopen(req, context=ssl_context, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            token = body.get("access_token")
            if not token:
                raise KeycloakError("token response did not include access_token")
            return token
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise KeycloakError(f"token request failed with {exc.code}: {body[:400]}") from exc


def groups_scope_mapper() -> dict[str, Any]:
    return {
        "name": "groups",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-group-membership-mapper",
        "consentRequired": False,
        "config": {
            "claim.name": "groups",
            "full.path": "false",
            "id.token.claim": "true",
            "access.token.claim": "true",
            "userinfo.token.claim": "true",
            "jsonType.label": "String",
        },
    }


def audience_mapper(audience: str) -> dict[str, Any]:
    return {
        "name": f"audience-{audience}",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-audience-mapper",
        "consentRequired": False,
        "config": {
            "included.client.audience": audience,
            "id.token.claim": "false",
            "access.token.claim": "true",
            "introspection.token.claim": "true",
        },
    }


def hardcoded_claim_mapper(name: str, value: str) -> dict[str, Any]:
    return {
        "name": f"claim-{name}",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-hardcoded-claim-mapper",
        "consentRequired": False,
        "config": {
            "claim.name": name,
            "claim.value": value,
            "jsonType.label": "String",
            "id.token.claim": "false",
            "access.token.claim": "true",
            "userinfo.token.claim": "false",
            "introspection.token.claim": "true",
        },
    }


def ensure_groups_scope(admin: KeycloakAdmin) -> dict[str, Any]:
    scope = admin.ensure_client_scope(
        {
            "name": "groups",
            "protocol": "openid-connect",
            "description": "Expose Keycloak tenant group memberships in Omni OIDC tokens",
            "attributes": {"include.in.token.scope": "true", "display.on.consent.screen": "false"},
        }
    )
    admin.ensure_client_scope_mapper(scope["id"], groups_scope_mapper())
    return scope


def ui_client_payload(tenant: dict[str, Any], secret: str) -> dict[str, Any]:
    main_url = tenant["urls"]["main"]
    return {
        "clientId": tenant["ui_client_id"],
        "name": f"Omni UI ({tenant['id']})",
        "enabled": True,
        "protocol": "openid-connect",
        "publicClient": False,
        "secret": secret,
        "standardFlowEnabled": True,
        "directAccessGrantsEnabled": False,
        "serviceAccountsEnabled": False,
        "implicitFlowEnabled": False,
        "rootUrl": main_url,
        "baseUrl": main_url,
        "redirectUris": [f"{main_url}/oidc/consume"],
        "webOrigins": [main_url],
        "attributes": {
            "post.logout.redirect.uris": "+",
            "oauth2.device.authorization.grant.enabled": "false",
        },
        "defaultClientScopes": ["profile", "email"],
        "optionalClientScopes": ["groups"],
    }


def service_client_payload(client_id: str, name: str, secret: str) -> dict[str, Any]:
    return {
        "clientId": client_id,
        "name": name,
        "enabled": True,
        "protocol": "openid-connect",
        "publicClient": False,
        "secret": secret,
        "standardFlowEnabled": False,
        "directAccessGrantsEnabled": False,
        "serviceAccountsEnabled": True,
        "implicitFlowEnabled": False,
        "fullScopeAllowed": False,
    }


def validate_config(config: dict[str, Any]) -> None:
    required = ["keycloak", "astra", "tenants"]
    for key in required:
        if key not in config:
            raise KeycloakError(f"config missing top-level key: {key}")
    if not config["tenants"]:
        raise KeycloakError("config must define at least one tenant")


def summarize_existing_client(admin: KeycloakAdmin, client_id: str) -> dict[str, Any]:
    existing = admin.get_client_by_client_id(client_id)
    if existing is None:
        return {"client_id": client_id, "present": False}
    return {"client_id": client_id, "present": True, "client_uuid": existing.get("id")}


def run(mode: str, config: dict[str, Any]) -> dict[str, Any]:
    validate_config(config)
    token = admin_token(config)
    kc = config["keycloak"]
    admin = KeycloakAdmin(kc["base_url"], kc["realm"], token, kc.get("verify_tls", True))
    realm_repr = admin.get_realm()
    summary: dict[str, Any] = {
        "realm": realm_repr.get("realm"),
        "base_url": kc["base_url"],
        "validated_admin_api": True,
        "groups": [],
        "client_scopes": [],
        "clients": [],
    }

    audience = config["astra"].get("audience", "astra")
    tenant_claim = config["astra"].get("tenant_claim", "tenant_id")
    migrator_cfg = config.get("migrator", {})
    migrator_client_id = migrator_cfg.get("client_id", "astra-migrator")

    if mode == "validate":
        groups_scope = admin.get_client_scope_by_name("groups")
        summary["client_scopes"].append(
            {
                "name": "groups",
                "present": groups_scope is not None,
                "id": None if groups_scope is None else groups_scope.get("id"),
            }
        )
        summary["clients"].append(summarize_existing_client(admin, audience))
        summary["clients"].append(summarize_existing_client(admin, migrator_client_id))
        groups = {group.get("name"): group for group in admin.list_groups()}
        for tenant in config["tenants"]:
            summary["groups"].append(
                {
                    "name": f"tenant:{tenant['id']}",
                    "present": f"tenant:{tenant['id']}" in groups,
                }
            )
            summary["clients"].append(summarize_existing_client(admin, tenant["ui_client_id"]))
            summary["clients"].append(summarize_existing_client(admin, tenant["astra_client_id"]))
        summary["status"] = mode
        return summary

    groups_scope = ensure_groups_scope(admin)
    summary["client_scopes"].append({"name": groups_scope["name"], "id": groups_scope["id"]})

    audience_client = admin.ensure_client(
        {
            "clientId": audience,
            "name": "Astra audience anchor",
            "enabled": True,
            "protocol": "openid-connect",
            "publicClient": False,
            "standardFlowEnabled": False,
            "directAccessGrantsEnabled": False,
            "serviceAccountsEnabled": False,
            "implicitFlowEnabled": False,
        }
    )
    summary["clients"].append({"client_id": audience_client.client_id, "client_uuid": audience_client.id, "secret": audience_client.secret})

    migrator_secret = migrator_cfg["client_secret"]
    migrator = admin.ensure_client(service_client_payload(migrator_client_id, "Astra migrator", migrator_secret))
    admin.ensure_client_mapper(migrator.id, audience_mapper(audience))
    admin.ensure_client_mapper(migrator.id, hardcoded_claim_mapper(tenant_claim, migrator_cfg.get("tenant_id", "ops")))
    summary["clients"].append({"client_id": migrator.client_id, "client_uuid": migrator.id, "secret": migrator.secret})

    for tenant in config["tenants"]:
        group = admin.ensure_group(f"tenant:{tenant['id']}", display_name=tenant.get("display_name", tenant["id"]))
        summary["groups"].append({"name": group["name"], "id": group["id"]})

        ui = admin.ensure_client(ui_client_payload(tenant, tenant["ui_client_secret"]))
        admin.ensure_optional_client_scope(ui.id, groups_scope["id"])
        summary["clients"].append({"client_id": ui.client_id, "client_uuid": ui.id, "secret": ui.secret})

        svc = admin.ensure_client(
            service_client_payload(
                tenant["astra_client_id"],
                f"Astra service ({tenant['id']})",
                tenant["astra_client_secret"],
            )
        )
        admin.ensure_client_mapper(svc.id, audience_mapper(audience))
        admin.ensure_client_mapper(svc.id, hardcoded_claim_mapper(tenant_claim, tenant["id"]))
        summary["clients"].append({"client_id": svc.client_id, "client_uuid": svc.id, "secret": svc.secret})

    summary["status"] = mode
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate or provision the Omni Keycloak realm model for Astra integration")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    parser.add_argument("--mode", choices=["validate", "apply"], default="validate")
    parser.add_argument("--output", help="Optional JSON output path")
    args = parser.parse_args()

    try:
        cfg = load_config(args.config)
        summary = run(args.mode, cfg)
    except KeycloakError as exc:
        print(json.dumps({"status": "error", "message": str(exc)}, indent=2), file=sys.stderr)
        return 2

    encoded = json.dumps(summary, indent=2)
    print(encoded)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(encoded + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

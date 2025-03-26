#!/bin/bash
set -euo pipefail

# Wait until Keycloak is up
until curl -sSf http://keycloak:8080/realms/master > /dev/null; do
  echo "⌛ Waiting for Keycloak..."
  sleep 2
done

echo "✅ Keycloak is up. Running configuration..."

# Authenticate with kcadm
/opt/keycloak/bin/kcadm.sh config credentials \
  --server http://keycloak:8080 \
  --realm master \
  --user "$KEYCLOAK_ADMIN" \
  --password "$KEYCLOAK_ADMIN_PASSWORD"

# Create realm if it doesn't exist
if ! /opt/keycloak/bin/kcadm.sh get realms/"$KC_REALM" > /dev/null 2>&1; then
  echo "📦 Creating realm '$KC_REALM'"
  /opt/keycloak/bin/kcadm.sh create realms \
    -s realm="$KC_REALM" \
    -s enabled=true
else
  echo "✅ Realm '$KC_REALM' already exists"
fi

# Create client if not exists
if ! /opt/keycloak/bin/kcadm.sh get clients -r "$KC_REALM" -q clientId="$KC_CLIENT_ID" | grep -q '"id"'; then
  echo "🔐 Creating client '$KC_CLIENT_ID'"
  /opt/keycloak/bin/kcadm.sh create clients -r "$KC_REALM" \
    -s clientId="$KC_CLIENT_ID" \
    -s enabled=true \
    -s protocol="openid-connect" \
    -s publicClient=false \
    -s secret="$KC_CLIENT_SECRET" \
    -s "redirectUris=[\"$KC_CLIENT_REDIRECT_URL\"]" \
    -s directAccessGrantsEnabled=true \
    -s standardFlowEnabled=true
else
  echo "✅ Client '$KC_CLIENT_ID' already exists"
fi

# Create user if not exists
if ! /opt/keycloak/bin/kcadm.sh get users -r "$KC_REALM" -q username="$KC_USERNAME" | grep -q '"id"'; then
  echo "👤 Creating user '$KC_USERNAME'"
  USER_ID=$(/opt/keycloak/bin/kcadm.sh create users -r "$KC_REALM" \
    -s username="$KC_USERNAME" \
    -s enabled=true -i)

  /opt/keycloak/bin/kcadm.sh set-password -r "$KC_REALM" \
    --userid "$USER_ID" \
    --new-password "$KC_PASSWORD"
else
  echo "✅ User '$KC_USERNAME' already exists"
fi

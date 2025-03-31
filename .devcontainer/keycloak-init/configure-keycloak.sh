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
    -s standardFlowEnabled=true \
    -s serviceAccountsEnabled=true
else
  echo "✅ Client '$KC_CLIENT_ID' already exists"
fi

# Get client UUID
client_uuid=$(/opt/keycloak/bin/kcadm.sh get clients -r "$KC_REALM" -q clientId="$KC_CLIENT_ID" --fields id --format csv | tail -n1 | tr -d '"' | tr -d '\r')

# Get service account username and user ID
service_account_username="service-account-$KC_CLIENT_ID"
service_account_id=$(/opt/keycloak/bin/kcadm.sh get users -r "$KC_REALM" -q username="$service_account_username" --fields id --format csv | tail -n1 | tr -d '"' | tr -d '\r')

# Get realm-management client UUID
realm_mgmt_uuid=$(/opt/keycloak/bin/kcadm.sh get clients -r "$KC_REALM" -q clientId=realm-management --fields id --format csv | tail -n1 | tr -d '"' | tr -d '\r')

# Assign roles if not already assigned
for role in view-users query-users; do
  echo "🔍 Checking if role '$role' is already assigned to '$service_account_username'..."

  role_already_assigned=$(
    /opt/keycloak/bin/kcadm.sh get users/$service_account_id/role-mappings/clients/$realm_mgmt_uuid -r "$KC_REALM" \
    | grep -F "\"name\" : \"$role\"" || true
  )

  if [[ -z "$role_already_assigned" ]]; then
    echo "➕ Assigning role '$role'"
    /opt/keycloak/bin/kcadm.sh add-roles \
      --uusername "$service_account_username" \
      --cclientid realm-management \
      --rolename "$role" \
      -r "$KC_REALM"
  else
    echo "✅ Role '$role' already assigned"
  fi
done

# Create user if not exists
if ! /opt/keycloak/bin/kcadm.sh get users -r "$KC_REALM" -q username="$KC_USER_USERNAME" | grep -q '"id"'; then
  echo "👤 Creating user '$KC_USER_USERNAME'"
  USER_ID=$(/opt/keycloak/bin/kcadm.sh create users -r "$KC_REALM" \
    -s username="$KC_USER_USERNAME" \
    -s email="$KC_USER_EMAIL" \
    -s enabled=true -i)

  /opt/keycloak/bin/kcadm.sh set-password -r "$KC_REALM" \
    --userid "$USER_ID" \
    --new-password "$KC_USER_PASSWORD"
else
  echo "✅ User '$KC_USER_USERNAME' already exists"
fi

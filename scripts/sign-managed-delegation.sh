#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 2 ]]; then
  echo 'usage: sign-managed-delegation.sh ROOT_PRIVATE_KEY MANIFEST_JSON' >&2
  exit 64
fi

private_key=$1
manifest=$2
[[ -f "$private_key" && ! -L "$private_key" ]]
[[ $(stat -c '%U:%G:%a' -- "$private_key") == root:root:600 ]]
[[ -f "$manifest" && ! -L "$manifest" ]]
[[ $(stat -c '%s' -- "$manifest") -le 262144 ]]

public_der_hex=$(openssl pkey -in "$private_key" -pubout -outform DER \
  | od -An -tx1 -v | tr -d ' \n')
[[ $public_der_hex =~ ^302a300506032b6570032100([0-9a-f]{64})$ ]]
public_key_hex=${BASH_REMATCH[1]}

umask 077
message_file=$(mktemp /dev/shm/symphony-grant-sign.XXXXXXXX)
trap 'rm -f -- "$message_file"' EXIT
printf '%s\0' 'hypergrid.symphony.managed-delegation.v1' >"$message_file"
cat -- "$manifest" >>"$message_file"
signature_hex=$(openssl pkeyutl -sign -rawin -inkey "$private_key" \
  -in "$message_file" \
  | od -An -tx1 -v | tr -d ' \n')
[[ $signature_hex =~ ^[0-9a-f]{128}$ ]]

printf 'DAHLIA_MANAGED_DELEGATION_SHA256=%s\n' "$(sha256sum -- "$manifest" | cut -d ' ' -f 1)"
printf 'DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519=%s\n' "$signature_hex"
printf 'DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519=%s\n' "$public_key_hex"

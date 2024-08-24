#!/usr/bin/env -S deno run -A
// Usage: deno run -A ./scripts/aes_decrypt.ts <password> <salt-hex> <nonce-hex> <aead-string> <ciphertext-hex>

const PBKDF2_ITERATIONS = 256;

// Function to convert a hex string to a Uint8Array
function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
  }
  return bytes;
}

// Function to derive the AES key using PBKDF2
async function deriveKey(password: string, salt: Uint8Array): Promise<CryptoKey> {
  const passwordKey = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(password),
    { name: "PBKDF2" },
    false,
    ["deriveKey"]
  );

  return await crypto.subtle.deriveKey(
    {
      name: "PBKDF2",
      salt: salt,
      iterations: PBKDF2_ITERATIONS,
      hash: "SHA-256",
    },
    passwordKey,
    { name: "AES-GCM", length: 256 },
    false,
    ["decrypt"]
  );
}

// Function to decrypt the ciphertext using AES-GCM
async function decrypt(
  aesKey: CryptoKey,
  nonce: Uint8Array,
  ciphertext: Uint8Array,
  aead: string
): Promise<string> {
  const decrypted = await crypto.subtle.decrypt(
    {
      name: "AES-GCM",
      iv: nonce,
      additionalData: new TextEncoder().encode(aead),
    },
    aesKey,
    ciphertext
  );

  return new TextDecoder().decode(decrypted);
}

// Main function
async function main() {
  const args = Deno.args;
  if (args.length !== 5) {
    console.error("Usage: deno run script.ts <password> <salt-hex> <nonce-hex> <aead-string> <ciphertext-hex>");
    Deno.exit(1);
  }

  const [password, saltHex, nonceHex, aead, ciphertextHex] = args;

  // Convert hex strings to byte arrays
  const salt = hexToBytes(saltHex);
  const nonce = hexToBytes(nonceHex);
  const ciphertext = hexToBytes(ciphertextHex);

  // Derive the AES key
  const aesKey = await deriveKey(password, salt);

  // Decrypt the ciphertext
  const plaintext = await decrypt(aesKey, nonce, ciphertext, aead);

  console.log("Plaintext:", plaintext);
}

// Run the main function
main();

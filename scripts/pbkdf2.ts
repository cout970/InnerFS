#!/usr/bin/env -S deno run -A
// Usage: deno run -A ./scripts/pbkdf2.ts <password> <salt> [n-iterations]
// Prints the PBKDF2 hash of the password using the salt and n-iterations provided
// The result binary array is encoded as a hex string

async function pbkdf2(password, salt, iterations, hashAlgorithm) {
    const keyMaterial = await crypto.subtle.importKey(
        "raw",
        new TextEncoder().encode(password),
        { name: "PBKDF2" },
        false,
        ["deriveBits", "deriveKey"]
    );
    const key = await crypto.subtle.deriveKey(
        {
            name: "PBKDF2",
            salt: new TextEncoder().encode(salt),
            iterations: iterations,
            hash: hashAlgorithm,
        },
        keyMaterial,
        { name: "HMAC", hash: hashAlgorithm, length: 256 },
        true,
        ["sign"]
    );
    return await crypto.subtle.exportKey("raw", key);
}

let array_buffer = await pbkdf2(Deno.args[0], Deno.args[1], Deno.args[2] || 1, 'SHA-256');
let hex = Array.from(new Uint8Array(array_buffer)).map((b) => b.toString(16).padStart(2, "0")).join("");

console.log(hex);

// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"reflect"
	"testing"
)

func TestPKCS7(t *testing.T) {
	for i := 0; i <= 255; i++ {
		// Make a buffer of size i
		buf := []byte{}
		for j := 0; j < i; j++ {
			buf = append(buf, byte(i))
		}

		// Copy to bytes buffer
		inp := bytes.NewBuffer(nil)
		inp.Write(buf)

		// Pad this out
		pkcs7encode(inp, 0, 16)

		// Unpad
		dec, err := pkcs7decode(inp.Bytes(), 16)
		if err != nil {
			t.Fatal(err)
		}

		// Ensure equivilence
		if !reflect.DeepEqual(buf, dec) {
			t.Fatalf("mismatch: %v %v", buf, dec)
		}
	}

}

func TestEncryptDecrypt_V0(t *testing.T) {
	encryptDecryptVersioned(0, t)
}

func TestEncryptDecrypt_V1(t *testing.T) {
	encryptDecryptVersioned(1, t)
}

func encryptDecryptVersioned(vsn encryptionVersion, t *testing.T) {
	k1 := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	plaintext := []byte("this is a plain text message")
	extra := []byte("random data")

	var buf bytes.Buffer
	err := encryptPayload(vsn, k1, plaintext, extra, &buf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	expLen := encryptedLength(vsn, len(plaintext))
	if buf.Len() != expLen {
		t.Fatalf("output length is unexpected %d %d %d", len(plaintext), buf.Len(), expLen)
	}

	msg, err := decryptPayload([][]byte{k1}, buf.Bytes(), extra)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	cmp := bytes.Compare(msg, plaintext)
	if cmp != 0 {
		t.Errorf("len %d %v", len(msg), msg)
		t.Errorf("len %d %v", len(plaintext), plaintext)
		t.Fatalf("encrypt/decrypt failed! %d '%s' '%s'", cmp, msg, plaintext)
	}
}

// TestDecryptPayloadRejectsInvalidPadding: version-0 payloads are PKCS7
// padded inside the authenticated ciphertext. A key holder that seals a
// malformed pad (zero, or longer than the plaintext) must get an error, not
// a slice-bounds panic on the receiver.
func TestDecryptPayloadRejectsInvalidPadding(t *testing.T) {
	key := []byte("0123456789abcdef")
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	// Plaintexts are at least one block long so they pass the size check
	// and reach the padding decoder.
	padded := func(n int, last byte) []byte {
		b := make([]byte, n)
		b[n-1] = last
		return b
	}
	for _, plain := range [][]byte{padded(16, 0xff), padded(16, 0), padded(32, 33)} {
		nonce := make([]byte, nonceSize)
		msg := append([]byte{0}, nonce...)
		msg = gcm.Seal(msg, nonce, plain, nil)

		var decErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decryptPayload(%x) panicked: %v", plain, r)
				}
			}()
			_, decErr = decryptPayload([][]byte{key}, msg, nil)
		}()
		if decErr == nil {
			t.Fatalf("decryptPayload accepted invalid PKCS7 padding %x", plain)
		}
	}
}

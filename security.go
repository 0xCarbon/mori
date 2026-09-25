// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
)

/*
Encrypted messages are prefixed with an encryptionVersion byte
that is used for us to be able to properly encode/decode. We
currently support the following versions:

	0 - AES-GCM 128, using PKCS7 padding
	1 - AES-GCM 128, no padding. Padding not needed, caused bloat.
*/
type encryptionVersion uint8

const (
	minEncryptionVersion encryptionVersion = 0
	maxEncryptionVersion encryptionVersion = 1
)

const (
	versionSize    = 1
	nonceSize      = 12
	tagSize        = 16
	maxPadOverhead = 16
	blockSize      = aes.BlockSize
)

// pkcs7encode is used to pad a byte buffer to a specific block size using
// the PKCS7 algorithm. "Ignores" some bytes to compensate for IV
func pkcs7encode(buf *bytes.Buffer, ignore, blockSize int) {
	n := buf.Len() - ignore
	more := blockSize - (n % blockSize)
	for range more {
		buf.WriteByte(byte(more))
	}
}

// pkcs7decode removes PKCS7 padding. The pad length must be between 1 and
// blockSize and fit the buffer; the padded bytes are authenticated by GCM,
// so a malformed pad means a misbehaving key holder, not line noise.
func pkcs7decode(buf []byte, blockSize int) ([]byte, error) {
	if len(buf) == 0 {
		return nil, errors.New("cannot decode a PKCS7 buffer of zero length")
	}
	pad := int(buf[len(buf)-1])
	if pad < 1 || pad > blockSize || pad > len(buf) {
		return nil, fmt.Errorf("invalid PKCS7 padding length %d", pad)
	}
	return buf[:len(buf)-pad], nil
}

// encryptOverhead returns the maximum possible overhead of encryption by version
func encryptOverhead(vsn encryptionVersion) int {
	switch vsn {
	case 0:
		return 45 // Version: 1, IV: 12, Padding: 16, Tag: 16
	case 1:
		return 29 // Version: 1, IV: 12, Tag: 16
	default:
		panic("unsupported version")
	}
}

// encryptedLength is used to compute the buffer size needed
// for a message of given length
func encryptedLength(vsn encryptionVersion, inp int) int {
	// If we are on version 1, there is no padding
	if vsn >= 1 {
		return versionSize + nonceSize + inp + tagSize
	}

	// Determine the padding size
	padding := blockSize - (inp % blockSize)

	// Sum the extra parts to get total size
	return versionSize + nonceSize + inp + padding + tagSize
}

// newAEAD returns the AES-GCM cipher for key.
func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// sealPayload appends the encryption of msg, authenticating data, to dst:
// version byte, random nonce, then the GCM ciphertext (of msg PKCS7-padded
// for version 0).
func sealPayload(vsn encryptionVersion, gcm cipher.AEAD, msg, data, dst []byte) ([]byte, error) {
	dst = slices.Grow(dst, encryptedLength(vsn, len(msg)))
	dst = append(dst, byte(vsn))
	start := len(dst)
	dst = dst[:start+nonceSize]
	nonce := dst[start:]
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	src := msg
	if vsn == 0 {
		pad := blockSize - len(msg)%blockSize
		src = make([]byte, len(msg), len(msg)+pad)
		copy(src, msg)
		for range pad {
			src = append(src, byte(pad))
		}
	}
	return gcm.Seal(dst, nonce, src, data), nil
}

// encryptPayload encrypts msg with key into dst (see sealPayload).
func encryptPayload(vsn encryptionVersion, key []byte, msg []byte, data []byte, dst *bytes.Buffer) error {
	gcm, err := newAEAD(key)
	if err != nil {
		return err
	}
	out, err := sealPayload(vsn, gcm, msg, data, nil)
	if err != nil {
		return err
	}
	dst.Write(out)
	return nil
}

// openPayload decrypts msg with the first cipher that authenticates it,
// removing version-0 padding.
func openPayload(aeads []cipher.AEAD, msg []byte, data []byte) ([]byte, error) {
	// Ensure we have at least one byte
	if len(msg) == 0 {
		return nil, fmt.Errorf("cannot decrypt empty payload")
	}

	// Verify the version
	vsn := encryptionVersion(msg[0])
	if vsn > maxEncryptionVersion {
		return nil, fmt.Errorf("unsupported encryption version %d", msg[0])
	}

	// Ensure the length is sane
	if len(msg) < encryptedLength(vsn, 0) {
		return nil, fmt.Errorf("payload is too small to decrypt: %d", len(msg))
	}

	nonce := msg[versionSize : versionSize+nonceSize]
	ciphertext := msg[versionSize+nonceSize:]
	for _, gcm := range aeads {
		plain, err := gcm.Open(nil, nonce, ciphertext, data)
		if err == nil {
			if vsn == 0 {
				return pkcs7decode(plain, aes.BlockSize)
			}
			return plain, nil
		}
	}

	return nil, fmt.Errorf("no installed keys could decrypt the message")
}

// decryptPayload decrypts msg with the given raw keys (see openPayload).
func decryptPayload(keys [][]byte, msg []byte, data []byte) ([]byte, error) {
	aeads := make([]cipher.AEAD, 0, len(keys))
	for _, key := range keys {
		gcm, err := newAEAD(key)
		if err != nil {
			return nil, err
		}
		aeads = append(aeads, gcm)
	}
	return openPayload(aeads, msg, data)
}

func appendBytes(first []byte, second []byte) []byte {
	hasFirst := len(first) > 0
	hasSecond := len(second) > 0

	switch {
	case hasFirst && hasSecond:
		out := make([]byte, 0, len(first)+len(second))
		out = append(out, first...)
		out = append(out, second...)
		return out
	case hasFirst:
		return first
	case hasSecond:
		return second
	default:
		return nil
	}
}

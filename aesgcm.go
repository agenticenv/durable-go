package durable

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

const (
	aesgcmVersion   byte = 0x01
	aesgcmNonceSize int  = 12
)

// aesgcmCodec encrypts payload bytes with AES-GCM. The key is held only in
// process memory; it is never written under dataDir.
type aesgcmCodec struct {
	gcm cipher.AEAD
}

// NewAESGCMCodec returns a PayloadCodec that wraps payloads as
// version | nonce | ciphertext+tag (AES-GCM). key must be 16, 24, or 32
// bytes (AES-128/192/256). The caller owns key storage (env / secret
// manager); this value is copied and never persisted by the engine.
func NewAESGCMCodec(key []byte) (PayloadCodec, error) {
	switch len(key) {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("durable: AES-GCM key must be 16, 24, or 32 bytes, got %d", len(key))
	}
	owned := make([]byte, len(key))
	copy(owned, key)
	block, err := aes.NewCipher(owned)
	if err != nil {
		return nil, fmt.Errorf("durable: AES-GCM cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("durable: AES-GCM: %w", err)
	}
	if gcm.NonceSize() != aesgcmNonceSize {
		return nil, fmt.Errorf("durable: AES-GCM nonce size %d, want %d", gcm.NonceSize(), aesgcmNonceSize)
	}
	return &aesgcmCodec{gcm: gcm}, nil
}

func (c *aesgcmCodec) Encode(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, aesgcmNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("durable: AES-GCM nonce: %w", err)
	}
	sealed := c.gcm.Seal(nil, nonce, plaintext, aad)
	out := make([]byte, 1+aesgcmNonceSize+len(sealed))
	out[0] = aesgcmVersion
	copy(out[1:], nonce)
	copy(out[1+aesgcmNonceSize:], sealed)
	return out, nil
}

func (c *aesgcmCodec) Decode(ciphertext, aad []byte) ([]byte, error) {
	min := 1 + aesgcmNonceSize + c.gcm.Overhead()
	if len(ciphertext) < min {
		return nil, fmt.Errorf("durable: AES-GCM ciphertext too short")
	}
	if ciphertext[0] != aesgcmVersion {
		return nil, fmt.Errorf("durable: AES-GCM unsupported version 0x%02x", ciphertext[0])
	}
	nonce := ciphertext[1 : 1+aesgcmNonceSize]
	plain, err := c.gcm.Open(nil, nonce, ciphertext[1+aesgcmNonceSize:], aad)
	if err != nil {
		return nil, fmt.Errorf("durable: AES-GCM decrypt: %w", err)
	}
	return plain, nil
}

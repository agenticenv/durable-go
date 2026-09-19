package durable

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

const (
	aesgcmVersion   byte = 0x01 // version | nonce | sealed
	aesgcmVersionID byte = 0x02 // version | keyID | nonce | sealed
	aesgcmNonceSize int  = 12
	// AESGCMRekeyAfter is the recommended maximum number of Encode calls
	// on one 96-bit-nonce key. Random-nonce birthday collision risk
	// becomes material around 2^32 seals; rotate sooner for high volume.
	AESGCMRekeyAfter uint64 = 1 << 32
)

// AESGCMKey is one AES-GCM key plus the identifier written next to each
// ciphertext when using the v2 wire format. ID 0 is also how v1 blobs
// (no key-ID byte) are decoded.
type AESGCMKey struct {
	ID  byte
	Key []byte
}

type aesgcmCodec struct {
	id       byte
	gcm      cipher.AEAD
	previous map[byte]cipher.AEAD
	writeV2  bool
}

// NewAESGCMCodec returns a PayloadCodec that wraps payloads as
// version | nonce | ciphertext+tag (AES-GCM v1). key must be 16, 24, or 32
// bytes (AES-128/192/256). The caller owns key storage (env / secret
// manager); this value is copied and never persisted by the engine.
//
// Prefer NewAESGCMCodecWithKeys when you need a key ID on the wire and a
// dual-key read window for rotation.
func NewAESGCMCodec(key []byte) (PayloadCodec, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	return &aesgcmCodec{
		id:       0,
		gcm:      aead,
		previous: map[byte]cipher.AEAD{0: aead},
		writeV2:  false,
	}, nil
}

// NewAESGCMCodecWithKeys encodes with current (v2: version | keyID | nonce |
// sealed) and can decode current, any previous key, and v1 blobs (treated
// as key ID 0). Pass the retiring key as previous so in-flight journals
// keep reading during a rotation. Rotate before AESGCMRekeyAfter seals
// on one key.
func NewAESGCMCodecWithKeys(current AESGCMKey, previous ...AESGCMKey) (PayloadCodec, error) {
	cur, err := newAEAD(current.Key)
	if err != nil {
		return nil, err
	}
	prev := make(map[byte]cipher.AEAD, len(previous)+1)
	prev[current.ID] = cur
	for _, k := range previous {
		aead, err := newAEAD(k.Key)
		if err != nil {
			return nil, err
		}
		prev[k.ID] = aead
	}
	return &aesgcmCodec{
		id:       current.ID,
		gcm:      cur,
		previous: prev,
		writeV2:  true,
	}, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
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
	return gcm, nil
}

func (c *aesgcmCodec) Encode(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, aesgcmNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("durable: AES-GCM nonce: %w", err)
	}
	sealed := c.gcm.Seal(nil, nonce, plaintext, aad)
	if !c.writeV2 {
		out := make([]byte, 1+aesgcmNonceSize+len(sealed))
		out[0] = aesgcmVersion
		copy(out[1:], nonce)
		copy(out[1+aesgcmNonceSize:], sealed)
		return out, nil
	}
	out := make([]byte, 2+aesgcmNonceSize+len(sealed))
	out[0] = aesgcmVersionID
	out[1] = c.id
	copy(out[2:], nonce)
	copy(out[2+aesgcmNonceSize:], sealed)
	return out, nil
}

func (c *aesgcmCodec) Decode(ciphertext, aad []byte) ([]byte, error) {
	if len(ciphertext) < 1 {
		return nil, fmt.Errorf("durable: AES-GCM ciphertext too short")
	}
	switch ciphertext[0] {
	case aesgcmVersion:
		return c.open(c.lookup(0), ciphertext[1:], aad)
	case aesgcmVersionID:
		if len(ciphertext) < 2 {
			return nil, fmt.Errorf("durable: AES-GCM ciphertext too short")
		}
		return c.open(c.lookup(ciphertext[1]), ciphertext[2:], aad)
	default:
		return nil, fmt.Errorf("durable: AES-GCM unsupported version 0x%02x", ciphertext[0])
	}
}

func (c *aesgcmCodec) lookup(id byte) cipher.AEAD {
	if aead, ok := c.previous[id]; ok {
		return aead
	}
	if id == c.id {
		return c.gcm
	}
	return nil
}

func (c *aesgcmCodec) open(aead cipher.AEAD, rest, aad []byte) ([]byte, error) {
	if aead == nil {
		return nil, fmt.Errorf("durable: AES-GCM unknown key id")
	}
	if len(rest) < aesgcmNonceSize+aead.Overhead() {
		return nil, fmt.Errorf("durable: AES-GCM ciphertext too short")
	}
	nonce := rest[:aesgcmNonceSize]
	plain, err := aead.Open(nil, nonce, rest[aesgcmNonceSize:], aad)
	if err != nil {
		return nil, fmt.Errorf("durable: AES-GCM decrypt: %w", err)
	}
	return plain, nil
}

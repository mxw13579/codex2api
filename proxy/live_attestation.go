package proxy

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"strings"
)

// decryptLiveAttestation 恢复建连时的决定；解密后的空串表示该会话不发送证明。
func (h *Handler) decryptLiveAttestation(record *liveCallRecord) (string, error) {
	if record == nil || strings.TrimSpace(record.AttestationCiphertext) == "" {
		return "", errors.New("the Live call has no reusable attestation")
	}
	plain, err := decryptLiveAttestation(record.AttestationCiphertext, h.liveAttestationSecret())
	if err != nil {
		return "", errors.New("the Live call attestation cannot be decrypted on this instance")
	}
	return plain, nil
}

func (h *Handler) liveAttestationSecret() string {
	if h != nil && h.cfg != nil {
		return strings.TrimSpace(h.cfg.AdminSecret)
	}
	return ""
}

func liveAttestationKey(secret string) [32]byte {
	return sha256.Sum256([]byte("codex2api/live-attestation/v1\x00" + secret))
}

func encryptLiveAttestation(plaintext, secret string) (string, error) {
	key := liveAttestationKey(secret)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	encrypted := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.RawStdEncoding.EncodeToString(encrypted), nil
}

func decryptLiveAttestation(ciphertext, secret string) (string, error) {
	encrypted, err := base64.RawStdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}
	key := liveAttestationKey(secret)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(encrypted) < gcm.NonceSize() {
		return "", errors.New("encrypted Live attestation is too short")
	}
	plain, err := gcm.Open(nil, encrypted[:gcm.NonceSize()], encrypted[gcm.NonceSize():], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

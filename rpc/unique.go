package rpc

import (
	"encoding/binary"
	"errors"
	"io"
)

type ProtocolUniqueID uint64
type TypeUniqueID uint64
type Position uint64

func (t TypeUniqueID) EncodeToBytes(b []byte) {
	binary.BigEndian.PutUint64(b, uint64(t))
}
func (t TypeUniqueID) Encode(w io.Writer) error {
	var b [8]byte
	t.EncodeToBytes(b[:])
	n, err := w.Write(b[:])
	if n != 8 {
		return errors.New("short buffer write")
	}
	return err
}

type Uniquer interface {
	ToUint64() uint64
}

func (t TypeUniqueID) ToUint64() uint64 {
	return uint64(t)
}

func (p ProtocolUniqueID) ToUint64() uint64 {
	return uint64(p)
}

var allUniques []uint64

func AddUnique(u Uniquer) {
	allUniques = append(allUniques, u.ToUint64())
}

func AllUniques() []uint64 {
	return allUniques
}

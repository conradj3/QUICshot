// Package tunnelproto is the control plane on the edge↔connector QUIC hop.
// HTTP requests stay on streams the edge opens; the connector opens one
// client-initiated stream and writes JSON lines:
//
//	{"t":"register","hostname":"local","ha_id":0}
//	{"t":"ping"}
//	{"t":"unregister","reason":"shutdown"}
//
// QUIC up is not the same as tunnel up: the edge only picks connectors that
// have an active register.
package tunnelproto

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

const (
	TypeRegister   = "register"
	TypePing       = "ping"
	TypeUnregister = "unregister"
)

type Msg struct {
	T        string `json:"t"`
	Hostname string `json:"hostname,omitempty"`
	HAID     int    `json:"ha_id,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

func Write(w io.Writer, m Msg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

func Read(r *bufio.Reader) (Msg, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return Msg{}, err
	}
	var m Msg
	if err := json.Unmarshal(line, &m); err != nil {
		return Msg{}, fmt.Errorf("tunnel control: %w", err)
	}
	if m.T == "" {
		return Msg{}, fmt.Errorf("tunnel control: missing t")
	}
	return m, nil
}

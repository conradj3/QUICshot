// Package udpsock creates UDP sockets with explicit buffer sizes and reports the
// kernel's UDP error counters, which is where dropped-because-the-receive-buffer-
// was-full shows up.
package udpsock

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// Listen binds a UDP socket. recvBuf/sendBuf are applied with SetReadBuffer /
// SetWriteBuffer when non-zero.
//
// Note: quic-go will try to raise the receive buffer to its own preferred size
// on startup. A deliberately small buffer therefore only sticks if the host's
// net.core.rmem_max is also low — see scripts/host-rmem.sh.
func Listen(addr string, recvBuf, sendBuf int) (*net.UDPConn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	if recvBuf > 0 {
		if err := conn.SetReadBuffer(recvBuf); err != nil {
			conn.Close()
			return nil, fmt.Errorf("set read buffer %d: %w", recvBuf, err)
		}
	}
	if sendBuf > 0 {
		if err := conn.SetWriteBuffer(sendBuf); err != nil {
			conn.Close()
			return nil, fmt.Errorf("set write buffer %d: %w", sendBuf, err)
		}
	}
	return conn, nil
}

// Counters mirrors the Udp: line of /proc/net/snmp.
type Counters struct {
	InDatagrams  uint64
	NoPorts      uint64
	InErrors     uint64
	OutDatagrams uint64
	RcvbufErrors uint64
	SndbufErrors uint64
	InCsumErrors uint64
}

func (c Counters) String() string {
	return fmt.Sprintf("in=%d out=%d in_errors=%d rcvbuf_errors=%d sndbuf_errors=%d",
		c.InDatagrams, c.OutDatagrams, c.InErrors, c.RcvbufErrors, c.SndbufErrors)
}

// Sub returns c minus prev, for per-interval deltas.
func (c Counters) Sub(prev Counters) Counters {
	return Counters{
		InDatagrams:  c.InDatagrams - prev.InDatagrams,
		NoPorts:      c.NoPorts - prev.NoPorts,
		InErrors:     c.InErrors - prev.InErrors,
		OutDatagrams: c.OutDatagrams - prev.OutDatagrams,
		RcvbufErrors: c.RcvbufErrors - prev.RcvbufErrors,
		SndbufErrors: c.SndbufErrors - prev.SndbufErrors,
		InCsumErrors: c.InCsumErrors - prev.InCsumErrors,
	}
}

// ReadCounters parses /proc/net/snmp. It returns ok=false on non-Linux hosts.
func ReadCounters() (Counters, bool) {
	f, err := os.Open("/proc/net/snmp")
	if err != nil {
		return Counters{}, false
	}
	defer f.Close()
	return parseCounters(f)
}

func parseCounters(r io.Reader) (Counters, bool) {
	var header []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "Udp:") {
			continue
		}
		fields := strings.Fields(line)
		if header == nil {
			header = fields
			continue
		}
		values := map[string]uint64{}
		for i := 1; i < len(fields) && i < len(header); i++ {
			n, err := strconv.ParseUint(fields[i], 10, 64)
			if err != nil {
				continue
			}
			values[header[i]] = n
		}
		return Counters{
			InDatagrams:  values["InDatagrams"],
			NoPorts:      values["NoPorts"],
			InErrors:     values["InErrors"],
			OutDatagrams: values["OutDatagrams"],
			RcvbufErrors: values["RcvbufErrors"],
			SndbufErrors: values["SndbufErrors"],
			InCsumErrors: values["InCsumErrors"],
		}, true
	}
	if err := sc.Err(); err != nil {
		return Counters{}, false
	}
	return Counters{}, false
}

package blast

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/conrad/quicshot/internal/quicerr"
	"github.com/conrad/quicshot/internal/udpsock"
)

// probe0RTT dials the target twice on a shared TLS session cache. The second
// DialEarly is the one that can use 0-RTT. This is a scenario, not a guarantee:
// the server has to issue a session ticket.
func probe0RTT(ctx context.Context, cfg config, tlsConf *tls.Config, quicConf *quic.Config, rec *recorder) error {
	addr, err := quicDialAddr(cfg.target)
	if err != nil {
		return err
	}
	first, err := dialEarlyOnce(ctx, cfg, tlsConf, quicConf, addr)
	if err != nil {
		return fmt.Errorf("0-rtt first dial: %w", err)
	}
	rec.emit(event{
		Event: "0rtt_probe", Conn: 0, Used0RTT: first.used0RTT,
		Detail: "first handshake (session ticket source)", Version: first.version,
		Local: first.local, Remote: first.remote,
	})
	first.close()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(150 * time.Millisecond):
	}

	second, err := dialEarlyOnce(ctx, cfg, tlsConf, quicConf, addr)
	if err != nil {
		kind, detail := quicerr.Classify(err)
		rec.emit(event{Event: "0rtt_probe", Kind: string(kind), Detail: "second dial failed: " + detail})
		return err
	}
	rec.emit(event{
		Event: "0rtt_probe", Conn: 1, Used0RTT: second.used0RTT,
		Detail:  fmt.Sprintf("second handshake used_0rtt=%t", second.used0RTT),
		Version: second.version, Local: second.local, Remote: second.remote,
	})
	second.close()
	return nil
}

type earlyDial struct {
	conn     *quic.Conn
	tr       *quic.Transport
	used0RTT bool
	version  string
	local    string
	remote   string
}

func (d earlyDial) close() {
	if d.conn != nil {
		_ = d.conn.CloseWithError(0, "0rtt probe")
	}
	if d.tr != nil {
		_ = d.tr.Close()
	}
}

func dialEarlyOnce(ctx context.Context, cfg config, tlsConf *tls.Config, quicConf *quic.Config, addr *net.UDPAddr) (earlyDial, error) {
	udp, err := udpsock.Listen(":0", cfg.recvBuf, 0)
	if err != nil {
		return earlyDial{}, err
	}
	tr := &quic.Transport{Conn: udp}
	dctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	conn, err := tr.DialEarly(dctx, addr, tlsConf, quicConf)
	if err != nil {
		_ = tr.Close()
		_ = udp.Close()
		return earlyDial{}, err
	}
	select {
	case <-conn.HandshakeComplete():
	case <-conn.Context().Done():
		_ = tr.Close()
		return earlyDial{}, fmt.Errorf("handshake aborted: %w", context.Cause(conn.Context()))
	case <-dctx.Done():
		_ = tr.Close()
		return earlyDial{}, dctx.Err()
	}
	state := conn.ConnectionState()
	return earlyDial{
		conn: conn, tr: tr, used0RTT: state.Used0RTT,
		version: fmt.Sprintf("0x%x", uint32(state.Version)),
		local:   conn.LocalAddr().String(), remote: conn.RemoteAddr().String(),
	}, nil
}

func quicDialAddr(rawURL string) (*net.UDPAddr, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "443"
	}
	return resolveUDP(net.JoinHostPort(host, port))
}

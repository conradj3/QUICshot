// Command quicshot is a single binary with four roles that together reproduce a
// Cloudflare-tunnel-style HTTP/3 path locally:
//
//	blast (h3 client) -> edge (h3 term, 524 owner) =QUIC tunnel= connector -> origin (http/1.1)
package main

import (
	"fmt"
	"os"

	"github.com/conrad/quicshot/internal/blast"
	"github.com/conrad/quicshot/internal/certs"
	"github.com/conrad/quicshot/internal/connector"
	"github.com/conrad/quicshot/internal/edge"
	"github.com/conrad/quicshot/internal/origin"
	"github.com/conrad/quicshot/internal/probe"
	"github.com/conrad/quicshot/internal/ui"
)

func usage() {
	fmt.Fprint(os.Stderr, `quicshot <command> [flags]

commands:
  gencerts    generate a CA + server cert into a shared directory
  origin      plain HTTP/1.1 origin with controllable latency/hang/reset behaviour
  connector   cloudflared-like connector: dials the edge over QUIC, proxies to origin
  edge        Cloudflare-like edge: terminates HTTP/3, enforces the origin timeout (524)
  blast       HTTP/3 load harness that logs client-side disconnects
  probe       one-shot diagnostic for one URL: DNS, TLS, Alt-Svc, HTTP/3
  ui          local control panel: drive the stack and watch every log

run "quicshot <command> -h" for flags
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "gencerts":
		err = certs.Main(os.Args[2:])
	case "origin":
		err = origin.Main(os.Args[2:])
	case "connector":
		err = connector.Main(os.Args[2:])
	case "edge":
		err = edge.Main(os.Args[2:])
	case "blast":
		err = blast.Main(os.Args[2:])
	case "probe":
		err = probe.Main(os.Args[2:])
	case "ui":
		err = ui.Main(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "quicshot %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

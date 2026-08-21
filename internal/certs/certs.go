// Package certs generates the self-signed CA and leaf certificate shared by the
// edge, the connector and the load harness.
package certs

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	CAFile     = "ca.pem"
	CertFile   = "server.pem"
	KeyFile    = "server.key"
	tunnelALPN = "cfd-repro"
)

// TunnelALPN is the ALPN used on the edge<->connector QUIC hop.
const TunnelALPN = tunnelALPN

func Main(args []string) error {
	fs := flag.NewFlagSet("gencerts", flag.ExitOnError)
	dir := fs.String("dir", "/certs", "output directory")
	hosts := fs.String("hosts", "edge,localhost,127.0.0.1,::1", "comma separated SANs")
	force := fs.Bool("force", false, "regenerate even if files exist")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return Generate(*dir, strings.Split(*hosts, ","), *force)
}

func Generate(dir string, hosts []string, force bool) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if !force {
		if _, err := os.Stat(filepath.Join(dir, CertFile)); err == nil {
			fmt.Fprintf(os.Stderr, "certs: reusing existing material in %s\n", dir)
			return nil
		}
	}
	ca, err := mintCA()
	if err != nil {
		return err
	}
	leafDER, leafKeyDER, err := mintLeaf(ca, hosts)
	if err != nil {
		return err
	}
	if err := writeMaterial(dir, ca.der, leafDER, leafKeyDER); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "certs: wrote %s/{%s,%s,%s}\n", dir, CAFile, CertFile, KeyFile)
	return nil
}

type caMaterial struct {
	key  *ecdsa.PrivateKey
	cert *x509.Certificate
	der  []byte
}

func mintCA() (caMaterial, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return caMaterial{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "QUICshot local CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return caMaterial{}, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return caMaterial{}, err
	}
	return caMaterial{key: key, cert: cert, der: der}, nil
}

func mintLeaf(ca caMaterial, hosts []string) (leafDER, keyDER []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: "QUICshot edge"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	addHosts(tmpl, hosts)
	leafDER, err = x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err = x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return leafDER, keyDER, nil
}

func addHosts(tmpl *x509.Certificate, hosts []string) {
	for _, h := range hosts {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
			continue
		}
		tmpl.DNSNames = append(tmpl.DNSNames, h)
	}
}

func writeMaterial(dir string, caDER, leafDER, leafKeyDER []byte) error {
	if err := writePEM(filepath.Join(dir, CAFile), "CERTIFICATE", caDER, 0o644); err != nil {
		return err
	}
	chain := append(pemBytes("CERTIFICATE", leafDER), pemBytes("CERTIFICATE", caDER)...)
	if err := os.WriteFile(filepath.Join(dir, CertFile), chain, 0o644); err != nil {
		return err
	}
	return writePEM(filepath.Join(dir, KeyFile), "EC PRIVATE KEY", leafKeyDER, 0o600)
}

// LoadCAPool reads the generated CA so clients can verify the edge.
func LoadCAPool(dir string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(filepath.Join(dir, CAFile))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("certs: no CA certificate found")
	}
	return pool, nil
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		panic(err)
	}
	return n
}

func pemBytes(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

func writePEM(path, typ string, der []byte, mode os.FileMode) error {
	return os.WriteFile(path, pemBytes(typ, der), mode)
}

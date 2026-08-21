package integration

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func runCurlH3(url, ca string) (string, error) {
	if hostCurlHasH3() {
		args := []string{"--http3-only", "--max-time", "8", "-skS", "-D-", "-o", os.DevNull, url}
		if ca != "" {
			args = []string{"--http3-only", "--cacert", ca, "--max-time", "8", "-sS", "-D-", "-o", os.DevNull, url}
		}
		out, err := exec.Command("curl", args...).CombinedOutput()
		if err != nil {
			return string(out), fmt.Errorf("host curl --http3-only: %w\n%s", err, out)
		}
		return string(out), nil
	}
	return "", fmt.Errorf("host curl has no --http3-only; install a quic-enabled curl for third-stack interop")
}

func hostCurlHasH3() bool {
	out, err := exec.Command("curl", "--help").CombinedOutput()
	return err == nil && strings.Contains(string(out), "--http3-only")
}

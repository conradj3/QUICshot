package tunnelproto

import (
	"bufio"
	"bytes"
	"testing"
)

func TestWriteReadRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	want := Msg{T: TypeRegister, Hostname: "local", HAID: 3}
	if err := Write(&buf, want); err != nil {
		t.Fatal(err)
	}
	got, err := Read(bufio.NewReader(&buf))
	if err != nil {
		t.Fatal(err)
	}
	if got.T != want.T || got.Hostname != want.Hostname || got.HAID != want.HAID {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

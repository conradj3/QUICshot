package ui

import (
	"reflect"
	"testing"
)

func TestSafeURL(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "https", value: "https://example.com/path"},
		{name: "http", value: "http://example.com/path"},
		{name: "missing", value: "", wantErr: true},
		{name: "unsupported scheme", value: "file:///tmp/x", wantErr: true},
		{name: "missing host", value: "https:///path", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := safeURL(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("safeURL(%q) error = %v, wantErr %v", tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestClamp(t *testing.T) {
	tests := []struct {
		value int
		want  int
	}{
		{value: -1, want: 1},
		{value: 4, want: 4},
		{value: 999, want: 10},
	}

	for _, tt := range tests {
		if got := clamp(tt.value, 1, 10); got != tt.want {
			t.Fatalf("clamp(%d, 1, 10) = %d, want %d", tt.value, got, tt.want)
		}
	}
}

func TestCountLines(t *testing.T) {
	if got := countLines("abc\n\ndef\n"); got != 2 {
		t.Fatalf("countLines() = %d, want 2", got)
	}
	if got := countLines("\n\t\n"); got != 0 {
		t.Fatalf("countLines() = %d, want 0", got)
	}
}

func TestStackCommandConnectorActions(t *testing.T) {
	tests := []struct {
		name    string
		req     stackReq
		want    []string
		wantErr bool
	}{
		{
			name: "start connector",
			req:  stackReq{Action: "start-connector"},
			want: []string{"compose", "up", "-d", "--scale", "connector=1", "connector"},
		},
		{
			name: "scale connectors",
			req:  stackReq{Action: "scale-connectors", Connectors: 3},
			want: []string{"compose", "up", "-d", "--scale", "connector=3", "connector"},
		},
		{
			name:    "scale below range",
			req:     stackReq{Action: "scale-connectors", Connectors: -1},
			wantErr: true,
		},
		{
			name:    "scale above range",
			req:     stackReq{Action: "scale-connectors", Connectors: 11},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got, err := stackCommand(tt.req)
			if (err != nil) != tt.wantErr {
				t.Fatalf("stackCommand() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("stackCommand() args = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestStackCommandValidatesDurations(t *testing.T) {
	_, _, err := stackCommand(stackReq{Action: "restart-edge", OriginTimeout: "ten seconds"})
	if err == nil {
		t.Fatal("stackCommand() succeeded with invalid duration")
	}
}

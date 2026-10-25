package bencode

import (
	"reflect"
	"testing"
)

func TestDecodeIntegers(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"i42e", 42},
		{"i0e", 0},
		{"i-15e", -15},
		{"i1234567890123e", 1234567890123},
	}

	for _, tt := range tests {
		val, err := Decode([]byte(tt.input))
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tt.input, err)
		}
		if val != tt.expected {
			t.Errorf("expected %v, got %v", tt.expected, val)
		}
	}
}

func TestDecodeStrings(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"4:spam", "spam"},
		{"0:", ""},
		{"12:Transmission", "Transmission"},
	}

	for _, tt := range tests {
		val, err := Decode([]byte(tt.input))
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tt.input, err)
		}
		if val != tt.expected {
			t.Errorf("expected %q, got %q", tt.expected, val)
		}
	}
}

func TestDecodeListsAndDicts(t *testing.T) {
	// list of strings: l4:spami42ee
	inputList := "l4:spami42ee"
	valList, err := Decode([]byte(inputList))
	if err != nil {
		t.Fatalf("unexpected error decoding list: %v", err)
	}
	expectedList := []any{"spam", int64(42)}
	if !reflect.DeepEqual(valList, expectedList) {
		t.Errorf("expected %v, got %v", expectedList, valList)
	}

	// dictionary: d3:bar4:spam3:fooi42ee
	inputDict := "d3:bar4:spam3:fooi42ee"
	valDict, err := Decode([]byte(inputDict))
	if err != nil {
		t.Fatalf("unexpected error decoding dict: %v", err)
	}
	expectedDict := map[string]any{
		"bar": "spam",
		"foo": int64(42),
	}
	if !reflect.DeepEqual(valDict, expectedDict) {
		t.Errorf("expected %v, got %v", expectedDict, valDict)
	}
}

func TestEncode(t *testing.T) {
	dict := map[string]any{
		"announce": "http://tracker.example.com/announce",
		"info": map[string]any{
			"length":       int64(1048576),
			"name":         "debian.iso",
			"piece length": int64(262144),
			"pieces":       "aabbccddeeff",
		},
	}

	encoded, err := Encode(dict)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	// Re-decode to verify roundtrip
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("failed to decode encoded data: %v", err)
	}

	decodedDict, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded result is not map[string]any: %T", decoded)
	}

	if decodedDict["announce"] != "http://tracker.example.com/announce" {
		t.Errorf("unexpected announce: %v", decodedDict["announce"])
	}
}

func TestDecodeLeading(t *testing.T) {
	// A dictionary followed by raw binary payload (like in ut_metadata)
	input := []byte("d8:msg_typei1e5:piecei0eeEXTRA_BINARY_DATA")

	val, consumed, err := DecodeLeading(input)
	if err != nil {
		t.Fatalf("DecodeLeading failed: %v", err)
	}

	dict, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", val)
	}

	if dict["msg_type"] != int64(1) || dict["piece"] != int64(0) {
		t.Errorf("unexpected dict content: %+v", dict)
	}

	expectedPrefix := "d8:msg_typei1e5:piecei0ee"
	if consumed != len(expectedPrefix) {
		t.Errorf("expected %d consumed bytes, got %d", len(expectedPrefix), consumed)
	}

	trailing := string(input[consumed:])
	if trailing != "EXTRA_BINARY_DATA" {
		t.Errorf("expected trailing 'EXTRA_BINARY_DATA', got %q", trailing)
	}
}


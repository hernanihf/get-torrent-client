package bencode

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
)

var (
	ErrInvalidBencode = errors.New("invalid bencode data")
	ErrUnexpectedEOF  = errors.New("unexpected EOF while parsing bencode")
)

// Decode parses a bencoded byte slice and returns the corresponding Go data structure:
// - string: []byte or string
// - integer: int64
// - list: []any
// - dictionary: map[string]any
func Decode(data []byte) (any, error) {
	r := bytes.NewReader(data)
	br := bufio.NewReader(r)
	val, err := decodeValue(br)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func decodeValue(r *bufio.Reader) (any, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch {
	case b == 'i':
		return decodeInt(r)
	case b == 'l':
		return decodeList(r)
	case b == 'd':
		return decodeDict(r)
	case b >= '0' && b <= '9':
		if err := r.UnreadByte(); err != nil {
			return nil, err
		}
		return decodeString(r)
	default:
		return nil, fmt.Errorf("%w: unexpected token '%c'", ErrInvalidBencode, b)
	}
}

func decodeInt(r *bufio.Reader) (int64, error) {
	line, err := r.ReadBytes('e')
	if err != nil {
		return 0, ErrUnexpectedEOF
	}
	numStr := string(line[:len(line)-1])
	val, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid integer '%s'", ErrInvalidBencode, numStr)
	}
	return val, nil
}

func decodeString(r *bufio.Reader) (string, error) {
	lenBytes, err := r.ReadBytes(':')
	if err != nil {
		return "", ErrUnexpectedEOF
	}
	lengthStr := string(lenBytes[:len(lenBytes)-1])
	strLen, err := strconv.Atoi(lengthStr)
	if err != nil || strLen < 0 {
		return "", fmt.Errorf("%w: invalid string length '%s'", ErrInvalidBencode, lengthStr)
	}

	buf := make([]byte, strLen)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return "", ErrUnexpectedEOF
	}
	return string(buf), nil
}

func decodeList(r *bufio.Reader) ([]any, error) {
	var list []any
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, ErrUnexpectedEOF
		}
		if b == 'e' {
			break
		}
		if err := r.UnreadByte(); err != nil {
			return nil, err
		}
		val, err := decodeValue(r)
		if err != nil {
			return nil, err
		}
		list = append(list, val)
	}
	return list, nil
}

func decodeDict(r *bufio.Reader) (map[string]any, error) {
	dict := make(map[string]any)
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, ErrUnexpectedEOF
		}
		if b == 'e' {
			break
		}
		if err := r.UnreadByte(); err != nil {
			return nil, err
		}

		key, err := decodeString(r)
		if err != nil {
			return nil, fmt.Errorf("%w: failed decoding dictionary key", err)
		}

		val, err := decodeValue(r)
		if err != nil {
			return nil, fmt.Errorf("%w: failed decoding dictionary value for key %q", err, key)
		}
		dict[key] = val
	}
	return dict, nil
}

// Encode serializes a Go data structure into its bencode representation.
func Encode(val any) ([]byte, error) {
	var buf bytes.Buffer
	if err := encodeValue(&buf, val); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeValue(w *bytes.Buffer, val any) error {
	switch v := val.(type) {
	case int:
		fmt.Fprintf(w, "i%de", v)
	case int64:
		fmt.Fprintf(w, "i%de", v)
	case string:
		fmt.Fprintf(w, "%d:%s", len(v), v)
	case []byte:
		fmt.Fprintf(w, "%d:%s", len(v), string(v))
	case []any:
		w.WriteByte('l')
		for _, item := range v {
			if err := encodeValue(w, item); err != nil {
				return err
			}
		}
		w.WriteByte('e')
	case []string:
		w.WriteByte('l')
		for _, item := range v {
			if err := encodeValue(w, item); err != nil {
				return err
			}
		}
		w.WriteByte('e')
	case map[string]any:
		w.WriteByte('d')
		// Keys in bencode dictionaries MUST be sorted lexicographically
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := encodeValue(w, k); err != nil {
				return err
			}
			if err := encodeValue(w, v[k]); err != nil {
				return err
			}
		}
		w.WriteByte('e')
	default:
		return fmt.Errorf("unsupported type for bencode encoding: %T", val)
	}
	return nil
}

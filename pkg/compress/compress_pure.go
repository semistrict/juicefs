/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//go:build nocompress

package compress

import (
	"fmt"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Compressor interface to be implemented by a compression algo
type Compressor interface {
	Name() string
	CompressBound(int) int
	Compress(dst, src []byte) (int, error)
	Decompress(dst, src []byte) (int, error)
}

// NewCompressor returns a struct implementing Compressor interface
func NewCompressor(algr string) Compressor {
	algr = strings.ToLower(algr)
	if algr == "zstd" {
		return ZStandard{}
	} else if algr == "lz4" {
		return LZ4{}
	} else if algr == "none" || algr == "" {
		return noOp{}
	}
	return nil
}

type noOp struct{}

func (n noOp) Name() string            { return "Noop" }
func (n noOp) CompressBound(l int) int { return l }
func (n noOp) Compress(dst, src []byte) (int, error) {
	if len(dst) < len(src) {
		return 0, fmt.Errorf("buffer too short: %d < %d", len(dst), len(src))
	}
	copy(dst, src)
	return len(src), nil
}
func (n noOp) Decompress(dst, src []byte) (int, error) {
	if len(dst) < len(src) {
		return 0, fmt.Errorf("buffer too short: %d < %d", len(dst), len(src))
	}
	copy(dst, src)
	return len(src), nil
}

// ZStandard implements Compressor using klauspost/compress/zstd (pure Go)
type ZStandard struct{}

func (n ZStandard) Name() string { return "Zstd" }

func (n ZStandard) CompressBound(l int) int {
	// zstd worst case: input size + header overhead
	return l + l/255 + 64
}

func (n ZStandard) Compress(dst, src []byte) (int, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return 0, err
	}
	d := enc.EncodeAll(src, dst[:0])
	enc.Close()
	if len(d) > 0 && len(dst) > 0 && &d[0] != &dst[0] {
		return 0, fmt.Errorf("buffer too short: %d < %d", cap(dst), len(d))
	}
	return len(d), nil
}

func (n ZStandard) Decompress(dst, src []byte) (int, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return 0, err
	}
	defer dec.Close()
	d, err := dec.DecodeAll(src, dst[:0])
	if err != nil {
		return 0, err
	}
	if len(d) > 0 && len(dst) > 0 && &d[0] != &dst[0] {
		return 0, fmt.Errorf("buffer too short: %d < %d", len(dst), len(d))
	}
	return len(d), nil
}

// LZ4 implements Compressor using pierrec/lz4 (pure Go)
type LZ4 struct{}

func (l LZ4) Name() string { return "LZ4" }

func (l LZ4) CompressBound(size int) int {
	return lz4.CompressBlockBound(size)
}

func (l LZ4) Compress(dst, src []byte) (int, error) {
	var c lz4.Compressor
	n, err := c.CompressBlock(src, dst)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, fmt.Errorf("data incompressible")
	}
	return n, nil
}

func (l LZ4) Decompress(dst, src []byte) (int, error) {
	if len(src) == 0 {
		return 0, fmt.Errorf("decompress an empty input")
	}
	n, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return 0, err
	}
	return n, nil
}

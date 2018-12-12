package chunker

import (
	"testing"
	"bytes"
	"io"
	"errors"
	"math"
	"os"
	"net"
	"strconv"
	"fmt"
	"context"
)

type mockBuffer struct {
	bytes.Buffer
}

func newMockBuffer() *mockBuffer {
	return &mockBuffer{}
}

func (b *mockBuffer) WriteBuffers(bufs net.Buffers) (int64, error) {
	return bufs.WriteTo(&b.Buffer)
}

func TestStreamEncoding(t *testing.T) {

	type testCase struct {
		in []byte
		csiz uint32
		rcsiz uint32
		err error
		msg string
	}

	cases := []testCase{
		{in: []byte{}, csiz: 1024, msg: "empty"},
		{in: []byte{0x23}, csiz: 1024, msg:"one byte"},
		{in: bytes.Repeat([]byte{0x23}, 1025), csiz: 512, msg: "multiple chunks"},
		{in: []byte{0x23, 0x24, 0x25}, csiz: 2, rcsiz: 1, err: ChunkSizeExceededError, msg:"excessive chunk size"},
	}

	for _, test := range cases {

		t.Logf("testing: %s", test.msg)

		src := bytes.NewBuffer(test.in)

		buf := newMockBuffer()
		err := WriteStream(context.Background(), buf, src, test.csiz)
		if err != nil {
			t.Errorf("WriteStream should not fail writing to bytes.Buffer")
			continue
		}

		rcsiz := test.rcsiz
		if rcsiz == 0 {
			rcsiz = test.csiz
		}
		r := NewStreamReader(buf, rcsiz)
		var dest bytes.Buffer
		n, err := io.Copy(&dest, r)
		if err != nil && err != test.err {
			t.Errorf("unexpected error: expecting\n%#v\ngot\n%#v", test.err, err)
			continue
		}
		if err != nil {
			continue
		}
		if n != int64(len(test.in)) {
			t.Errorf("reader returned length %d != %d (expected)", n, len(test.in))
		}

		if bytes.Compare(test.in, dest.Bytes()) != 0 {
			t.Errorf("input bytes do not match output bytes.Expected:\t%#v\nGot: \t%#v", test.in, dest.Bytes())
		}

	}

}

type failingReader struct {
	Wrap io.Reader
	FailAt int
	FailWith error
	at int
}

func (r *failingReader) InducedFail() bool {
	return r.at >= r.FailAt
}

func (r *failingReader) Read(p []byte) (n int, err error) {
	if r.at >= r.FailAt {
		return 0, r.FailWith
	}
	maxread := r.FailAt - r.at
	if maxread > len(p) {
		maxread = len(p)
	}
	n, err = r.Wrap.Read(p[0:maxread])
	r.at += n
	if r.at >= r.FailAt {
		return n, r.FailWith
	}
	return n, err
}


type failingWriter struct {
	Wrap io.Writer
	FailAt int
	FailWith error
	at int
}

func (w *failingWriter) InducedFail() bool {
	return w.at >= w.FailAt
}


func (w *failingWriter) Write(p []byte) (n int, err error) {
	if w.at >= w.FailAt {
		return 0, w.FailWith
	}
	n, err = w.Wrap.Write(p)
	w.at += n
	if w.at >= w.FailAt {
		return n, w.FailWith
	}
	return n, err
}

type failingChunkBufferWriter struct {
	failingWriter
}

func newFailingChunkBufferWriter(fw failingWriter) failingChunkBufferWriter {
	return failingChunkBufferWriter{fw}
}

func (b failingChunkBufferWriter) WriteBuffers(bufs net.Buffers) (int64, error) {
	return bufs.WriteTo(&b.failingWriter)
}


func TestStreamDecodingIOError(t *testing.T) {

	dataStr := []byte("this is a test string")
	var totalLen int
	{
		data := bytes.NewBuffer(dataStr)
		stream := newMockBuffer()
		WriteStream(context.Background(), stream, data, 2)
		totalLen = stream.Len()
	}

	t.Logf("total stream length = %d", totalLen)

	for failAt := 0; failAt < totalLen; failAt++ {
		t.Logf("failAt: %d / %d", failAt, totalLen)
		data := bytes.NewBuffer(dataStr)
		stream := newMockBuffer()
		WriteStream(context.Background(), stream, data, 2)

		e := errors.New("subtle error")

		fstream := failingReader{stream, failAt, e, 0}

		dec := NewStreamReader(&fstream, 10)
		var decBuf bytes.Buffer
		cn, err := io.Copy(&decBuf, dec)
		t.Logf("io.Copy n = %d / %d (len(dataStr))", cn, len(dataStr))
		if err != e {
			t.Errorf("StreamReader should passs through io errors, but got: %#v", err)
		}
		t.Logf("bytes left in stream: %d", stream.Len())
		e2 := errors.New("this error should not appear")
		fstream.FailWith = e2
		var buf [10]byte
		n, err := dec.Read(buf[:])
		if n != 0 {
			t.Errorf("subsequent calls to StreamReader should return n = 0, but got %d", n)
		}
		if err != e {
			t.Errorf("subsequent calls should return the original io error, but got %#v", err)
		}
	}
}

func TestStreamEncodingSrcIOErrorForwarding(t *testing.T) {

	data := bytes.NewBufferString("this is a test")
	e := errors.New("123") // will be truncated to "12" because chunk size is 2
	fdata := failingReader{data, 3, e, 0}

	chunkSize := uint32(2)
	out := newMockBuffer()
	err := WriteStream(context.Background(), out, &fdata, chunkSize)
	if wrappedErr, ok := err.(*SourceStreamError); !ok || wrappedErr.StreamError != e {
		t.Errorf("WriteStream should wrap reader io errors, but got: %#v", err)
	}

	dec := NewStreamReader(out, 2)
	var decBuf bytes.Buffer
	_, err = io.Copy(&decBuf, dec)
	if err == nil {
		t.Errorf("WriteStream should have encoded an error, and StreamReader should have returned it")
		return
	}
	if err.Error() != e.Error()[:chunkSize] {
		t.Errorf("error messages must be equal.\nexpected:\t%#v\ngot:\t%#v", e.Error(), err.Error())
	}

}


func TestStreamEncodingWriteIOError(t *testing.T) {

	dataStr := []byte("this is a test string that is longer than the chunk size")
	chunkSize := 2
	const chunkHeaderLen = 5 // uint32 + uint8
	chunks := int(math.Ceil(float64(len(dataStr)) / float64(chunkSize)))
	approxTotalLen := chunks * ( chunkSize + chunkHeaderLen) + (chunkHeaderLen + chunkSize) // data + error chunks

	for writeFailAt := 0; writeFailAt <= approxTotalLen; writeFailAt++ {
		for readFailAt := 0; readFailAt <= len(dataStr) + 1; readFailAt++ {
			writeErr := errors.New("write error")
			readErr := errors.New("read  error")
			data := bytes.NewBuffer(dataStr)
			fdata := failingReader{data, readFailAt, readErr, 0}
			out := newMockBuffer()
			fout := newFailingChunkBufferWriter(failingWriter{out, writeFailAt, writeErr, 0})
			err := WriteStream(context.Background(), &fout, &fdata, uint32(chunkSize))

			if fdata.InducedFail() && !fout.InducedFail() {
				// expect readError
				if wrappedErr, ok := err.(*SourceStreamError); !ok || wrappedErr.StreamError != readErr {
					t.Errorf("WriteStream should wrap read errors, but got '%#v'", err)
				}
			} else {
				if err != writeErr {
					t.Errorf("WriteStream should pass through write io errors, but got: %#v", err)
				}
			}
		}
	}

}

func doEncodingBenchmark(maxChunkSize uint32, copyByteCount int64, b *testing.B) {
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		l, err := net.Listen("tcp", "127.0.0.1:12345")
		if err != nil {
			panic(err)
		}
		defer l.Close()
		close(ready)
		c, err := l.Accept()
		if err != nil {
			panic(err)
		}
		defer c.Close()
		dest, err := os.OpenFile("/dev/null", os.O_WRONLY, 0600)
		if err != nil {
			panic(err)
		}
		io.Copy(dest, NewStreamReader(c, maxChunkSize))
	}()

	<-ready

	dest, err := net.Dial("tcp", "127.0.0.1:12345")
	if err != nil {
		panic(err)
	}
	defer dest.Close()
	src, _ := os.Open("/dev/zero")
	if b != nil {
		b.ResetTimer()
	}
	WriteStream(context.Background(), NetConnExportBuffersWriter{dest}, io.LimitReader(src, copyByteCount), maxChunkSize)
	dest.Close()

	<- done
}

func TestStreamEncodingBenchmark(t *testing.T) {

	// Compile a test binary using go test -c -x, and use flame graphs or some other tool to look at performance
	// This setup was used to root out unnecessary allocations.
	// For performance evaluation, see BenchmarkStreamEncoding

	maxChunkSize, _ := strconv.ParseUint(os.Getenv("MAX_CHUNK_SIZE"), 10, 32)
	copyByteCount, _ := strconv.ParseInt(os.Getenv("COPY_BYTES"), 10, 64)
	if maxChunkSize == 0 || copyByteCount == 0 {
		t.Skip("for manual usage, e.g. for generating flame graphs")
	}
	doEncodingBenchmark(uint32(maxChunkSize), copyByteCount, nil)
}

func BenchmarkStreamEncoding(b *testing.B) {
	// Usage: go test -test.bench=BenchmarkStreamEncoding -test.run=^$
	//
	// Useful to find good defaults for csiz.
	// The ns/op correspond to ns/byte, because b.N is used to specify the number of bytes to transfer lower is better.
	//
	// Note that this only tests one benchmark setup.
	//
	// Example output 1:
	// 	goos: freebsd
	// 	goarch: amd64
	// 	pkg: github.com/problame/go-streamrpc
	// 	BenchmarkStreamEncoding/csiz=2^10-8             300000000                5.57 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^11-8             500000000                3.22 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^12-8             2000000000               1.54 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^13-8             2000000000               1.03 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^14-8             2000000000               0.61 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^15-8             2000000000               0.41 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^16-8             2000000000               0.34 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^17-8             2000000000               0.31 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^18-8             2000000000               0.28 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^19-8             2000000000               0.28 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^20-8             2000000000               0.28 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^21-8             2000000000               0.29 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^22-8             2000000000               0.32 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^23-8             2000000000               0.36 ns/op
	//
	// Example output 2:
	// 	goos: linux
	// 	goarch: amd64
	// 	pkg: github.com/problame/go-streamrpc
	// 	BenchmarkStreamEncoding/csiz=2^10-4             200000000                6.75 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^11-4             500000000                3.01 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^12-4             2000000000               1.57 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^13-4             2000000000               0.71 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^14-4             2000000000               0.57 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^15-4             2000000000               0.39 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^16-4             2000000000               0.32 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^17-4             2000000000               0.27 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^18-4             2000000000               0.24 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^19-4             2000000000               0.23 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^20-4             2000000000               0.22 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^21-4             2000000000               0.25 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^22-4             2000000000               0.28 ns/op
	// 	BenchmarkStreamEncoding/csiz=2^23-4             2000000000               0.29 ns/op
	//
	for csiz := uint32(10); csiz <= 23; csiz++ {
		b.Run(fmt.Sprintf("csiz=2^%v", csiz), func(b *testing.B) {
			doEncodingBenchmark(1 << csiz, int64(b.N), b)
		})
	}
}

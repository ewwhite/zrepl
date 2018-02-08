package cmd

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/zrepl/zrepl/zfs"
	"testing"

	"io"
)

func TestReceiveRequest_ToReader(t *testing.T) {

	var in bytes.Buffer
	in.WriteString("foobar2342")

	path, _ := zfs.NewDatasetPath("foo/bar")
	req := ReceiveRequest{
		ReceiveRequestHeader{
			path,
			false,
		},
		&in,
	}

	reader, err := req.ToReader()
	assert.NoError(t, err)

	var buf bytes.Buffer
	io.Copy(&buf, reader)

	t.Logf("buf contents: %s", buf.String())

	res, err := ReceiveRequestFromReader(&buf)
	assert.NoError(t, err)
	assert.Equal(t, req.Header.Filesystem, res.Header.Filesystem)
	assert.Equal(t, req.Header.Rollback, res.Header.Rollback)
	var out bytes.Buffer
	io.Copy(&out, res.Stream)
	assert.Equal(t, "foobar2342", out.String())

}

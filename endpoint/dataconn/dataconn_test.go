package dataconn

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReqHeaderMarshalling(t *testing.T) {
	var r reqHeader
	secondline := `{"endpoint":"someendpoint","protolen":23}`
	input := fmt.Sprintf("ZREPL_DATA_CONN HEADER_LEN=%0*d\n%s\n", 10, len(secondline)+1, secondline)
	var buf bytes.Buffer
	buf.WriteString(input)
	err := r.unmarshalFromWire(&buf, 4096)
	assert.NoError(t, err)
	assert.Equal(t, "someendpoint", r.Endpoint)
	assert.Equal(t, uint32(23), r.ProtoLen)
}

func TestReqHeaderRoundtripMarshal(t *testing.T) {
	r := reqHeader{
		Endpoint:   "someendpointwith\ninit",
		ProtoLen:   23,
		HandlerErr: "some error message\n{}{}{}{}",
	}

	var buf bytes.Buffer
	assert.NoError(t, r.marshalToWire(&buf))
	buf.WriteString("teststream")

	var ur reqHeader
	assert.NoError(t, ur.unmarshalFromWire(&buf, 4096))
	assert.Equal(t, uint32(23), ur.ProtoLen)
	assert.Equal(t, "someendpointwith\ninit", ur.Endpoint)
	assert.Equal(t, "some error message\n{}{}{}{}", ur.HandlerErr)

	assert.Equal(t, string(buf.Bytes()), "teststream")
}

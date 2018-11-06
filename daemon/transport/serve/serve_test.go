package serve

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAuthRemoteAddr_FromString(t *testing.T) {

	a := AuthRemoteAddr{ClientIdentity: `identity with a"`}
	s := a.String()
	t.Logf("encoded: %s", s)

	b := AuthRemoteAddr{}
	err := b.FromString(s)
	assert.NoError(t, err)
	assert.Equal(t, a.ClientIdentity, b.ClientIdentity)

}

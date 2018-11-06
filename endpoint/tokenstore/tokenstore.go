package tokenstore

import (
	"fmt"
	"github.com/pkg/errors"
	"sync"
	"time"
	"github.com/dgrijalva/jwt-go"
	cryptorand "crypto/rand"
)

type assocData struct {
	expiration time.Time
	data interface{}
}

type Store struct {
	mtx sync.Mutex
	key []byte
	data map[string]assocData // key is the jwt.SignedString(key)
}

func (s *Store) Add(data interface{}, expirationTime time.Time) (token string, err error) {
	if time.Now().After(expirationTime) {
		return "", fmt.Errorf("expiration time must be in the future")
	}
	// include claims to make the token self-speaking but we don't verify it on take
	// because assocData stores expirationTime separately
	claims := jwt.StandardClaims{
		ExpiresAt: expirationTime.Unix(),
		IssuedAt:  time.Now().Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token, err = tok.SignedString(s.key)
	if err != nil {
		return "", errors.Wrap(err, "could not sign token")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.data[token] = assocData{data: data, expiration: expirationTime}
	return token, nil
}

func (s *Store) Take(token string) (data interface{}, err error) {
	s.mtx.Lock()
	assocData, ok := s.data[token]
	delete(s.data, token)
	s.mtx.Unlock()

	if !ok {
		return nil, fmt.Errorf("unknown token")
	}

	// ExpirationScanInterval is the tolerance interval, no need to check again
	return assocData.data, nil
}

type StopExpirationFunc func()

// The interval (and resolution) at which token expiration is enforced.
var ExpirationScanInterval = 10 * time.Second


func NewWithRandomKey() (*Store, StopExpirationFunc, error) {
	var key [256/8]byte // 256 bit key TODO verify this is a sensible key length
	_, err := cryptorand.Read(key[:])
	if err != nil {
		return nil, nil, err
	}
	return New(key[:])
}

func New(key []byte) (*Store, StopExpirationFunc, error) {
	if len(key) == 0 { // FIXME more sensible min value?
		return nil, nil, fmt.Errorf("key length must be greater than 0")
	}
	store := Store{
		key: key,
		data: make(map[string]assocData),
	}

	stopExpirationChan := make(chan struct{})
	go func() {
		tick := time.NewTicker(ExpirationScanInterval)
		defer tick.Stop()
		for {
			select {
			case <-stopExpirationChan:
				return
			case <-tick.C:
			}

			func() {
				store.mtx.Lock()
				defer store.mtx.Unlock()
				for token, val := range store.data {
					if time.Now().After(val.expiration) {
						delete(store.data, token)
					}
					select {
					case <-stopExpirationChan:
						return
					default:
					}
				}
			}()

		}
	}()

	stopExpirationFunc := func() {
		close(stopExpirationChan)
	}

	return &store, stopExpirationFunc, nil
}

package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRPC(t *testing.T) {
	conf := testValidConfig(t, `
jobs:

- name: default
  type: pull
  connect:
    type: tcp
    address: "server1.foo.bar:8888"
  #rpc:
  #  timeout: 20s # different form default, should merge
  root_fs: "pool2/backup_servers"
  interval: 10m
  pruning:
    keep_sender:
      - type: not_replicated
    keep_receiver:
      - type: last_n
        count: 100

- name: pull_servers
  type: pull
  connect:
    type: tcp
    address: "server1.foo.bar:8888"
  rpc:
    send_call_idle_timeout: 20s # different form default, should merge
  root_fs: "pool2/backup_servers"
  interval: 10m
  pruning:
    keep_sender:
      - type: not_replicated
    keep_receiver:
      - type: last_n
        count: 100

- type: sink
  name: "laptop_sink"
  root_fs: "pool2/backup_laptops"
  serve:
    type: tcp
    listen: "192.168.122.189:8888"
    clients: {
	"10.23.42.23":"client1"
    }
  rpc:
    zfs_send_idle_timeout: 23s

- type: sink
  name: "other_sink"
  root_fs: "pool2/backup_laptops"
  serve:
    type: tcp
    listen: "192.168.122.189:8888"
    clients: {
	"10.23.42.23":"client1"
    }
  rpc:
    zfs_recv_idle_timeout: 42s

- type: sink
  name: "default_sink"
  root_fs: "pool2/backup_laptops"
  serve:
    type: tcp
    listen: "192.168.122.189:8888"
    clients: {
	"10.23.42.23":"client1"
    }
  #rpc:
  #  zfs_recv_idle_timeout: 42s

`)
	// default client
	assert.Equal(t, 10, conf.Jobs[0].Ret.(*PullJob).RPC.MaxIdleConns)
	assert.Equal(t, 10*time.Second, conf.Jobs[0].Ret.(*PullJob).RPC.IdleConnTimeout)
	assert.Equal(t, 60*time.Second, conf.Jobs[0].Ret.(*PullJob).RPC.RPCCallTimeout)
	assert.Equal(t, 10*time.Second, conf.Jobs[0].Ret.(*PullJob).RPC.SendCallIdleTimeout)
	assert.Equal(t, 10*time.Second, conf.Jobs[0].Ret.(*PullJob).RPC.RecvCallIdleTimeout)

	// individual overrides work
	assert.Equal(t, 20*time.Second, conf.Jobs[1].Ret.(*PullJob).RPC.SendCallIdleTimeout)

	assert.Equal(t, 23*time.Second, conf.Jobs[2].Ret.(*SinkJob).RPC.ZFSSendIdleTimeout)
	assert.Equal(t, 42*time.Second, conf.Jobs[3].Ret.(*SinkJob).RPC.ZFSReceiveIdleTimeout)

	// default server
	assert.Equal(t, 10*time.Second, conf.Jobs[4].Ret.(*SinkJob).RPC.ZFSSendIdleTimeout)
	assert.Equal(t, 10*time.Second, conf.Jobs[4].Ret.(*SinkJob).RPC.ZFSReceiveIdleTimeout)
}

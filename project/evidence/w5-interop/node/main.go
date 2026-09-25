// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

// Command node runs one Mori member for the mixed-version interop check.
// The same source builds against Mori v0.7.0 (go-msgpack codec, tag
// moriold) and against the current tree (owned codec). It prints one JSON
// line per second with the members it sees and the user messages it got.
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/0xCarbon/mori"
)

type delegate struct {
	mu    sync.Mutex
	meta  []byte
	msgs  []string
	queue *mori.TransmitLimitedQueue
}

// broadcast is a user gossip message, retransmitted by the queue.
type broadcast []byte

func (b broadcast) Invalidates(mori.Broadcast) bool { return false }
func (b broadcast) Message() []byte                 { return b }
func (b broadcast) Finished()                       {}

func (d *delegate) NodeMeta(limit int) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.meta
}

func (d *delegate) NotifyMsg(b []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.msgs = append(d.msgs, string(b))
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.queue.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(join bool) []byte           { return []byte("state") }
func (d *delegate) MergeRemoteState(buf []byte, join bool) {}

type report struct {
	Node    string            `json:"node"`
	Members map[string]string `json:"members"` // name -> meta
	Msgs    []string          `json:"msgs"`
}

func main() {
	name := flag.String("name", "", "node name")
	bind := flag.String("bind", "127.0.0.1", "bind address")
	port := flag.Int("port", 0, "bind port")
	join := flag.String("join", "", "comma-separated peers")
	secret := flag.String("secret", "", "hex AES key")
	label := flag.String("label", "", "cluster label")
	compress := flag.Bool("compress", true, "enable compression")
	updateAt := flag.Duration("update-at", 0, "update node meta after this long")
	sendAt := flag.Duration("send-at", 0, "send user messages after this long")
	run := flag.Duration("run", 6*time.Second, "run time")
	flag.Parse()

	d := &delegate{meta: []byte("meta-" + *name), queue: &mori.TransmitLimitedQueue{RetransmitMult: 4}}
	c := mori.DefaultLocalConfig()
	c.Name = *name
	c.BindAddr = *bind
	c.BindPort = *port
	c.AdvertisePort = *port
	c.Label = *label
	c.EnableCompression = *compress
	c.Delegate = d
	c.PushPullInterval = time.Second
	quiet(c)
	if *secret != "" {
		key, err := hex.DecodeString(*secret)
		if err != nil {
			panic(err)
		}
		c.SecretKey = key
	}
	m, err := mori.Create(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(1)
	}
	d.queue.NumNodes = m.NumMembers
	if *join != "" {
		deadline := time.Now().Add(3 * time.Second)
		for {
			if _, err := m.Join(strings.Split(*join, ",")); err == nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	start := time.Now()
	updated, sent := false, false
	for time.Since(start) < *run {
		time.Sleep(250 * time.Millisecond)
		if *updateAt > 0 && !updated && time.Since(start) > *updateAt {
			updated = true
			d.mu.Lock()
			d.meta = []byte("updated-" + *name)
			d.mu.Unlock()
			_ = m.UpdateNode(time.Second)
		}
		if *sendAt > 0 && !sent && time.Since(start) > *sendAt {
			sent = true
			for _, n := range m.Members() {
				if n.Name == *name {
					continue
				}
				_ = m.SendReliable(n, []byte("reliable:"+*name+"->"+n.Name))
				_ = m.SendBestEffort(n, []byte("besteffort:"+*name+"->"+n.Name))
			}
			d.queue.QueueBroadcast(broadcast("gossip:" + *name))
		}
	}
	r := report{Node: *name, Members: map[string]string{}}
	for _, n := range m.Members() {
		r.Members[n.Name] = string(n.Meta)
	}
	d.mu.Lock()
	r.Msgs = slices.Clone(d.msgs)
	d.mu.Unlock()
	slices.Sort(r.Msgs)
	out, _ := json.Marshal(r)
	fmt.Println(string(out))
	// No Leave: peers still running must keep seeing this node until they
	// report. Shutdown alone leaves it alive in their view for seconds.
	_ = m.Shutdown()
}

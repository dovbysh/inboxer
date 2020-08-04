package subscriber

import (
	"fmt"
	"github.com/dovbysh/go-utils/testing/tlog"
	"github.com/dovbysh/inboxer/ievent"
	"github.com/dovbysh/tests_common/v3"
	"github.com/go-pg/pg/v9"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

const (
	Subscr1 = "subscr1"
	Subscr2 = "subscr2"
	Subscr3 = "subscr3"
)

func TestNewNatsSubscriber(t *testing.T) {
	ts := NewTStruct(t)
	defer ts.Close()
	ts.InitInbox()

	n := NewNatsSubscriber("_nats_inbox", ts.sc, ts.db)
	defer n.Close()
	err := n.Subscribe(Subscr1, func(ch <-chan uint64) {
		var c bool
		for id := range ch {
			if id > 0 {
				t.Log(id)
			} else {
				c = true
			}
		}
		assert.False(t, c)
	}, 2, 8)
	assert.NoError(t, err)
	err = n.Subscribe(Subscr1, func(ch <-chan uint64) {}, 0, 0)
	assert.Error(t, err)
	for i := 0; i < 20; i++ {
		ts.sc.Publish(Subscr1, []byte(fmt.Sprintf("test %d", i)))
	}

	n.Close()
}

type TStruct struct {
	sc      stan.Conn
	db      *pg.DB
	dbOpt   *pg.Options
	t       *testing.T
	closers []func()
}

func NewTStruct(t *testing.T) *TStruct {
	ts := TStruct{
		closers: make([]func(), 0),
		t:       t,
	}
	var wg sync.WaitGroup
	var pgCloser func()
	o, pgCloser, _, _ := tests_common.PostgreSQLContainer(&wg)
	ts.closers = append(ts.closers, pgCloser)

	natsOpt, natsCloser, _, _ := tests_common.NatsStreamingContainer(&wg)
	ts.closers = append(ts.closers, natsCloser)

	wg.Wait()

	ts.dbOpt = &pg.Options{
		Addr:         o.Addr,
		User:         o.User,
		Password:     o.Password,
		Database:     o.Database,
		PoolSize:     o.PoolSize,
		MinIdleConns: o.MinIdleConns,
	}

	opts := []nats.Option{nats.Name("NATS Streaming inboxer test")}
	nc, err := nats.Connect(natsOpt.Url, opts...)
	if err != nil {
		panic(err)
	}
	ts.closers = append(ts.closers, nc.Close)
	ts.sc, err = stan.Connect(natsOpt.ClusterId, "inboxer-test-ClientId", stan.NatsConn(nc))
	if err != nil {
		panic(err)
	}
	ts.closers = append(ts.closers, func() { ts.sc.Close() })

	ts.db = pg.Connect(ts.dbOpt)
	ts.db.AddQueryHook(tlog.NewShowQuery(ts.t))

	return &ts
}

func (ts *TStruct) Close() {
	for i := len(ts.closers) - 1; i >= 0; i-- {
		ts.closers[i]()
	}
}

func (ts *TStruct) InitInbox() {
	ts.db.Model((*ievent.Inbox)(nil)).Table("_nats_inbox").CreateTable(nil)
}

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
	"runtime"
	"sync"
	"testing"
	"time"
)

const (
	subject1   = "subject1"
	subject2   = "subject2"
	subject3   = "subject3"
	inboxTable = "_stan_inbox"
	numMessage = 20
)

func TestNewNatsSubscriber(t *testing.T) {
	ts := NewTStruct(t)
	defer ts.Close()
	ts.InitInbox()

	n := NewNatsSubscriber(inboxTable, ts.sc, ts.db)
	defer n.Close()
	err := n.Subscribe(subject1, func(ch <-chan uint64) {
		var c bool
		for id := range ch {
			if id > 0 {
				t.Log(id)
			} else {
				c = true
			}
		}
		assert.False(t, c)
	}, 2, 8, nil)
	assert.NoError(t, err)
	err = n.Subscribe(subject1, func(ch <-chan uint64) {}, 0, 0, nil)
	assert.Error(t, err)
	for i := 0; i < numMessage; i++ {
		ts.sc.Publish(subject1, []byte(fmt.Sprintf("test %d", i)))
	}

	n.Close()

	n2 := NewNatsSubscriber("_table_does_not_exist", ts.sc, ts.db)
	defer n2.Close()
	var sc2fired bool
	err = n2.Subscribe(subject2, func(ch <-chan uint64) {}, 0, 0, func(err error) {
		assert.Error(t, err)
		sc2fired = true
	})
	assert.NoError(t, err)
	for i := 0; i < numMessage; i++ {
		ts.sc.Publish(subject2, []byte(fmt.Sprintf("test %d", i)))
	}
	runtime.Gosched()
	assert.True(t, sc2fired)
}

func TestReSendSubscriptionEvents(t *testing.T) {
	ts := NewTStruct(t)
	defer ts.Close()
	ts.InitInbox()

	n3 := NewNatsSubscriber(inboxTable, ts.sc, ts.db)
	defer n3.Close()
	var wg sync.WaitGroup
	err := n3.Subscribe(subject1, func(ch <-chan uint64) {
		for id := range ch {
			r, err := ts.db.
				Model((*ievent.Inbox)(nil)).
				Table(inboxTable).
				Set("proceed = true").
				Set("proceed_at = ?", time.Now()).
				Where("proceed = false and id = ?", id).
				Update()
			wg.Done()
			assert.NoError(t, err)
			assert.Equal(t, 1, r.RowsAffected(), "id=", id)
		}
	}, 2, 8, nil)
	assert.NoError(t, err)
	err = n3.Subscribe(subject3, func(ch <-chan uint64) {
		for id := range ch {
			t.Log(subject3, id)
			r, err := ts.db.
				Model((*ievent.Inbox)(nil)).
				Table(inboxTable).
				Set("proceed = true").
				Set("proceed_at = ?", time.Now()).
				Where("proceed = false and id = ?", id).
				Update()
			wg.Done()
			assert.NoError(t, err)
			assert.Equal(t, 1, r.RowsAffected(), "id = ", id)
		}
	}, 2, 8, nil)
	assert.NoError(t, err)
	for i := 0; i < numMessage; i++ {
		wg.Add(1)
		ts.sc.Publish(subject3, []byte(fmt.Sprintf("test %d", i)))

		ev := ievent.Inbox{
			Proceed:   false,
			CreatedAt: time.Now(),
			Subject:   subject1,
			Data:      nil,
			Sequence:  1000 + uint64(i),
			Timestamp: time.Now(),
		}
		r, err := ts.db.Model(&ev).Table(inboxTable).Insert()
		assert.NoError(t, err)
		assert.Equal(t, 1, r.RowsAffected())
	}
	wg.Wait()

	wg.Add(numMessage)
	assert.NoError(t, n3.ReSendSubscriptionEvents())
	wg.Wait()
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
	ts.db.Model((*ievent.Inbox)(nil)).Table(inboxTable).CreateTable(nil)
}

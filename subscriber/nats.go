package subscriber

import (
	"fmt"
	"github.com/dovbysh/inboxer/ievent"
	"github.com/go-pg/pg/v9"
	"github.com/nats-io/stan.go"
	"sync"
	"time"
)

type NatsSubscriber struct {
	channelMap      map[string]chan uint64
	channelMutexMap map[string]*sync.Mutex
	subscrMap       map[string]stan.Subscription
	subMutex        sync.Mutex
	tableName       string
	sc              stan.Conn
	db              *pg.DB
}

func NewNatsSubscriber(tableName string, sc stan.Conn, db *pg.DB) *NatsSubscriber {
	s := NatsSubscriber{
		tableName:       tableName,
		sc:              sc,
		db:              db,
		channelMap:      make(map[string]chan uint64),
		channelMutexMap: make(map[string]*sync.Mutex),
		subscrMap:       make(map[string]stan.Subscription),
	}

	return &s
}

type Subscriber func(ch <-chan uint64)

var ErrOnlyOneSubscriberAllowed = fmt.Errorf("only one subscriber allowed")

func (n *NatsSubscriber) Subscribe(subject string, f Subscriber, threads uint64, chanLength uint64) error {
	n.subMutex.Lock()
	defer n.subMutex.Unlock()
	_, exists := n.channelMap[subject]
	if exists {
		return fmt.Errorf("%w, subject: %s", ErrOnlyOneSubscriberAllowed, subject)
	}
	n.channelMutexMap[subject] = &sync.Mutex{}
	m, _ := n.channelMutexMap[subject]
	m.Lock()
	defer m.Unlock()

	n.channelMap[subject] = make(chan uint64, chanLength)
	for i := uint64(0); i < threads; i++ {
		c := n.channelMap[subject]
		go f(c)
	}
	subscription, err := n.sc.Subscribe(subject, func(msg *stan.Msg) {
		i := ievent.Inbox{
			CreatedAt: time.Now(),
			Subject:   msg.Subject,
			Data:      msg.Data,
			Sequence:  msg.Sequence,
			Timestamp: time.Unix(0, msg.Timestamp),
		}
		err := n.db.RunInTransaction(func(tx *pg.Tx) error {
			r, err := tx.Model(&i).Table(n.tableName).Returning("*").Insert(&i)
			if err != nil {
				return err
			}
			if r.RowsAffected() != 1 {
				return fmt.Errorf("not 1 row affected")
			}
			return nil
		})
		if err == nil {
			msg.Ack()
			m, ex := n.channelMutexMap[msg.Subject]
			if ex {
				m.Lock()
				defer m.Unlock()
				ch, ok := n.channelMap[msg.Subject]
				if ok {
					ch <- i.ID
				}
			}
		}
		if err != nil {
			pgErr, ok := err.(pg.Error)
			if ok && pgErr.IntegrityViolation() {
				msg.Ack()
			}
		}
	},
		stan.SetManualAckMode(),
		stan.DeliverAllAvailable(),
	)

	if err != nil {
		return err
	}
	n.subscrMap[subject] = subscription

	return nil
}

func (n *NatsSubscriber) Close() error {
	n.subMutex.Lock()
	defer n.subMutex.Unlock()
	for subject, subscription := range n.subscrMap {
		err := subscription.Close()
		if err != nil {
			return fmt.Errorf("%w subject: %s", err, subject)
		}
		delete(n.subscrMap, subject)
	}

	for subject, ch := range n.channelMap {
		m, _ := n.channelMutexMap[subject]
		m.Lock()
		defer m.Unlock()
		delete(n.channelMap, subject)
		delete(n.channelMutexMap, subject)
		close(ch)
	}
	return nil
}

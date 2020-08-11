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
	subscriptionMap map[string]stan.Subscription
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
		subscriptionMap: make(map[string]stan.Subscription),
	}

	return &s
}

type Subscriber func(ch <-chan uint64)

var ErrOnlyOneSubscriberAllowed = fmt.Errorf("only one subscriber allowed")

func (n *NatsSubscriber) Subscribe(subject string, f Subscriber, threads uint64, chanLength uint64, ef func(error)) error {
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
			errorFunc(msg.Ack(), ef)
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
				errorFunc(msg.Ack(), ef)
			}
			errorFunc(err, ef)
		}
	},
		stan.SetManualAckMode(),
		stan.DeliverAllAvailable(),
	)

	if err != nil {
		return err
	}
	n.subscriptionMap[subject] = subscription

	return nil
}
func (n *NatsSubscriber) ReSendSubscriptionEvents() error {
	if len(n.subscriptionMap) == 0 {
		return fmt.Errorf("no subscribers")
	}
	n.subMutex.Lock()
	defer n.subMutex.Unlock()

	subjects := make([]string, 0, len(n.subscriptionMap))
	for subject := range n.subscriptionMap {
		subjects = append(subjects, subject)
	}
	var evs []ievent.Inbox
	var lastId uint64
	locked := make(map[string]bool)
	for {
		if err := n.db.
			Model(&evs).
			Table(n.tableName).
			Column("id", "subject").
			Where("proceed = false").
			Where("id > ?", lastId).
			WhereIn("subject in (?)", subjects).
			Order("id", "sequence", "created_at", "timestamp").
			Limit(1000).
			Select(); err != nil {
			return err
		}
		if len(evs) == 0 {
			break
		}
		for _, ev := range evs {
			lastId = ev.ID
			if !locked[ev.Subject] {
				s := ev.Subject
				n.channelMutexMap[s].Lock()
				defer n.channelMutexMap[s].Unlock()
				locked[ev.Subject] = true
			}
			n.channelMap[ev.Subject] <- ev.ID
		}
	}

	return nil
}
func (n *NatsSubscriber) Close() error {
	n.subMutex.Lock()
	defer n.subMutex.Unlock()
	for subject, subscription := range n.subscriptionMap {
		err := subscription.Close()
		if err != nil {
			return fmt.Errorf("%w subject: %s", err, subject)
		}
		delete(n.subscriptionMap, subject)
	}

	for subject, ch := range n.channelMap {
		m, _ := n.channelMutexMap[subject]
		m.Lock()
		delete(n.channelMap, subject)
		delete(n.channelMutexMap, subject)
		close(ch)

		m.Unlock()
	}
	return nil
}

func errorFunc(err error, ef func(error)) {
	if err != nil && ef != nil {
		ef(err)
	}
}

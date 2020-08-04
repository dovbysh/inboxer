package ievent

import "time"

type Inbox struct {
	tableName struct{}  `pg:"_"`
	ID        uint64    `pg:",pk"`
	Proceed   bool      `pg:",use_zero,notnull,default:false"`
	CreatedAt time.Time `pg:",use_zero,notnull,default:now()"`
	ProceedAt time.Time
	Subject   string
	Data      []byte
	Sequence  uint64    `pg:",unique:stan_pk"`
	Timestamp time.Time `pg:",unique:stan_pk"`
}

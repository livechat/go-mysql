package mysql

import "time"

type relay struct {
	relayChan chan bool
}

func (t *relay) setRelaySize(size int) {
	t.relayChan = make(chan bool, size)
}

func (t *relay) start() time.Duration {
	s := time.Now()
	t.relayChan <- true
	return time.Now().Sub(s)
}

func (t *relay) end() {
	select {
	case <-t.relayChan:
	default:
	}
}

func (t *relay) conditionalEnd(err *error) {
	if err != nil {
		t.end()
	}
}

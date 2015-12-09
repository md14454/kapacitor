package kapacitor

import (
	"log"
	"sync"

	"github.com/influxdata/kapacitor/pipeline"
)

// User defined function
type UDFNode struct {
	node
	u       *pipeline.UDFNode
	process *UDFProcess
	aborted chan struct{}

	mu      sync.Mutex
	stopped bool
}

// Create a new UDFNode that sends incoming data to child process
func newUDFNode(et *ExecutingTask, n *pipeline.UDFNode, l *log.Logger) (*UDFNode, error) {
	un := &UDFNode{
		node:    node{Node: n, et: et, logger: l},
		u:       n,
		aborted: make(chan struct{}),
	}
	un.process = NewUDFProcess(
		n.Cmd,
		l,
		n.Timeout,
		un.abortedCallback,
	)

	un.node.runF = un.runUDF
	un.node.stopF = un.stopUDF
	return un, nil
}

func (u *UDFNode) stopUDF() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !u.stopped {
		u.process.Abort()
	}
}

func (u *UDFNode) runUDF() error {
	u.process.Start()
	u.process.Init(u.Wants(), u.Provides(), u.u.Options)
	forwardErr := make(chan error, 1)
	go func() {
		switch u.Provides() {
		case pipeline.StreamEdge:
			for p := range u.process.PointOut {
				for _, out := range u.outs {
					err := out.CollectPoint(p)
					if err != nil {
						forwardErr <- err
						return
					}
				}
			}
		case pipeline.BatchEdge:
			for b := range u.process.BatchOut {
				for _, out := range u.outs {
					err := out.CollectBatch(b)
					if err != nil {
						forwardErr <- err
						return
					}
				}
			}
		}
		forwardErr <- nil
	}()

	switch u.Wants() {
	case pipeline.StreamEdge:
	STREAM:
		for p, ok := u.ins[0].NextPoint(); ok; p, ok = u.ins[0].NextPoint() {
			select {
			case u.process.PointIn <- p:
			case <-u.aborted:
				break STREAM
			}
		}
		u.logger.Println("I! closing PointIn")
		close(u.process.PointIn)
	case pipeline.BatchEdge:
	BATCH:
		for b, ok := u.ins[0].NextBatch(); ok; b, ok = u.ins[0].NextBatch() {
			select {
			case u.process.BatchIn <- b:
			case <-u.aborted:
				break BATCH
			}
		}
		close(u.process.BatchIn)
	}
	// Stop the process
	u.mu.Lock()
	defer u.mu.Unlock()
	err := u.process.Stop()
	u.stopped = true
	if err != nil {
		return err
	}
	// Wait/Return any error from the forwarding gorouting
	return <-forwardErr
}

func (u *UDFNode) abortedCallback() {
	close(u.aborted)
}

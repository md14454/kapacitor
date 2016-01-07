package kapacitor

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"sync"

	"github.com/influxdata/kapacitor/pipeline"
)

// A node that can be  in an executor.
type Node interface {
	pipeline.Node

	addParentEdge(*Edge)

	// start the node and its children
	start()
	stop()

	// wait for the node to finish processing and return any errors
	Err() error

	// link specified child
	linkChild(c Node) error
	addParent(p Node)

	// close children edges
	closeChildEdges()
	// abort parent edges
	abortParentEdges()

	// executing dot
	edot(buf *bytes.Buffer)
}

//implementation of Node
type node struct {
	pipeline.Node
	et         *ExecutingTask
	parents    []Node
	children   []Node
	runF       func() error
	stopF      func()
	errCh      chan error
	err        error
	finishedMu sync.Mutex
	finished   bool
	ins        []*Edge
	outs       []*Edge
	logger     *log.Logger
}

func (n *node) addParentEdge(e *Edge) {
	n.ins = append(n.ins, e)
}

func (n *node) abortParentEdges() {
	for _, in := range n.ins {
		in.Abort()
	}
}

func (n *node) start() {
	n.errCh = make(chan error, 1)
	go func() {
		var err error
		defer func() {
			// Always close children edges
			n.closeChildEdges()
			// Propogate error up
			if err != nil {
				// Handle panic in runF
				r := recover()
				if r != nil {
					trace := make([]byte, 512)
					n := runtime.Stack(trace, false)
					err = fmt.Errorf("%s: Trace:%s", r, string(trace[:n]))
				}
				n.abortParentEdges()
				n.logger.Println("E!", err)
			}
			n.errCh <- err
		}()
		// Run node
		err = n.runF()
	}()
}

func (n *node) stop() {
	if n.stopF != nil {
		n.stopF()
	}

}

func (n *node) Err() error {
	n.finishedMu.Lock()
	defer n.finishedMu.Unlock()
	if !n.finished {
		n.finished = true
		n.err = <-n.errCh
	}
	return n.err
}

func (n *node) addChild(c Node) (*Edge, error) {
	if n.Provides() != c.Wants() {
		return nil, fmt.Errorf("cannot add child mismatched edges: %s -> %s", n.Provides(), c.Wants())
	}
	n.children = append(n.children, c)

	edge := newEdge(n.et.Task.Name, n.Name(), c.Name(), n.Provides(), n.et.tm.LogService)
	if edge == nil {
		return nil, fmt.Errorf("unknown edge type %s", n.Provides())
	}
	c.addParentEdge(edge)
	return edge, nil
}

func (n *node) addParent(p Node) {
	n.parents = append(n.parents, p)
}

func (n *node) linkChild(c Node) error {

	// add child
	edge, err := n.addChild(c)
	if err != nil {
		return err
	}

	// add parent
	c.addParent(n)

	// store edge to child
	n.outs = append(n.outs, edge)
	return nil
}

func (n *node) closeChildEdges() {
	for _, child := range n.outs {
		child.Close()
	}
}

func (n *node) edot(buf *bytes.Buffer) {
	for i, c := range n.children {
		buf.Write([]byte(
			fmt.Sprintf("%s -> %s [label=\"%s\"];\n",
				n.Name(),
				c.Name(),
				n.outs[i].collectedCount(),
			),
		))
	}
}

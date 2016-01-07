package kapacitor

import (
	"log"

	"github.com/influxdata/kapacitor/pipeline"
)

type UnionNode struct {
	node
	u *pipeline.UnionNode
}

// Create a new  UnionNode which combines all parent data streams into a single stream.
// No transformation of any kind is performed.
func newUnionNode(et *ExecutingTask, n *pipeline.UnionNode, l *log.Logger) (*UnionNode, error) {
	un := &UnionNode{
		u:    n,
		node: node{Node: n, et: et, logger: l},
	}
	un.node.runF = un.runUnion
	return un, nil
}

func (u *UnionNode) runUnion() error {
	rename := u.u.Rename
	if rename == "" {
		//the calling node is always the last node
		rename = u.parents[len(u.parents)-1].Name()
	}
	errors := make(chan error, len(u.ins))
	for _, in := range u.ins {
		go func(e *Edge) {
			switch u.Wants() {
			case pipeline.StreamEdge:
				for p, ok := e.NextPoint(); ok; p, ok = e.NextPoint() {
					p.Name = rename
					for _, out := range u.outs {
						err := out.CollectPoint(p)
						if err != nil {
							errors <- err
							return
						}
					}
				}
			case pipeline.BatchEdge:
				for b, ok := e.NextBatch(); ok; b, ok = e.NextBatch() {
					b.Name = rename
					for _, out := range u.outs {
						err := out.CollectBatch(b)
						if err != nil {
							errors <- err
							return
						}
					}
				}
			}
			errors <- nil
		}(in)
	}

	for range u.ins {
		err := <-errors
		if err != nil {
			return err
		}
	}
	return nil
}

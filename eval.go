package kapacitor

import (
	"errors"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick"
)

type EvalNode struct {
	node
	e           *pipeline.EvalNode
	expressions []*tick.StatefulExpr
}

// Create a new  ApplyNode which applies a transformation func to each point in a stream and returns a single point.
func newApplyNode(et *ExecutingTask, n *pipeline.EvalNode) (*EvalNode, error) {
	if len(n.AsList) != len(n.Expressions) {
		return nil, errors.New("must provide one name per expression via the 'As' property")
	}
	en := &EvalNode{
		node: node{Node: n, et: et},
		e:    n,
	}
	// Create stateful expressions
	en.expressions = make([]*tick.StatefulExpr, len(n.Expressions))
	for i, expr := range n.Expressions {
		en.expressions[i] = tick.NewStatefulExpr(expr)
	}

	en.node.runF = en.runApply
	return en, nil
}

func (e *EvalNode) runApply() error {
	switch e.Provides() {
	case pipeline.StreamEdge:
		for p, ok := e.ins[0].NextPoint(); ok; p, ok = e.ins[0].NextPoint() {
			fields, err := e.eval(p.Fields, p.Tags)
			if err != nil {
				return err
			}
			p.Fields = fields
			for _, child := range e.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := e.ins[0].NextBatch(); ok; b, ok = e.ins[0].NextBatch() {
			for i, p := range b.Points {
				fields, err := e.eval(p.Fields, p.Tags)
				if err != nil {
					return err
				}
				b.Points[i].Fields = fields
			}
			for _, child := range e.outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (e *EvalNode) eval(fields models.Fields, tags map[string]string) (models.Fields, error) {
	vars, err := mergeFieldsAndTags(fields, tags)
	if err != nil {
		return nil, err
	}
	for i, expr := range e.expressions {
		v, err := expr.EvalNum(vars)
		if err != nil {
			return nil, err
		}
		name := e.e.AsList[i]
		vars.Set(name, v)
	}
	var newFields models.Fields
	if e.e.KeepFlag {
		if l := len(e.e.KeepList); l != 0 {
			newFields = make(models.Fields, l)
			for _, f := range e.e.KeepList {
				newFields[f], err = vars.Get(f)
				if err != nil {
					return nil, err
				}
			}
		} else {
			newFields = make(models.Fields, len(fields)+len(e.e.AsList))
			for f, v := range fields {
				newFields[f] = v
			}
			for _, f := range e.e.AsList {
				newFields[f], err = vars.Get(f)
				if err != nil {
					return nil, err
				}
			}
		}
	} else {
		newFields = make(models.Fields, len(e.e.AsList))
		for _, f := range e.e.AsList {
			newFields[f], err = vars.Get(f)
			if err != nil {
				return nil, err
			}
		}
	}
	return newFields, nil
}

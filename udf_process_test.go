package kapacitor_test

import (
	"bytes"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/kapacitor"
	cmd_test "github.com/influxdata/kapacitor/command/test"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/udf"
)

func TestMessage_ReadWrite(t *testing.T) {
	req := &udf.Request{}
	req.Message = &udf.Request_Keepalive{
		Keepalive: &udf.KeepAliveRequest{
			Time: 42,
		},
	}

	var buf bytes.Buffer

	err := udf.WriteMessage(req, &buf)
	if err != nil {
		t.Fatal(err)
	}

	nreq := &udf.Request{}
	var b []byte
	err = udf.ReadMessage(&b, &buf, nreq)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(req, nreq) {
		t.Errorf("unexpected request: \ngot %v\nexp %v", nreq, req)
	}
}

func TestMessage_ReadWriteMultiple(t *testing.T) {
	req := &udf.Request{}
	req.Message = &udf.Request_Keepalive{
		Keepalive: &udf.KeepAliveRequest{
			Time: 42,
		},
	}

	var buf bytes.Buffer

	var count int = 1e4
	for i := 0; i < count; i++ {
		err := udf.WriteMessage(req, &buf)
		if err != nil {
			t.Fatal(err)
		}
	}

	nreq := &udf.Request{}
	var b []byte

	for i := 0; i < count; i++ {
		err := udf.ReadMessage(&b, &buf, nreq)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(req, nreq) {
			t.Fatalf("unexpected request: i:%d \ngot %v\nexp %v", i, nreq, req)
		}
	}
}

func TestProcess_StartStop(t *testing.T) {
	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_Start] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, 0, nil)
	p.Start()
	p.Init(pipeline.StreamEdge, pipeline.StreamEdge, nil)
	req := <-cmd.Requests
	_, ok := req.Message.(*udf.Request_Init)
	if !ok {
		t.Error("expected init message")
	}

	close(p.PointIn)
	p.Stop()
	// read all requests and wait till the chan is closed
	for range cmd.Requests {
	}
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}

func TestProcess_KeepAlive(t *testing.T) {
	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_KeepAlive] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, time.Millisecond*10, nil)
	p.Start()
	p.Init(pipeline.StreamEdge, pipeline.StreamEdge, nil)
	req := <-cmd.Requests
	_, ok := req.Message.(*udf.Request_Init)
	if !ok {
		t.Error("expected init message")
	}
	req = <-cmd.Requests
	_, ok = req.Message.(*udf.Request_Keepalive)
	if !ok {
		t.Error("expected keepalive message")
	}

	close(p.PointIn)
	p.Stop()
	// read all requests and wait till the chan is closed
	for range cmd.Requests {
	}
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}

func TestProcess_MissedKeepAlive(t *testing.T) {
	abortCalled := make(chan struct{})
	aborted := func() {
		close(abortCalled)
	}

	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_MissedKeepAlive] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, 2, aborted)
	p.Start()
	p.Init(pipeline.StreamEdge, pipeline.StreamEdge, nil)
	req := <-cmd.Requests
	_, ok := req.Message.(*udf.Request_Init)
	if !ok {
		t.Error("expected init message")
	}

	// Since the keepalive is missed, the process should abort on its own.
	for range cmd.Requests {
	}

	select {
	case <-abortCalled:
	case <-time.After(time.Millisecond * 20):
		t.Error("expected abort callback to be called")
	}
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}

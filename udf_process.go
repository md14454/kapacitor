package kapacitor

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/udf"
)

var ErrUDFProcessAborted = errors.New("process aborted")

type byteReadCloser struct {
	*bufio.Reader
	io.Closer
}

// Wraps an external process and sends and receives data
// over STDIN and STDOUT. Lines received over STDERR are logged
// via normal Kapacitor logging.
//
// Once a UDFProcess is created and started the owner can send points or batches
// to the subprocess by writing them to the PointIn or BatchIn channels respectively,
// and according to the type of process created.
//
// The UDFProcess may be Aborted at anytime for various reasons. It is the owner's responsibility
// via the abortCallback to stop writing to the *In channels since no more selects on the channels
// will be performed.
//
// Calling Stop on the process should only be done once the owner has closed the *In channel,
// at which point the remaining data will be processed and the subprocess will be allowed to exit cleanly.
type UDFProcess struct {
	// If the processes is Aborted (via KeepAlive timeout, etc.)
	// then no more data will be read off the *In channels.
	//
	// Optional callback if the process aborts.
	// It is the owners response
	abortCallback func()
	abortOnce     sync.Once

	pointIn chan models.Point
	PointIn chan<- models.Point
	batchIn chan models.Batch
	BatchIn chan<- models.Batch

	pointOut chan models.Point
	PointOut <-chan models.Point
	batchOut chan models.Batch
	BatchOut <-chan models.Batch

	closing  chan struct{}
	aborting chan struct{}

	keepalives       chan *udf.Request
	keepalive        chan int64
	keepaliveTimeout time.Duration

	cmd    command.Command
	stdin  io.WriteCloser
	stdout udf.ByteReadCloser
	stderr io.ReadCloser

	readErr  chan error
	writeErr chan error
	err      error

	mu     sync.Mutex
	logger *log.Logger
	wg     sync.WaitGroup

	responseBuf []byte
	response    *udf.Response

	batch *models.Batch
}

func NewUDFProcess(
	cmd command.Command,
	l *log.Logger,
	timeout time.Duration,
	abortCallback func(),
) *UDFProcess {
	p := &UDFProcess{
		cmd:              cmd,
		logger:           l,
		keepalives:       make(chan *udf.Request),
		keepalive:        make(chan int64, 1),
		keepaliveTimeout: timeout,
		abortCallback:    abortCallback,
		response:         &udf.Response{},
	}
	return p
}

// Start the UDFProcess
func (p *UDFProcess) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closing = make(chan struct{})
	p.aborting = make(chan struct{})
	p.writeErr = make(chan error, 1)
	p.readErr = make(chan error, 1)

	stdin, err := p.cmd.StdinPipe()
	if err != nil {
		return err
	}
	p.stdin = stdin

	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		return err
	}
	brc := byteReadCloser{
		bufio.NewReader(stdout),
		stdout,
	}
	p.stdout = brc

	stderr, err := p.cmd.StderrPipe()
	if err != nil {
		return err
	}
	p.stderr = stderr

	err = p.cmd.Start()
	if err != nil {
		return err
	}

	return nil
}

func (p *UDFProcess) Init(wants, provides pipeline.EdgeType, options []*udf.Option) error {
	// Create the appropriate channels
	switch wants {
	case pipeline.StreamEdge:
		p.pointIn = make(chan models.Point)
		p.PointIn = p.pointIn
	case pipeline.BatchEdge:
		p.batchIn = make(chan models.Batch)
		p.BatchIn = p.batchIn
	}
	switch provides {
	case pipeline.StreamEdge:
		p.pointOut = make(chan models.Point)
		p.PointOut = p.pointOut
	case pipeline.BatchEdge:
		p.batchOut = make(chan models.Batch)
		p.BatchOut = p.batchOut
	}

	err := p.initUDFProcess(options)
	if err != nil {
		return err
	}

	go func() {
		p.writeErr <- p.writeData()
	}()
	go func() {
		p.readErr <- p.readData()
	}()

	p.wg.Add(3)
	go p.runKeepAlive()
	go p.watchKeepAlive()
	go p.logStdErr()
	return nil
}

// Abort the process.
// Data in-flight will not be processed.
func (p *UDFProcess) Abort() {
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.aborting)
	p.stop()
	if p.abortCallback != nil {
		p.abortOnce.Do(p.abortCallback)
	}
}

// Stop the UDFProcess cleanly.
//
// Calling Stop should only be done once the owner has closed the *In channel,
// at which point the remaining data will be processed and the subprocess will be allowed to exit cleanly.
func (p *UDFProcess) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stop()
}

// internal stop function you must acquire the lock before calling
func (p *UDFProcess) stop() error {

	close(p.closing)
	p.logger.Println("D! closed closing")

	writeErr := <-p.writeErr
	readErr := <-p.readErr
	p.logger.Println("D! got read write errors")
	p.wg.Wait()
	p.logger.Println("D! wg waited")

	if p.cmd != nil {
		p.cmd.Wait()
		p.logger.Println("D! cmd waited")
	}

	if writeErr != nil {
		return writeErr
	}
	return readErr
}

type UDFProcessInfo struct {
	Cmd      command.Command
	Timeout  time.Duration
	Wants    pipeline.EdgeType
	Provides pipeline.EdgeType
	Options  map[string]*udf.OptionInfo
}

// Get information about the process,
// available options etc.
func (p *UDFProcess) Info() (UDFProcessInfo, error) {
	info := UDFProcessInfo{}
	req := &udf.Request{Message: &udf.Request_Info{
		Info: &udf.InfoRequest{},
	}}
	p.writeRequest(req)
	resp, err := p.readResponse()
	if err != nil {
		return info, err
	}
	ri, ok := resp.Message.(*udf.Response_Info)
	if !ok {
		return info, fmt.Errorf("unexpected response from process of type %T", resp.Message)
	}
	info.Cmd = p.cmd
	info.Timeout = p.keepaliveTimeout
	info.Options = ri.Info.Options

	switch ri.Info.Wants {
	case udf.EdgeType_STREAM:
		info.Wants = pipeline.StreamEdge
	case udf.EdgeType_BATCH:
		info.Wants = pipeline.BatchEdge
	}
	switch ri.Info.Provides {
	case udf.EdgeType_STREAM:
		info.Provides = pipeline.StreamEdge
	case udf.EdgeType_BATCH:
		info.Provides = pipeline.BatchEdge
	}

	return info, nil
}

func (p *UDFProcess) initUDFProcess(options []*udf.Option) error {
	req := &udf.Request{Message: &udf.Request_Init{
		Init: &udf.InitializeRequest{
			Options: options,
		},
	}}
	p.writeRequest(req)
	return nil
}

func (p *UDFProcess) runKeepAlive() {
	defer p.wg.Done()
	defer close(p.keepalives)
	if p.keepaliveTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(p.keepaliveTimeout / 2)
	for {
		select {
		case <-ticker.C:
			req := &udf.Request{Message: &udf.Request_Keepalive{
				Keepalive: &udf.KeepAliveRequest{
					Time: time.Now().UnixNano(),
				},
			}}
			p.keepalives <- req
		case <-p.closing:
			ticker.Stop()
			return
		}
	}
}
func (p *UDFProcess) watchKeepAlive() {
	// Defer functions are called LIFO.
	// We need to call p.Abort after p.wg.Done so we just set a flag
	aborted := false
	defer func() {
		if aborted {
			p.Abort()
		}
	}()
	defer p.wg.Done()
	if p.keepaliveTimeout <= 0 {
		return
	}
	last := time.Now().UnixNano()
	for {
		select {
		case last = <-p.keepalive:
		case <-time.After(p.keepaliveTimeout):
			p.logger.Println("E! keepalive timedout, last keepalive received was:", time.Unix(0, last))
			aborted = true
			return
		case <-p.closing:
			return
		}
	}
}

func (p *UDFProcess) writeData() error {
	defer p.stdin.Close()
	for {
		select {
		case pt, ok := <-p.pointIn:
			if ok {
				err := p.writePoint(pt)
				if err != nil {
					return err
				}
			} else {
				p.pointIn = nil
			}
		case bt, ok := <-p.batchIn:
			if ok {
				err := p.writeBatch(bt)
				if err != nil {
					return err
				}
			} else {
				p.batchIn = nil
			}
		case req, ok := <-p.keepalives:
			if ok {
				err := p.writeRequest(req)
				if err != nil {
					return err
				}
			} else {
				p.keepalives = nil
			}
		case <-p.aborting:
			return ErrUDFProcessAborted
		}
		if p.pointIn == nil && p.batchIn == nil && p.keepalives == nil {
			break
		}
	}
	return nil
}

func (p *UDFProcess) writePoint(pt models.Point) error {
	strs, floats, ints := p.fieldsToTypedMaps(pt.Fields)
	udfPoint := &udf.Point{
		Time:            pt.Time.UnixNano(),
		Name:            pt.Name,
		Database:        pt.Database,
		RetentionPolicy: pt.RetentionPolicy,
		Group:           string(pt.Group),
		Dimensions:      pt.Dimensions,
		Tags:            pt.Tags,
		FieldsDouble:    floats,
		FieldsInt:       ints,
		FieldsString:    strs,
	}
	req := &udf.Request{}
	req.Message = &udf.Request_Point{udfPoint}
	return p.writeRequest(req)
}

func (p *UDFProcess) fieldsToTypedMaps(fields models.Fields) (
	strs map[string]string,
	floats map[string]float64,
	ints map[string]int64,
) {
	for k, v := range fields {
		switch value := v.(type) {
		case string:
			if strs == nil {
				strs = make(map[string]string)
			}
			strs[k] = value
		case float64:
			if floats == nil {
				floats = make(map[string]float64)
			}
			floats[k] = value
		case int64:
			if ints == nil {
				ints = make(map[string]int64)
			}
			ints[k] = value
		default:
			panic("unsupported field value type")
		}
	}
	return
}

func (p *UDFProcess) typeMapsToFields(
	strs map[string]string,
	floats map[string]float64,
	ints map[string]int64,
) models.Fields {
	fields := make(models.Fields)
	for k, v := range strs {
		fields[k] = v
	}
	for k, v := range ints {
		fields[k] = v
	}
	for k, v := range floats {
		fields[k] = v
	}
	return fields
}

func (p *UDFProcess) writeBatch(b models.Batch) error {
	req := &udf.Request{}
	req.Message = &udf.Request_Begin{&udf.BeginBatch{}}
	err := p.writeRequest(req)
	if err != nil {
		return err
	}
	rp := &udf.Request_Point{}
	req.Message = rp
	for _, pt := range b.Points {
		strs, floats, ints := p.fieldsToTypedMaps(pt.Fields)
		udfPoint := &udf.Point{
			Time:         pt.Time.UnixNano(),
			Group:        string(b.Group),
			Tags:         pt.Tags,
			FieldsDouble: floats,
			FieldsInt:    ints,
			FieldsString: strs,
		}
		rp.Point = udfPoint
		err := p.writeRequest(req)
		if err != nil {
			return err
		}
	}

	req.Message = &udf.Request_End{
		&udf.EndBatch{
			Name: b.Name,
		},
	}
	return p.writeRequest(req)
}

func (p *UDFProcess) writeRequest(req *udf.Request) error {
	return udf.WriteMessage(req, p.stdin)
}

func (p *UDFProcess) readData() error {
	defer p.stdout.Close()
	defer func() {
		if p.pointOut != nil {
			close(p.pointOut)
		}
		if p.batchOut != nil {
			close(p.batchOut)
		}
	}()
	for {
		response, err := p.readResponse()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = p.handleResponse(response)
		if err != nil {
			return err
		}
	}
}

func (p *UDFProcess) readResponse() (*udf.Response, error) {
	err := udf.ReadMessage(&p.responseBuf, p.stdout, p.response)
	if err != nil {
		return nil, err
	}
	return p.response, nil
}

func (p *UDFProcess) handleResponse(response *udf.Response) error {
	// Always reset the keepalive timer since we received a response
	p.keepalive <- time.Now().UnixNano()
	switch msg := response.Message.(type) {
	case *udf.Response_Keepalive:
		// Noop we already reset the keepalive timer
	case *udf.Response_State:
	case *udf.Response_Restore:
	case *udf.Response_Error:
		p.logger.Println("E!", msg.Error.Error)
		return errors.New(msg.Error.Error)
	case *udf.Response_Begin:
		p.batch = &models.Batch{}
	case *udf.Response_Point:
		if p.batch != nil {
			pt := models.BatchPoint{
				Time: time.Unix(0, msg.Point.Time).UTC(),
				Tags: msg.Point.Tags,
				Fields: p.typeMapsToFields(
					msg.Point.FieldsString,
					msg.Point.FieldsDouble,
					msg.Point.FieldsInt,
				),
			}
			p.batch.Points = append(p.batch.Points, pt)
		} else {
			pt := models.Point{
				Time:            time.Unix(0, msg.Point.Time).UTC(),
				Name:            msg.Point.Name,
				Database:        msg.Point.Database,
				RetentionPolicy: msg.Point.RetentionPolicy,
				Group:           models.GroupID(msg.Point.Group),
				Dimensions:      msg.Point.Dimensions,
				Tags:            msg.Point.Tags,
				Fields: p.typeMapsToFields(
					msg.Point.FieldsString,
					msg.Point.FieldsDouble,
					msg.Point.FieldsInt,
				),
			}
			p.pointOut <- pt
		}
	case *udf.Response_End:
		p.batch.Name = msg.End.Name
		p.batch.TMax = time.Unix(0, msg.End.TMax)
		p.batch.Group = models.GroupID(msg.End.Group)
		p.batch.Tags = msg.End.Tags
		p.batchOut <- *p.batch
		p.batch = nil
	default:
		panic(fmt.Sprintf("unexpected response message %T", msg))
	}
	return nil
}

func (p *UDFProcess) logStdErr() {
	defer p.wg.Done()
	defer p.stderr.Close()
	scanner := bufio.NewScanner(p.stderr)
	for scanner.Scan() {
		p.logger.Println("E!", scanner.Text())
	}
}

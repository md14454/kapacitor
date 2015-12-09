package udf

import (
	"fmt"
	"log"
	"sync"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/command"
)

type Service struct {
	functionConfig map[string]FunctionConfig
	functions      map[string]kapacitor.UDFProcessInfo
	logger         *log.Logger
	mu             sync.RWMutex
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		functionConfig: c.Functions,
		logger:         l,
	}
}

func (s *Service) Open() error {
	for name := range s.functionConfig {
		err := s.RefreshFunction(name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) FunctionList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	functions := make([]string, 0, len(s.functions))
	for name := range s.functions {
		functions = append(functions, name)
	}
	return functions
}

func (s *Service) FunctionInfo(name string) (kapacitor.UDFProcessInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.functions[name]
	return info, ok
}

func (s *Service) RefreshFunction(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fc, ok := s.functionConfig[name]
	if ok {
		info, err := s.loadProcessInfo(fc)
		if err != nil {
			return err
		}
		s.functions[name] = info
	}
	return fmt.Errorf("no function %s configured", name)
}

func (s *Service) loadProcessInfo(f FunctionConfig) (kapacitor.UDFProcessInfo, error) {
	//TODO Implement user and group perms
	p := kapacitor.NewUDFProcess(command.NewCommand(f.Prog, f.Args...), s.logger, f.Timeout, nil)
	p.Start()
	defer p.Stop()
	return p.Info()
}

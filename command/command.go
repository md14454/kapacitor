package command

import (
	"io"
	"os/exec"
)

type Command interface {
	Start() error
	Wait() error

	StdinPipe() (io.WriteCloser, error)
	StdoutPipe() (io.ReadCloser, error)
	StderrPipe() (io.ReadCloser, error)
}

// Create a new Command using golang exec package.
func NewCommand(prog string, args ...string) Command {
	return exec.Command(prog, args...)
}

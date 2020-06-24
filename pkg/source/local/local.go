package local

import (
	"context"

	"github.com/boringsoftwarecompany/pipeline/pkg/pipeline"
)

// Client ...
type Client struct{}

// New ...
func New() *Client { return &Client{} }

// Source ...
func (c *Client) Source(ctx context.Context, sendc chan *pipeline.Message) (<-chan *pipeline.Message, <-chan error, error) {
	out := make(chan *pipeline.Message, 1000000)
	errc := make(chan error)

	go func() {
		defer close(out)
		defer close(errc)

		for {
			select {
			case msg := <-sendc:
				out <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, errc, nil
}

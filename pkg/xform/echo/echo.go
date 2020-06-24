package echo

import (
	"context"

	"github.com/boringsoftwarecompany/pipeline/pkg/pipeline"
)

// Client ...
type Client struct{}

// New ...
func New() *Client { return &Client{} }

// Transform ...
func (c *Client) Transform(ctx context.Context, msgc <-chan *pipeline.Message, xform pipeline.Xform) (<-chan *pipeline.Message, <-chan error, error) {
	resc, errc, err := c.transform(ctx, msgc, xform)
	if err != nil {
		return nil, nil, err
	}

	return resc, errc, err
}

// Transform ...
func (c *Client) transform(ctx context.Context, msgc <-chan *pipeline.Message, xform pipeline.Xform) (<-chan *pipeline.Message, <-chan error, error) {
	out := make(chan *pipeline.Message, 1000000)
	errc := make(chan error)

	go func() {
		defer close(out)
		defer close(errc)

		for {
			select {
			case msg := <-msgc:
				go callXform(ctx, msg, out, errc, xform)
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, errc, nil
}

func callXform(ctx context.Context, msg *pipeline.Message, msgc chan *pipeline.Message, errc chan error, xform pipeline.Xform) {
	res, err := xform(ctx, msg)
	if err != nil {
		errc <- err
	}

	msgc <- res
}

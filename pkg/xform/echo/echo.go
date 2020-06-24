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
	reqc, err := c.createTransformBuffer(ctx, msgc)
	if err != nil {
		return nil, nil, err
	}

	resc, errc, err := c.transform(ctx, reqc, xform)
	if err != nil {
		return nil, nil, err
	}

	return resc, errc, err
}

func (c *Client) createTransformBuffer(ctx context.Context, msgc <-chan *pipeline.Message) (<-chan *pipeline.Message, error) {
	reqc := make(chan *pipeline.Message)

	go func() {
		defer close(reqc)

		for msg := range msgc {
			select {
			case reqc <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	return reqc, nil
}

// Transform ...
func (c *Client) transform(ctx context.Context, msgc <-chan *pipeline.Message, xform pipeline.Xform) (<-chan *pipeline.Message, <-chan error, error) {
	out := make(chan *pipeline.Message)
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

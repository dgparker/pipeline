package pipeline

import (
	"context"
	"errors"
)

var (
	// ErrConduitNotFound ...
	ErrConduitNotFound = errors.New("conduit not found")
	// ErrXformExists ...
	ErrXformExists = errors.New("transformer with that name already exists")
	// ErrXformNotFound ...
	ErrXformNotFound = errors.New("transformer not found")
)

// Client ...
type Client struct {
	conduits map[string]*Conduit
	// might need to clean this dep up later
	// not sure what/where the sourcer,transformer,sinker interfaces should live
	sourcer Sourcer
	xformer Transformer
	sinker  Sinker
}

// Conduit ...
type Conduit struct {
	sendc   chan *Message
	srcc    <-chan *Message
	srcerrc <-chan error

	// might need to clean this dep up later
	xformer Transformer

	xformc    map[string]<-chan *Message
	xformerrc map[string]<-chan error

	// might need to clean this dep up later
	sinker Sinker

	sinkerrc <-chan error
}

// Message ...
type Message struct {
	Route string
	Data  []byte
}

// Sourcer ...
type Sourcer interface {
	Source(ctx context.Context, sendc chan *Message) (<-chan *Message, <-chan error, error)
}

// Transformer ...
type Transformer interface {
	Transform(ctx context.Context, msgc <-chan *Message, xformr Xform) (<-chan *Message, <-chan error, error)
}

// Sinker ...
type Sinker interface {
	Sink(ctx context.Context, msgc <-chan *Message, rcvr Receiver) (<-chan error, error)
}

// Xform ..
type Xform func(context.Context, *Message) (*Message, error)

// Receiver ...
type Receiver func(context.Context, *Message)

// New ...
func New(srcr Sourcer, xformr Transformer) *Client {
	return &Client{
		sourcer: srcr,
		xformer: xformr,
	}
}

// NewConduit ...
func (c *Client) NewConduit(ctx context.Context, name string) (*Conduit, error) {
	sendc := make(chan *Message, 1000000)
	srcc, srcerrc, err := c.sourcer.Source(ctx, sendc)
	if err != nil {
		return nil, err
	}

	return &Conduit{
		sendc:     sendc,
		srcc:      srcc,
		srcerrc:   srcerrc,
		xformer:   c.xformer,
		xformc:    map[string]<-chan *Message{},
		xformerrc: map[string]<-chan error{},
	}, nil
}

// GetConduit ...
func (c *Client) GetConduit(name string) (*Conduit, error) {
	conduit, ok := c.conduits[name]
	if !ok {
		return nil, ErrConduitNotFound
	}

	return conduit, nil
}

// Send ...
func (c *Conduit) Send(msg *Message) error {
	c.sendc <- msg
	return nil
}

// NewTransform ...
func (c *Conduit) NewTransform(ctx context.Context, name string, xform Xform) error {
	xformc, xformerrc, err := c.xformer.Transform(ctx, c.srcc, xform)
	if err != nil {
		return err
	}

	_, ok := c.xformc[name]
	if ok {
		return ErrXformExists
	}

	c.xformc[name] = xformc
	c.xformerrc[name] = xformerrc

	return nil
}

// Receive ...
func (c *Conduit) Receive(ctx context.Context, xformName string, rcvr Receiver) error {
	sinkerrc := make(chan error)
	msgc, ok := c.xformc[xformName]
	if !ok {
		return ErrXformNotFound
	}

	go func() {
		defer close(sinkerrc)

		for {
			select {
			case msg := <-msgc:
				rcvr(ctx, msg)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

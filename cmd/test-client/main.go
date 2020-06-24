package main

import (
	"context"
	"log"
	"time"

	"github.com/boringsoftwarecompany/pipeline/pkg/pipeline"
	"github.com/boringsoftwarecompany/pipeline/pkg/source/local"
	"github.com/boringsoftwarecompany/pipeline/pkg/xform/echo"
	"github.com/paulbellamy/ratecounter"
)

func main() {
	recvCounter := ratecounter.NewRateCounter(1 * time.Second)
	sendCounter := ratecounter.NewRateCounter(1 * time.Second)
	timer := time.NewTimer(1 * time.Second)

	srcr := local.New()
	xformr := echo.New()
	c := pipeline.New(srcr, xformr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conduit, err := c.NewConduit(ctx, "test")
	if err != nil {
		log.Fatal(err)
	}

	err = conduit.NewTransform(ctx, "test", echoXform)
	if err != nil {
		log.Fatal(err)
	}

	err = conduit.Receive(ctx, "test", printReceiver(ctx, recvCounter))
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		time.Sleep(100 * time.Second)
		cancel()
	}()

	go func() {
		for {
			select {
			case <-timer.C:
				log.Printf("recv: %d\n", recvCounter.Rate())
				log.Printf("send: %d\n", sendCounter.Rate())
				timer.Reset(time.Second * 1)
			}
		}
	}()

	for {
		conduit.Send(&pipeline.Message{
			Route: "test",
			Data:  []byte(""),
		})
		sendCounter.Incr(1)
	}
}

func echoXform(ctx context.Context, msg *pipeline.Message) (*pipeline.Message, error) {
	return msg, nil
}

func printReceiver(ctx context.Context, counter *ratecounter.RateCounter) pipeline.Receiver {
	return func(ctx context.Context, msg *pipeline.Message) {
		counter.Incr(1)
	}
}

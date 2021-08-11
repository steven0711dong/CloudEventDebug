package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
)

var cl cloudevents.Client
var ctx context.Context

func main() {
	ctx = cloudevents.ContextWithTarget(context.Background(), os.Getenv("SinkAdd"))
	fmt.Println(os.Getenv("SinkAdd"))
	p, err := cloudevents.NewHTTP()
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}
	c, err := cloudevents.NewClient(p, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}
	cl = c
	for i := 0; i < 300; i++ {
		go as(i)
	}
	time.Sleep(30 * time.Second)

}

func as(c int) {
	e := cloudevents.NewEvent()
	e.SetID("partition:0/offset:1")
	e.SetType("com.cloudevents.sample.sent")
	e.SetSource("io#sample")
	_ = e.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
		"id":      1,
		"message": "Hello, World!",
	})
	res := cl.Send(ctx, e)
	if cloudevents.IsUndelivered(res) {
		fmt.Printf("Failed to send: %+v", res)
	} else {
		var httpResult *cehttp.Result
		cloudevents.ResultAs(res, &httpResult)
		log.Printf("Sent with status code %d", httpResult.StatusCode)
	}
}

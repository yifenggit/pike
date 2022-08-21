package queue

import (
	"log"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

var pulsarClient pulsar.Client
var pulsarClientOnce sync.Once

// global Client
func NewClient(options pulsar.ClientOptions) pulsar.Client {
	pulsarClientOnce.Do(func() {
		client, err := pulsar.NewClient(options)
		if err != nil {
			log.Fatalf("Could not instantiate Pulsar client: %v", err)
		} else {
			pulsarClient = client
		}
	})
	return pulsarClient
}

package influxclient

import (
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/influxdb/influxdb/client"
)

var Client *InfluxClient

func init() {
	rand.Seed(time.Now().UnixNano())
}

type InfluxClient struct {
	client   *client.Client
	quitChan chan struct{}
}

func New(influxURL string, quitChan chan struct{}) (*InfluxClient, error) {
	cc := &client.ClientConfig{}

	url, err := url.Parse(influxURL)
	if err != nil {
		return nil, err
	}

	if url.Scheme != "influxdb" {
		return nil, fmt.Errorf("Invalid influxdb scheme: %s", url.Scheme)
	}

	cc.Host = url.Host

	if url.User != nil {
		cc.Username = url.User.Username()

		if pw, set := url.User.Password(); set {
			cc.Password = pw
		}
	}

	cc.Database = strings.TrimPrefix(url.Path, "/")
	cc.IsUDP = true

	c, err := client.New(cc)
	if err != nil {
		return nil, err
	}

	log.Printf("Sending stats to InfluxDB at %s (%s)", cc.Host, cc.Database)

	ic := &InfluxClient{
		client:   c,
		quitChan: quitChan,
	}

	return ic, nil
}

func SetDefaultClient(c *InfluxClient) {
	Client = c
}

func (c *InfluxClient) Send(name string, columns []string, points []interface{}, sampleRate float32) {
	if c == nil {
		return
	}

	if sampleRate < 1 && rand.Float32() > sampleRate {
		return
	}

	series := &client.Series{
		Name:    name,
		Columns: columns,
		Points:  [][]interface{}{points},
	}

	if err := c.client.WriteSeriesOverUDP([]*client.Series{series}); err != nil {
		log.Printf("Failed to write series to influxdb: %s", err)
	}
}

package main

import (
	"fmt"
	"net/http"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

var VERSION = "v0.0.2-dev"

func main() {
	app := cli.NewApp()
	app.Name = "scheduler"
	app.Version = VERSION
	app.Usage = "Windows scheduler for Rancher."
	app.Action = run
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "metadata-address",
			Usage: "The metadata service address",
			Value: "rancher-metadata",
		},
		cli.StringFlag{
			Name:  "listen",
			Usage: "Listen on this port for requests",
			Value: ":80",
		},
		cli.BoolFlag{
			Name:  "debug",
			Usage: "Enable debug logging",
		},
	}

	app.Run(os.Args)
}

func run(c *cli.Context) error {
	if c.Bool("debug") {
		log.SetLevel(log.DebugLevel)
	}

	url := os.Getenv("CATTLE_URL")
	ak := os.Getenv("CATTLE_ACCESS_KEY")
	sk := os.Getenv("CATTLE_SECRET_KEY")
	if url == "" || ak == "" || sk == "" {
		log.Fatalf("Cattle connection environment variables not available. URL: %v, access key %v, secret key redacted.", url, ak)
	}

	exit := make(chan error)
	watcher := NewMetadataWatcher(c.String("metadata-address"))
	watcher.Start()

	go func(exit chan<- error) {
		scheduler := NewScheduler(watcher)
		err := ConnectToEventStream(url, ak, sk, scheduler)
		exit <- errors.Wrapf(err, "Cattle event subscriber exited.")
	}(exit)

	go func(exit chan<- error) {
		err := startHealthCheck(c.String("listen"))
		exit <- errors.Wrapf(err, "Healthcheck provider died.")
	}(exit)

	err := <-exit
	log.Errorf("Exiting scheduler with error: %v", err)
	return err
}

func startHealthCheck(port string) error {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if s, err := stats.ToJSON(); err == nil {
			fmt.Fprintf(w, string(s))
		}
	})
	log.Infof("Listening for requests on 0.0.0.0%s", port)
	err := http.ListenAndServe(port, nil)
	return err
}

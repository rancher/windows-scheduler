package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher-metadata/metadata"
)

type MetadataWatcher struct {
	sync.RWMutex
	client  metadata.Client
	version string
	Hosts   []metadata.Host
}

func NewMetadataWatcher(metadataAddress string) *MetadataWatcher {
	return &MetadataWatcher{
		client:  metadata.NewClient(fmt.Sprintf("http://%s/2015-12-19", metadataAddress)),
		version: "init",
	}
}

func (w *MetadataWatcher) Start() {
	log.Infof("Subscribing to metadata changes.")
	go w.start()
}

func (w *MetadataWatcher) start() {
	for {
		defaultRetry(func() error {
			newVersion, err := w.waitVersion(5, w.version)
			if err != nil {
				return err
			} else if w.version == newVersion {
				log.Debug("No changes in metadata version")
			} else {
				log.Debugf("Metadata Version has been changed. Old version: %s. New version: %s.", w.version, newVersion)
				w.version = newVersion
				w.update()
			}
			return nil
		})
	}
}

func (w *MetadataWatcher) update() {
	w.Lock()
	defer w.Unlock()

	defaultRetry(func() error {
		hosts, err := w.client.GetHosts()
		if err == nil {
			w.Hosts = hosts
		}
		return err
	})

	log.WithFields(log.Fields{
		"hosts": len(w.Hosts),
	}).Debug("metadata_update")
	stats.Metadata.IncUpdate()
}

func (w *MetadataWatcher) waitVersion(maxWait int, version string) (string, error) {
	resp, err := w.client.SendRequest(fmt.Sprintf("/version?wait=true&value=%s&maxWait=%d", version, maxWait))
	if err != nil {
		return "", err
	}
	err = json.Unmarshal(resp, &version)
	return version, err
}

func defaultRetry(do func() error) {
	retry(5, 500*time.Millisecond, 2.0, do)
}

func retry(maxTries int, initialSleep time.Duration, backoff float64, do func() error) {
	sleep := initialSleep
	for tries := 1; tries <= maxTries; tries++ {
		err := do()
		if err == nil {
			return
		}
		log.WithFields(log.Fields{
			"count": tries,
			"error": err,
		}).Error("retry")
		time.Sleep(sleep)
		sleep = sleep * time.Duration(backoff)
	}
	panic(fmt.Sprintf("%d consecutive errors!", maxTries))
}

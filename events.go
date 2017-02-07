package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/rancher/event-subscriber/events"
	"github.com/rancher/event-subscriber/locks"
	"github.com/rancher/go-rancher/client"
)

func ConnectToEventStream(cattleURL, accessKey, secretKey string, scheduler *Scheduler) error {
	log.Info("Connecting to cattle event stream.")

	eventHandlers := map[string]events.EventHandler{
		"scheduler.prioritize": scheduler.Prioritize,
		"scheduler.reserve":    scheduler.Reserve,
		"scheduler.release":    scheduler.Release,
		"ping":                 func(_ *events.Event, _ *client.RancherClient) error { return nil },
	}

	router, err := events.NewEventRouter("", 0, cattleURL, accessKey, secretKey, nil, eventHandlers, "", 100, events.DefaultPingConfig)
	if err != nil {
		return err
	}

	wp := events.SkippingWorkerPool(100, nopLocker)
	err = router.RunWithWorkerPool(wp)
	return err
}

func nopLocker(_ *events.Event) locks.Locker { return locks.NopLocker() }

func PublishEvent(event *events.Event, data map[string]interface{}, apiClient *client.RancherClient) error {
	reply := &client.Publish{
		Name:        event.ReplyTo,
		PreviousIds: []string{event.ID},
	}
	reply.ResourceType = "schedulerRequest"
	reply.ResourceId = event.ResourceID
	reply.Data = data

	log.Debugf("Reply: Name: %v, PreviousIds: %v, ResourceId: %v, Data: %v.", reply.Name, reply.PreviousIds, reply.ResourceId, reply.Data)
	_, err := apiClient.Publish.Create(reply)
	return err
}

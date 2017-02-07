package main

import (
	"encoding/json"
	"fmt"
	"strings"

	//"github.com/mitchellh/mapstructure"
	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/event-subscriber/events"
	"github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/go-rancher/client"
)

type Scheduler struct {
	watcher *MetadataWatcher
}

func NewScheduler(watcher *MetadataWatcher) *Scheduler {
	return &Scheduler{
		watcher: watcher,
	}
}

func (s *Scheduler) Prioritize(event *events.Event, client *client.RancherClient) error {
	stats.Event.IncPrioritize()
	log.Debugf("Received event: Name: %s, Event Id: %s, Resource Id: %s", event.Name, event.ID, event.ResourceID)
	request, err := decodeRequest(event)
	if err != nil {
		return errors.Wrapf(err, "Error decoding prioritize event %v.", event)
	}

	s.watcher.RLock()
	response := &SchedulerResponse{
		// FIXME copy
		Hosts: s.watcher.Hosts,
	}
	s.watcher.RUnlock()

	response.FilterWindows(request)
	response.FilterAffinities(request)

	eventDataWrapper := map[string]interface{}{"prioritizedCandidates": getHostUUID(response.Hosts)}
	return PublishEvent(event, eventDataWrapper, client)
}

func (s *Scheduler) Reserve(event *events.Event, client *client.RancherClient) error {
	stats.Event.IncReserve()
	// No-Op, our decision making doesn't require tracking state
	return PublishEvent(event, nil, client)
}

func (s *Scheduler) Release(event *events.Event, client *client.RancherClient) error {
	stats.Event.IncRelease()
	// No-Op, our decision making doesn't require tracking state
	return PublishEvent(event, nil, client)
}

func decodeRequest(event *events.Event) (*SchedulerRequest, error) {
	data := &SchedulerRequest{}
	if s, ok := event.Data["schedulerRequest"]; ok {
		jdata, err := json.Marshal(s)
		if err == nil {
			err = json.Unmarshal(jdata, data)
		}
		return data, err
	}
	return data, fmt.Errorf("Event doesn't contain a scheduler request. Event: %#v", event)
}

type SchedulerRequest struct {
	Instances []Instance `json:"context"`
}

type Instance struct {
	Data InstanceData `json:"data"`
}

type InstanceData struct {
	Fields InstanceFields `json:"fields"`
}

type InstanceFields struct {
	Image  string            `json:"imageUuid"`
	Labels map[string]string `json:"labels"`
}

type SchedulerResponse struct {
	Hosts []metadata.Host
}

func (s *SchedulerResponse) FilterWindows(r *SchedulerRequest) {
	for _, i := range r.Instances {
		i.AddSchedulingAffinity("host_label", fmt.Sprintf("io.rancher.host.os=%s", i.DetectOS()))
	}
}

func (s *SchedulerResponse) FilterAffinities(r *SchedulerRequest) {
	list := r.GetAffinityList()
	list.Sort()
	s.FilterAffinityList(list)
}

func (i *Instance) DetectOS() string {
	os := "windows"

	wl := false
	identifier := strings.Split(i.Data.Fields.Image, ":")[1]
	x := strings.Split(identifier, "/")
	switch x[0] {
	case "llparse":
		wl = true
	case "rancher":
		wl = true
	}
	if wl {
		wl = false
		switch x[1] {
		case "network-manager":
			wl = true
		case "metadata":
			wl = true
		case "dns":
			wl = true
		case "healthcheck":
			wl = true
		case "lb-service-haproxy":
			wl = true
		case "windows-scheduler":
			wl = true
		case "scheduler":
			wl = true
		}
	}
	if wl {
		os = "linux"
	}

	log.WithFields(log.Fields{
		"image": i.Data.Fields.Image,
		"os":    os,
	}).Debug("detect_os")

	return os
}

func (i *Instance) AddSchedulingAffinity(kind string, value string) {
	i.AddLabel(fmt.Sprintf("io.rancher.scheduler.affinity:%s", kind), value)
}

func (i *Instance) AddLabel(key string, val string) {
	newval, ok := i.Data.Fields.Labels[key]
	if ok {
		newval += ","
	}
	newval += val
	i.Data.Fields.Labels[key] = newval
}

func getHostUUID(hosts []metadata.Host) []string {
	uuid := []string{}
	for _, host := range hosts {
		uuid = append(uuid, host.UUID)
	}
	return uuid
}

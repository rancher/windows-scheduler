package main

import (
	"encoding/json"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
)

const StatsLogInterval = 15 * time.Second

type Stats struct {
	Metadata *MetadataStats `json:"metadata"`
	Event    *EventStats    `json:"event"`
}

type MetadataStats struct {
	Update int64 `json:"update"`
}

type EventStats struct {
	Prioritize int64 `json:"prioritize"`
	Reserve    int64 `json:"reserve"`
	Release    int64 `json:"release"`
}

var stats *Stats

func init() {
	stats = &Stats{
		Metadata: &MetadataStats{},
		Event:    &EventStats{},
	}
	go func() {
		t := time.NewTicker(StatsLogInterval)
		for _ = range t.C {
			stats.Log()
		}
	}()
}

func (s *MetadataStats) IncUpdate() {
	atomic.AddInt64(&s.Update, 1)
}

func (s *EventStats) IncPrioritize() {
	atomic.AddInt64(&s.Prioritize, 1)
}

func (s *EventStats) IncReserve() {
	atomic.AddInt64(&s.Reserve, 1)
}

func (s *EventStats) IncRelease() {
	atomic.AddInt64(&s.Release, 1)
}

func (s *Stats) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *Stats) Log() {
	log.WithFields(log.Fields{
		"metadata_update":  atomic.LoadInt64(&s.Metadata.Update),
		"event_prioritize": atomic.LoadInt64(&s.Event.Prioritize),
		"event_reserve":    atomic.LoadInt64(&s.Event.Reserve),
		"event_release":    atomic.LoadInt64(&s.Event.Release),
	}).Info("stats")
}

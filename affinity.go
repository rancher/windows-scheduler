package main

import (
	"fmt"
	"sort"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher-metadata/metadata"
)

type InstanceAffinity struct {
	kind  string // host_label, container_label, container
	soft  bool   // best-effort affinity
	anti  bool   // anti-affinity
	value string // label key=value pair or a container name
}

func (p *InstanceAffinity) Copy() *InstanceAffinity {
	return &InstanceAffinity{
		kind:  p.kind,
		soft:  p.soft,
		anti:  p.anti,
		value: p.value,
	}
}

func (p *InstanceAffinity) String() string {
	return fmt.Sprintf("Affinity kind=%s soft=%v anti=%v value=%s", p.kind, p.soft, p.anti, p.value)
}

// InstanceAffinityList implements Sort interface
type InstanceAffinityList struct {
	affinities []*InstanceAffinity
}

func (p InstanceAffinityList) Sort() {
	sort.Sort(p)
}

func (p InstanceAffinityList) Len() int {
	return len(p.affinities)
}

func (p InstanceAffinityList) Swap(i, j int) {
	p.affinities[i], p.affinities[j] = p.affinities[j], p.affinities[i]
}

func (p InstanceAffinityList) Less(i, j int) bool {
	// hard constraint takes precedence over soft constraint
	// host_label soft constraint takes precedence over container* soft constraints
	return (!p.affinities[i].soft && p.affinities[j].soft) ||
		(p.affinities[i].soft && p.affinities[j].soft &&
			p.affinities[i].kind == "host_label" &&
			strings.HasPrefix(p.affinities[j].kind, "container"))
}

func (p *InstanceAffinityList) String() string {
	val := "AffinityList"
	for _, affinity := range p.affinities {
		val += fmt.Sprintf(" %s\n", affinity)
	}
	return val
}

func (response *SchedulerResponse) FilterAffinityList(list *InstanceAffinityList) {
	for _, affinity := range list.affinities {
		if affinity.kind != "host_label" {
			// TODO implement container_label, container affinities
			log.Debugf("%s affinities are unimplemented", affinity.kind)
			continue
		}
		candidates := []metadata.Host{}
		for _, host := range response.Hosts {
			viable := affinity.anti
			affinityValueParts := strings.Split(affinity.value, "=")
			if hostLabelValue, hostLabelKeyExists := host.Labels[affinityValueParts[0]]; hostLabelKeyExists {
				if affinityValueParts[1] == hostLabelValue {
					viable = viable != true
				}
			}
			if viable {
				candidates = append(candidates, host)
			}
		}
		// ignore unsatisfiable soft constraint
		if !affinity.soft || len(candidates) > 0 {
			response.Hosts = candidates
		}
	}
}

func (request *SchedulerRequest) GetAffinityList() *InstanceAffinityList {
	list := &InstanceAffinityList{}
	for _, instance := range request.Instances {
		for instanceLabelKey, instanceLabelValues := range instance.Data.Fields.Labels {
			if strings.HasPrefix(instanceLabelKey, "io.rancher.scheduler.affinity") {
				affinityDef := strings.Split(instanceLabelKey, ":")[1]
				affinityDefParts := strings.Split(affinityDef, "_")

				affinity := &InstanceAffinity{}
				if affinityDefParts[0] == "host" && affinityDefParts[1] == "label" {
					affinity.kind = "host_label"
					affinityDefParts = affinityDefParts[2:]
				} else if affinityDefParts[0] == "container" {
					if affinityDefParts[1] == "label" {
						affinity.kind = "container_label"
						affinityDefParts = affinityDefParts[2:]
					} else {
						affinity.kind = "container"
						affinityDefParts = affinityDefParts[1:]
					}
				}
				for _, affinityModifier := range affinityDefParts {
					switch affinityModifier {
					case "soft":
						affinity.soft = true
					case "ne":
						affinity.anti = true
					}
				}
				for _, instanceLabelValue := range strings.Split(instanceLabelValues, ",") {
					a := affinity.Copy()
					a.value = instanceLabelValue
					list.affinities = append(list.affinities, a)
				}
			}
		}
	}
	return list
}

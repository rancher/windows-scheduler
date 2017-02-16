Windows Scheduler
=================

A Rancher scheduler for heterogeneous Windows/Linux environments.

The primary goal is selecting a host with correct operating system for a particular container. It accomplishes this task in two stages:

1. Inject a host_label constraint into incoming prioritize events based on `io.rancher.container.system` label presence
2. Evaluate host_label constraints and return a set of host candidates

## Building

`make`

## Running

`./bin/windows-scheduler`

## License
Copyright (c) 2014-2017 [Rancher Labs, Inc.](http://rancher.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

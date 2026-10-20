/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topology

import (
	"sync"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
)

// MaxDenseDomains is the number of domains up to which a topology key's domains are interned and counted densely by
// DomainID. Keys with more domains, and kubernetes.io/hostname, are counted sparsely by domain name.
const MaxDenseDomains = 4096

// DomainID identifies a domain of one topology key. IDs are dense, start at 0, and are never reused, so arrays and masks
// indexed by the IDs of an older DomainView stay valid for newer ones.
type DomainID uint32

// DomainDict interns the domains of one topology key. There is one dictionary per key per process (see Dict), shared
// by every scheduling simulation and by count sources, so that DomainIDs agree everywhere. Domains are only ever added.
// Interning takes a lock; reading a view doesn't.
type DomainDict struct {
	key  string
	mu   sync.Mutex
	view atomic.Pointer[DomainView]
}

// DomainView is an immutable prefix of a DomainDict.
type DomainView struct {
	names []string
	ids   map[string]DomainID
}

var dicts sync.Map // topology key -> *DomainDict

// Dict returns the process-wide dictionary of the topology key.
func Dict(topologyKey string) *DomainDict {
	if d, ok := dicts.Load(topologyKey); ok {
		return d.(*DomainDict)
	}
	d := &DomainDict{key: topologyKey}
	d.view.Store(&DomainView{ids: map[string]DomainID{}})
	actual, _ := dicts.LoadOrStore(topologyKey, d)
	return actual.(*DomainDict)
}

// Dense reports whether the key's domains are counted densely by DomainID: every key except kubernetes.io/hostname,
// while it has at most MaxDenseDomains domains.
func Dense(topologyKey string) bool {
	return topologyKey != corev1.LabelHostname && Dict(topologyKey).View().Len() <= MaxDenseDomains
}

// Key returns the dictionary's topology key.
func (d *DomainDict) Key() string {
	return d.key
}

// View returns the domains interned so far.
func (d *DomainDict) View() *DomainView {
	return d.view.Load()
}

// Intern returns the ID of the domain, adding it if it's new.
func (d *DomainDict) Intern(name string) DomainID {
	if id, ok := d.view.Load().ids[name]; ok {
		return id
	}
	return d.InternAll(name)[0]
}

// InternAll returns the IDs of the domains, adding the new ones under a single lock.
func (d *DomainDict) InternAll(names ...string) []DomainID {
	d.mu.Lock()
	defer d.mu.Unlock()
	v := d.view.Load()
	ids := make([]DomainID, len(names))
	var next *DomainView
	for i, name := range names {
		if id, ok := v.ids[name]; ok {
			ids[i] = id
			continue
		}
		if next == nil {
			// copy the map so readers of the current view never see a write; dictionaries stay small
			next = &DomainView{names: v.names, ids: make(map[string]DomainID, len(v.ids)+len(names))}
			for k, id := range v.ids {
				next.ids[k] = id
			}
			v = next
		}
		if id, ok := next.ids[name]; ok {
			ids[i] = id
			continue
		}
		ids[i] = DomainID(len(next.names))
		next.ids[name] = ids[i]
		next.names = append(next.names, name)
	}
	if next != nil {
		d.view.Store(next)
	}
	return ids
}

// Len returns the number of domains in the view.
func (v *DomainView) Len() int {
	return len(v.names)
}

// Name returns the name of the domain.
func (v *DomainView) Name(id DomainID) string {
	return v.names[id]
}

// Lookup returns the ID of the domain, if the view has it.
func (v *DomainView) Lookup(name string) (DomainID, bool) {
	id, ok := v.ids[name]
	return id, ok
}

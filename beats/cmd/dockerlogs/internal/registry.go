package internal

import (
	"fmt"
	"sync"
)

type Digest struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Name        string `json:"name"`
	DiscoveryAt int64  `json:"discovery_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

func (d *Digest) String() string {
	return fmt.Sprintf("%+v", *d)
}

type ContainerRegistry struct {
	mu sync.RWMutex
	// ID=>ContainerInfo
	containers map[string]*Digest
}

func NewRegistry() *ContainerRegistry {
	return &ContainerRegistry{containers: make(map[string]*Digest, 10)}
}

func (r *ContainerRegistry) Get(containerID string) (info *Digest, exists bool) {
	r.mu.RLock()
	info, exists = r.containers[containerID]
	r.mu.RUnlock()
	return
}

func (r *ContainerRegistry) Put(containerID string, info *Digest) {
	r.mu.Lock()
	r.containers[containerID] = info
	r.mu.Unlock()
	return
}

func (r *ContainerRegistry) Remove(ID string) {
	r.mu.Lock()
	delete(r.containers, ID)
	r.mu.Unlock()
}

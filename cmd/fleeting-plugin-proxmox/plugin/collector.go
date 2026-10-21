package plugin

import (
	"context"
	"sync"
	"time"

	"github.com/luthermonson/go-proxmox"
)

const (
	collectionInterval         = 1 * time.Minute
	collectionTimeout          = 5 * time.Minute
	collectionWaitAfterTrigger = 2 * time.Second
)

func (ig *InstanceGroup) startRemovedInstanceCollector() {
	ig.collectorWaitGroup.Add(1)

	go func() {
		defer ig.collectorWaitGroup.Done()
		ig.runRemovedInstanceCollector()
	}()
}

func (ig *InstanceGroup) runRemovedInstanceCollector() {
	ig.collectRemovedInstances()

	for {
		select {
		case <-ig.collectorShutdownTrigger:
			return
		case <-time.After(collectionInterval):
			ig.collectRemovedInstances()
		case <-ig.instanceCollectionTrigger:
			ig.drainInstanceCollectionTriggerChannel()

			select {
			case <-ig.collectorShutdownTrigger:
				return
			case <-time.After(collectionWaitAfterTrigger):
			}

			ig.collectRemovedInstances()
		}
	}
}

func (ig *InstanceGroup) collectRemovedInstances() {
	ctx, cancel := context.WithTimeout(context.Background(), collectionTimeout)
	defer cancel()

	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		ig.log.Error("collector failed to list instances", "err", err)
		return
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	for _, member := range pool.Members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		if !ig.isInstanceDeleting(member) {
			continue
		}

		if !ig.tryStartCollecting(member.VMID) {
			continue
		}

		ig.log.Info("collector found instance to remove", "vmid", member.VMID, "name", member.Name)

		wg.Add(1)

		go func(member proxmox.ClusterResource) {
			defer wg.Done()
			defer ig.finishCollecting(member.VMID)
			ig.collectInstance(ctx, member)
		}(member)
	}
}

func (ig *InstanceGroup) collectInstance(ctx context.Context, member proxmox.ClusterResource) {
	vm, err := ig.getProxmoxVMOnNode(ctx, int(member.VMID), member.Node)
	if err != nil {
		ig.log.Error("collector failed to fetch instance info", "vmid", member.VMID, "err", err)
		return
	}

	if vm.Status == proxmox.StatusVirtualMachineRunning {
		task, err := vm.Stop(ctx)
		if err == nil {
			err = waitForProxmoxTask(ctx, task, ig.taskWaitInterval(), collectionTimeout)
		}

		if err != nil {
			ig.log.Error("collector failed to stop instance", "vmid", member.VMID, "err", err)
			return
		}
	}

	task, err := vm.Delete(ctx, &proxmox.VirtualMachineDeleteOptions{
		DestroyUnreferencedDisks: true,
		Purge:                    true,
	})
	if err == nil {
		err = waitForProxmoxTask(ctx, task, ig.taskWaitInterval(), collectionTimeout)
	}

	if err != nil {
		ig.log.Error("collector failed to delete instance", "vmid", member.VMID, "err", err)
	}
}

func (ig *InstanceGroup) tryStartCollecting(vmid uint64) bool {
	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	if ig.collectingInstances == nil {
		ig.collectingInstances = make(map[uint64]struct{})
	}

	if _, exists := ig.collectingInstances[vmid]; exists {
		return false
	}

	ig.collectingInstances[vmid] = struct{}{}

	return true
}

func (ig *InstanceGroup) finishCollecting(vmid uint64) {
	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	delete(ig.collectingInstances, vmid)
}

func (ig *InstanceGroup) drainInstanceCollectionTriggerChannel() {
	for {
		select {
		case <-ig.instanceCollectionTrigger:
			// NOOP
		default:
			return
		}
	}
}

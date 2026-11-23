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
	baseCtx := ig.lifecycleContext()
	ig.collectRemovedInstances()

	for {
		intervalTimer := time.NewTimer(collectionInterval)
		select {
		case <-baseCtx.Done():
			intervalTimer.Stop()
			return
		case <-ig.collectorShutdownTrigger:
			intervalTimer.Stop()
			return
		case <-intervalTimer.C:
			ig.collectRemovedInstances()
		case <-ig.instanceCollectionTrigger:
			intervalTimer.Stop()
			ig.drainInstanceCollectionTriggerChannel()

			waitTimer := time.NewTimer(collectionWaitAfterTrigger)
			select {
			case <-baseCtx.Done():
				waitTimer.Stop()
				return
			case <-ig.collectorShutdownTrigger:
				waitTimer.Stop()
				return
			case <-waitTimer.C:
			}

			ig.collectRemovedInstances()
		}
	}
}

func (ig *InstanceGroup) collectRemovedInstances() {
	baseCtx := ig.lifecycleContext()
	if baseCtx.Err() != nil {
		return
	}

	timeout := ig.taskWaitTimeout()
	if timeout < collectionTimeout {
		timeout = collectionTimeout
	}

	ctx, cancel := context.WithTimeout(baseCtx, timeout)
	defer cancel()

	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		ig.logError("collector failed to list instances", "err", err)
		return
	}

	ig.reapOrphanedCreatingInstances(ctx, pool.Members, time.Now())

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

		ig.logInfo("collector found instance to remove", "vmid", member.VMID, "name", member.Name)

		wg.Add(1)

		go func(member proxmox.ClusterResource) {
			defer wg.Done()
			defer ig.finishCollecting(member.VMID)
			ig.collectInstance(ctx, member)
		}(member)
	}
}

func (ig *InstanceGroup) reapOrphanedCreatingInstances(ctx context.Context, members []proxmox.ClusterResource, now time.Time) {
	staleTimeout := ig.staleCreatingTimeout()
	if staleTimeout <= 0 {
		return
	}

	creatingVMIDs := make(map[uint64]proxmox.ClusterResource)
	for _, member := range members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		if ig.isInstanceCreating(member) && !ig.isActiveDeploy(member.VMID) {
			creatingVMIDs[member.VMID] = member
		}
	}

	toMark := ig.collectTimedOutCreatingMembers(creatingVMIDs, now, staleTimeout)
	if len(toMark) == 0 {
		return
	}

	for _, member := range toMark {
		ig.logWarn("found orphaned creating instance past timeout, marking for removal", "vmid", member.VMID, "node", member.Node, "timeout", staleTimeout)
	}

	if err := ig.markInstancesForRemoval(ctx, toMark...); err != nil {
		ig.logError("failed to mark orphaned creating instances for removal", "err", err)
	}
}

func (ig *InstanceGroup) collectTimedOutCreatingMembers(
	creatingVMIDs map[uint64]proxmox.ClusterResource,
	now time.Time,
	staleTimeout time.Duration,
) []*proxmox.ClusterResource {
	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	if ig.staleCreatingSeen == nil {
		ig.staleCreatingSeen = make(map[uint64]time.Time)
	}

	for vmid := range ig.staleCreatingSeen {
		if _, exists := creatingVMIDs[vmid]; !exists {
			delete(ig.staleCreatingSeen, vmid)
		}
	}

	var timedOut []*proxmox.ClusterResource

	for vmid, member := range creatingVMIDs {
		firstSeen, tracked := ig.staleCreatingSeen[vmid]
		if !tracked {
			ig.staleCreatingSeen[vmid] = now
			continue
		}

		if now.Sub(firstSeen) >= staleTimeout {
			memberCopy := member
			timedOut = append(timedOut, &memberCopy)
			delete(ig.staleCreatingSeen, vmid)
		}
	}

	return timedOut
}

func (ig *InstanceGroup) collectInstance(ctx context.Context, member proxmox.ClusterResource) {
	vm, err := ig.getProxmoxVMOnNode(ctx, int(member.VMID), member.Node)
	if err != nil {
		if isVMNotFoundError(err) {
			ig.logInfo("collector instance already removed", "vmid", member.VMID, "node", member.Node)
			return
		}

		ig.logError("collector failed to fetch instance info", "vmid", member.VMID, "err", err)

		return
	}

	timeout := ig.taskWaitTimeout()

	if vm.Status != proxmox.StatusVirtualMachineStopped {
		err = retryOnProxmoxLock(ctx, 2, 500*time.Millisecond, func() error {
			task, stopErr := vm.Stop(ctx)
			if stopErr != nil {
				return stopErr
			}

			return waitForProxmoxTask(ctx, task, ig.taskWaitInterval(), timeout)
		})
		if err != nil {
			ig.logCollectError("stop", member.VMID, err)
			return
		}
	}

	err = retryOnProxmoxLock(ctx, 2, 500*time.Millisecond, func() error {
		task, delErr := vm.Delete(ctx, &proxmox.VirtualMachineDeleteOptions{
			DestroyUnreferencedDisks: true,
			Purge:                    true,
		})
		if delErr != nil {
			return delErr
		}

		return waitForProxmoxTask(ctx, task, ig.taskWaitInterval(), timeout)
	})
	if err != nil {
		ig.logCollectError("delete", member.VMID, err)
	}
}

func (ig *InstanceGroup) logCollectError(action string, vmid uint64, err error) {
	if isVMNotFoundError(err) {
		ig.logInfo("collector instance already absent during "+action, "vmid", vmid)
		return
	}

	if isProxmoxLockError(err) {
		ig.logWarn("collector deferred "+action+" due to proxmox lock", "vmid", vmid, "err", err)
		return
	}

	ig.logError("collector failed to "+action+" instance", "vmid", vmid, "err", err)
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

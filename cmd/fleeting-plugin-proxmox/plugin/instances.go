package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/luthermonson/go-proxmox"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"golang.org/x/sync/errgroup"
)

const (
	proxmoxTaskWaitInterval  = 1 * time.Second
	proxmoxTaskWaitTimeout   = 5 * time.Minute
	proxmoxAgentStartTimeout = 2 * time.Minute

	cloneAllocateMaxAttempts = 3
	cloneRetryBaseDelay      = 500 * time.Millisecond
)

var ErrCloneVMWithoutConfiguredStorage = errors.New("attempted to clone a VM without configured storage")

type cloneAttemptTarget struct {
	cloneTarget   string
	effectiveNode string
}

func (ig *InstanceGroup) deployInstance(ctx context.Context, template *proxmox.VirtualMachine, cloneMu *sync.Mutex) (int, error) {
	totalStart := time.Now()

	cloneStart := time.Now()
	VMID, targetNode, err := ig.cloneTemplate(ctx, template, cloneMu)
	cloneDuration := time.Since(cloneStart)
	if err != nil {
		return VMID, fmt.Errorf("failed to deploy instance: %w", err)
	}

	defer ig.decrementInFlightClones(targetNode)
	defer ig.unmarkActiveDeploy(uint64(VMID))

	vm, err := ig.findDeployedVM(ctx, VMID, targetNode)
	if err != nil {
		ig.setInstanceState(uint64(VMID), provider.StateDeleting)
		ig.triggerInstanceCollection()

		return VMID, fmt.Errorf("failed to find newly deployed instance vmid='%d': %w", VMID, err)
	}

	if err := ig.applyPreStartConfig(ctx, vm, VMID); err != nil {
		ig.logError("instance pre-start config failed, marking for removal", "vmid", VMID, "err", err)
		ig.markFailedVMForRemoval(vm, VMID)

		return VMID, fmt.Errorf("failed to apply pre-start config, marked for removal due to: %w", err)
	}

	startDuration, agentDuration, err := ig.startAndConfigureInstance(ctx, vm)
	if err != nil {
		ig.logError("instance deployment failed, marking for removal", "vmid", VMID, "err", err)
		ig.markFailedVMForRemoval(vm, VMID)

		return VMID, fmt.Errorf("failed to configure instance, marked for removal due to: %w", err)
	}

	runningName := ig.instanceNameForState(ig.Settings.InstanceNameRunning, VMID)
	applyRunningTags := ig.Settings.InstanceTagsRunning != "" || ig.Settings.InstanceTagsCreating != ""
	renameErr := ig.configureVMOptions(ctx, vm, runningName, ig.Settings.InstanceTagsRunning, applyRunningTags, nil, nil)

	if renameErr != nil {
		ig.logError("failed to rename instance to running, marking for removal", "vmid", VMID, "err", renameErr)
		ig.markFailedVMForRemoval(vm, VMID)

		return VMID, fmt.Errorf("failed to rename instance, marked for removal due to: %w", renameErr)
	}

	ig.setInstanceState(uint64(VMID), provider.StateRunning)

	ig.logInfo(
		"instance ready",
		"vmid", VMID,
		"node", targetNode,
		"clone_ms", cloneDuration.Milliseconds(),
		"start_ms", startDuration.Milliseconds(),
		"agent_ready_ms", agentDuration.Milliseconds(),
		"total_ms", time.Since(totalStart).Milliseconds(),
	)

	return VMID, nil
}

func (ig *InstanceGroup) applyPreStartConfig(ctx context.Context, vm *proxmox.VirtualMachine, vmid int) error {
	creatingName := ""
	if ig.Settings.InstanceNameIncludeVMID {
		creatingName = ig.instanceNameForState(ig.Settings.InstanceNameCreating, vmid)
	}

	hasHardwareSpecs := ig.Settings.VMCores != nil || ig.Settings.VMMemory != nil
	if creatingName == "" && ig.Settings.InstanceTagsCreating == "" && !hasHardwareSpecs {
		return nil
	}

	err := ig.configureVMOptions(
		ctx,
		vm,
		creatingName,
		ig.Settings.InstanceTagsCreating,
		false,
		ig.Settings.VMCores,
		ig.Settings.VMMemory,
	)
	if err != nil {
		if !hasHardwareSpecs && creatingName == "" {
			ig.logWarn("failed to set creating tags on instance", "vmid", vmid, "err", err)
			return nil
		}

		return err
	}

	return nil
}

func (ig *InstanceGroup) findDeployedVM(ctx context.Context, vmid int, targetNode string) (*proxmox.VirtualMachine, error) {
	if targetNode != "" {
		vm, err := ig.getProxmoxVMOnNode(ctx, vmid, targetNode)
		if err == nil {
			return vm, nil
		}
	}

	return ig.getProxmoxVM(ctx, vmid)
}

func (ig *InstanceGroup) startAndConfigureInstance(ctx context.Context, vm *proxmox.VirtualMachine) (time.Duration, time.Duration, error) {
	startBegan := time.Now()

	task, err := vm.Start(ctx)
	if err == nil {
		err = waitForProxmoxTask(ctx, task, ig.taskWaitInterval(), ig.taskWaitTimeout())
	}

	startDuration := time.Since(startBegan)
	if err != nil {
		return startDuration, 0, fmt.Errorf("failed to start newly deployed instance: %w", err)
	}

	agentBegan := time.Now()
	if err := ig.waitForInstanceReady(ctx, vm); err != nil {
		return startDuration, time.Since(agentBegan), fmt.Errorf("failed when waiting for qemu agent to start on newly deployed instance: %w", err)
	}

	return startDuration, time.Since(agentBegan), nil
}

func (ig *InstanceGroup) waitForInstanceReady(ctx context.Context, vm *proxmox.VirtualMachine) error {
	waitCtx, cancel := context.WithTimeout(ctx, ig.agentStartTimeout())
	defer cancel()

	ticker := time.NewTicker(ig.taskWaitInterval())
	defer ticker.Stop()

	var lastErr error

	for {
		networkInterfaces, err := vm.AgentGetNetworkIFaces(waitCtx)
		if err == nil {
			_, _, lastErr = determineAddresses(
				networkInterfaces,
				ig.Settings.InstanceNetworkInterface,
				ig.Settings.InstanceNetworkProtocol,
			)
			if lastErr == nil {
				return nil
			}
		} else {
			if proxmox.IsNotAuthorized(err) {
				return fmt.Errorf("not authorized to query qemu agent on instance vmid='%d': %w", vm.VMID, err)
			}

			if isVMNotFoundError(err) {
				return fmt.Errorf("instance vmid='%d' disappeared while waiting for qemu agent: %w", vm.VMID, err)
			}

			lastErr = err
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("timed out waiting for qemu agent and IP on instance vmid='%d' (last err: %w): %w", vm.VMID, lastErr, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (ig *InstanceGroup) markFailedVMForRemoval(vm *proxmox.VirtualMachine, vmid int) {
	cleanupCtx, cancel := context.WithTimeout(ig.lifecycleContext(), ig.taskWaitTimeout())
	defer cancel()

	targetVM := vm
	if ig.currentClient() != nil && vm != nil && vm.Node != "" {
		liveVM, err := ig.getProxmoxVMOnNode(cleanupCtx, vmid, vm.Node)
		if err != nil {
			if isVMNotFoundError(err) || proxmox.IsNotAuthorized(err) {
				ig.deleteInstanceState(uint64(vmid))
				return
			}
		} else if liveVM != nil {
			targetVM = liveVM
		}
	}

	if targetVM != nil && targetVM.Name != "" && !ig.ownsInstanceName(targetVM.Name, uint64(vmid)) {
		ig.logWarn("skipping removal of recycled VMID owned by foreign name", "vmid", vmid, "name", targetVM.Name)
		ig.deleteInstanceState(uint64(vmid))

		return
	}

	ig.setInstanceState(uint64(vmid), provider.StateDeleting)

	removingName := ig.instanceNameForState(ig.Settings.InstanceNameRemoving, vmid)
	applyRemovingTags := ig.Settings.InstanceTagsRemoving != "" || ig.Settings.InstanceTagsCreating != "" || ig.Settings.InstanceTagsRunning != ""
	renameErr := ig.configureVMOptions(cleanupCtx, targetVM, removingName, ig.Settings.InstanceTagsRemoving, applyRemovingTags, nil, nil)

	if renameErr != nil && !isVMNotFoundError(renameErr) && !proxmox.IsNotAuthorized(renameErr) {
		ig.logError("failed to rename failed instance for removal", "vmid", vmid, "err", renameErr)
	}

	ig.triggerInstanceCollection()
}

func (ig *InstanceGroup) cloneTemplate(ctx context.Context, template *proxmox.VirtualMachine, cloneMu *sync.Mutex) (int, string, error) {
	cloneOptions, err := ig.getTemplateCloneOptions(template)
	if err != nil {
		return -1, "", err
	}

	if err := ig.acquireCloneSlot(ctx); err != nil {
		return -1, "", fmt.Errorf("failed waiting for clone concurrency slot: %w", err)
	}
	defer ig.releaseCloneSlot()

	delay := cloneRetryBaseDelay

	for attempt := 0; attempt < cloneAllocateMaxAttempts; attempt++ {
		VMID, targetNode, task, err := ig.allocateAndStartCloneTask(ctx, template, cloneOptions, cloneMu)
		if err != nil {
			return -1, "", err
		}

		waitErr := waitForProxmoxTask(ctx, task, ig.taskWaitInterval(), ig.taskWaitTimeout())
		if waitErr == nil {
			return VMID, targetNode, nil
		}

		ig.decrementInFlightClones(targetNode)
		ig.unmarkActiveDeploy(uint64(VMID))

		if isDuplicateVMIDError(waitErr) && attempt < cloneAllocateMaxAttempts-1 {
			ig.deleteInstanceState(uint64(VMID))
			ig.logWarn("clone task hit duplicate VMID allocated concurrently, retrying", "vmid", VMID, "node", targetNode, "attempt", attempt+1, "err", waitErr)

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return VMID, targetNode, fmt.Errorf("failed waiting for clone task: %w", waitErr)
			case <-timer.C:
			}

			delay *= 2
			continue
		}

		ig.cleanupFailedCloneTask(VMID, targetNode, waitErr)

		return VMID, targetNode, fmt.Errorf("failed waiting for clone task: %w", waitErr)
	}

	return -1, "", errors.New("failed to clone template after maximum duplicate VMID retries")
}

func (ig *InstanceGroup) cleanupFailedCloneTask(vmid int, targetNode string, waitErr error) {
	if isDuplicateVMIDError(waitErr) {
		// Never delete a pre-existing VM owned by another runner or process.
		ig.deleteInstanceState(uint64(vmid))
		return
	}

	findCtx, findCancel := context.WithTimeout(ig.lifecycleContext(), ig.httpTimeout())
	vm, findErr := ig.findDeployedVM(findCtx, vmid, targetNode)
	findCancel()

	if findErr != nil {
		ig.deleteInstanceState(uint64(vmid))
		return
	}

	if vm.Name != "" && !matchesInstanceName(vm.Name, ig.Settings.InstanceNameCreating, uint64(vmid)) {
		ig.logWarn("skipping removal of failed clone VMID with foreign name", "vmid", vmid, "name", vm.Name)
		ig.deleteInstanceState(uint64(vmid))

		return
	}

	ig.markFailedVMForRemoval(vm, vmid)
}

func (ig *InstanceGroup) allocateAndStartCloneTask(
	ctx context.Context,
	template *proxmox.VirtualMachine,
	cloneOptions *proxmox.VirtualMachineCloneOptions,
	cloneMu *sync.Mutex,
) (int, string, *proxmox.Task, error) {
	cloneMu.Lock()
	defer cloneMu.Unlock()

	candidates, err := ig.rankTargetNodes(ctx, template)
	if err != nil {
		return -1, "", nil, err
	}

	targets := buildCloneAttemptTargets(template, candidates)

	var lastErr error
	for _, target := range targets {
		vmid, task, cloneErr := ig.cloneToTargetWithRetry(ctx, template, cloneOptions, target.cloneTarget)
		if cloneErr == nil {
			ig.logInfo("Deploying new instance", "vmid", vmid, "node", target.effectiveNode)
			ig.setInstanceState(uint64(vmid), provider.StateCreating)
			ig.incrementInFlightClones(target.effectiveNode)
			ig.markActiveDeploy(uint64(vmid))

			return vmid, target.effectiveNode, task, nil
		}

		lastErr = cloneErr
		ig.logWarn("clone attempt failed on candidate node", "target_node", target.effectiveNode, "err", cloneErr)
	}

	return -1, "", nil, fmt.Errorf("failed to clone the template: %w", lastErr)
}

func buildCloneAttemptTargets(template *proxmox.VirtualMachine, candidates []nodeCandidate) []cloneAttemptTarget {
	templateNode := ""
	if template != nil {
		templateNode = template.Node
	}

	if len(candidates) == 0 {
		return []cloneAttemptTarget{{
			cloneTarget:   "",
			effectiveNode: templateNode,
		}}
	}

	targets := make([]cloneAttemptTarget, 0, len(candidates))
	for _, candidate := range candidates {
		cloneTarget := candidate.name
		if candidate.name == templateNode {
			cloneTarget = ""
		}

		targets = append(targets, cloneAttemptTarget{
			cloneTarget:   cloneTarget,
			effectiveNode: candidate.name,
		})
	}

	return targets
}

func (ig *InstanceGroup) cloneToTargetWithRetry(
	ctx context.Context,
	template *proxmox.VirtualMachine,
	baseOptions *proxmox.VirtualMachineCloneOptions,
	cloneTarget string,
) (int, *proxmox.Task, error) {
	delay := cloneRetryBaseDelay

	var (
		vmid int
		task *proxmox.Task
		err  error
	)

	for attempt := 0; attempt < cloneAllocateMaxAttempts; attempt++ {
		attemptOptions := *baseOptions
		attemptOptions.NewID = 0
		attemptOptions.Target = cloneTarget

		vmid, task, err = template.Clone(ctx, &attemptOptions)
		if err == nil {
			return vmid, task, nil
		}

		if (!isProxmoxLockError(err) && !isDuplicateVMIDError(err)) || attempt == cloneAllocateMaxAttempts-1 {
			return -1, nil, err
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return -1, nil, err
		case <-timer.C:
		}

		delay *= 2
	}

	return -1, nil, err
}

func (ig *InstanceGroup) getTemplateCloneOptions(template *proxmox.VirtualMachine) (*proxmox.VirtualMachineCloneOptions, error) {
	cloneOptions := &proxmox.VirtualMachineCloneOptions{
		Name:    ig.Settings.InstanceNameCreating,
		Pool:    ig.Settings.Pool,
		Storage: ig.Settings.Storage,
		Full:    true,
		BWLimit: ig.Settings.CloneBWLimit,
	}

	isTemplate := isVMTemplate(template)

	if !isTemplate && ig.Settings.Storage == "" {
		return nil, ErrCloneVMWithoutConfiguredStorage
	}

	if isTemplate && ig.Settings.Storage == "" {
		cloneOptions.Full = false
	}

	return cloneOptions, nil
}

func isVMTemplate(vm *proxmox.VirtualMachine) bool {
	if vm.VirtualMachineConfig != nil {
		return bool(vm.VirtualMachineConfig.Template)
	}

	return bool(vm.Template)
}

func (ig *InstanceGroup) markStaleInstancesForRemoval(ctx context.Context) error {
	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return err
	}

	instancesToMarkForRemoval := make([]*proxmox.ClusterResource, 0, len(pool.Members))

	for _, member := range pool.Members {
		member := member

		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		if !matchesInstanceName(member.Name, ig.Settings.InstanceNameCreating, member.VMID) {
			continue
		}

		ig.logInfo("Found stale instance, marking for removal", "name", member.Name, "vmid", member.VMID, "node", member.Node)
		instancesToMarkForRemoval = append(instancesToMarkForRemoval, &member)
	}

	if len(instancesToMarkForRemoval) < 1 {
		return nil
	}

	if err := ig.markInstancesForRemoval(ctx, instancesToMarkForRemoval...); err != nil {
		return fmt.Errorf("failed to mark stale instances for removal: %w", err)
	}

	return nil
}

func (ig *InstanceGroup) markInstancesForRemoval(ctx context.Context, instances ...*proxmox.ClusterResource) error {
	var errorGroup errgroup.Group

	for _, instance := range instances {
		instance := instance

		errorGroup.Go(func() error {
			vm, err := ig.getProxmoxVMOnNode(ctx, int(instance.VMID), instance.Node)
			if err != nil {
				if isVMNotFoundError(err) || proxmox.IsNotAuthorized(err) {
					ig.deleteInstanceState(instance.VMID)
					ig.logInfo("Instance already removed from proxmox", "vmid", instance.VMID, "node", instance.Node)

					return nil
				}

				ig.logError("Failed to mark instance for removal", "name", instance.Name, "vmid", instance.VMID, "node", instance.Node, "err", err)

				return fmt.Errorf("failed to mark instance for removal: %w", err)
			}

			if vm.Name != "" && !ig.ownsInstanceName(vm.Name, instance.VMID) {
				ig.logWarn("skipping mark-for-removal on recycled VMID owned by foreign name", "vmid", instance.VMID, "name", vm.Name)
				ig.deleteInstanceState(instance.VMID)

				return nil
			}

			removingName := ig.instanceNameForState(ig.Settings.InstanceNameRemoving, int(instance.VMID))
			applyRemovingTags := ig.Settings.InstanceTagsRemoving != "" || ig.Settings.InstanceTagsCreating != "" || ig.Settings.InstanceTagsRunning != ""
			err = ig.configureVMOptions(ctx, vm, removingName, ig.Settings.InstanceTagsRemoving, applyRemovingTags, nil, nil)

			if err != nil && !isVMNotFoundError(err) {
				ig.logError("Failed to mark instance for removal", "name", instance.Name, "vmid", instance.VMID, "node", instance.Node, "err", err)
				return fmt.Errorf("failed to mark instance for removal: %w", err)
			}

			ig.setInstanceState(instance.VMID, provider.StateDeleting)

			return nil
		})
	}

	if err := errorGroup.Wait(); err != nil {
		ig.triggerInstanceCollection()
		return fmt.Errorf("failed to mark one or more instances for removal: %w", err)
	}

	ig.triggerInstanceCollection()

	return nil
}

func (ig *InstanceGroup) triggerInstanceCollection() {
	if ig.instanceCollectionTrigger == nil {
		return
	}

	select {
	case ig.instanceCollectionTrigger <- struct{}{}:
	default:
	}
}

func (ig *InstanceGroup) isProxmoxResourceAnInstance(member proxmox.ClusterResource) bool {
	return member.Type == "qemu" && member.VMID != uint64(*ig.Settings.TemplateID)
}

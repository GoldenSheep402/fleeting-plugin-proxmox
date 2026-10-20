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
)

var ErrCloneVMWithoutConfiguredStorage = errors.New("attempted to clone a VM without configured storage")

func (ig *InstanceGroup) deployInstance(ctx context.Context, template *proxmox.VirtualMachine, cloneMu *sync.Mutex) (int, error) {
	VMID, targetNode, err := ig.cloneTemplate(ctx, template, cloneMu)
	if err != nil {
		return VMID, fmt.Errorf("failed to deploy instance: %w", err)
	}

	defer ig.decrementInFlightClones(targetNode)

	vm, err := ig.findDeployedVM(ctx, VMID, targetNode)
	if err != nil {
		ig.setInstanceState(uint64(VMID), provider.StateDeleting)
		ig.triggerInstanceCollection()

		return VMID, fmt.Errorf("failed to find newly deployed instance vmid='%d': %w", VMID, err)
	}

	if err := ig.startAndConfigureInstance(ctx, vm); err != nil {
		ig.log.Error("instance deployment failed, marking for removal", "vmid", VMID, "err", err)
		ig.markFailedVMForRemoval(vm, VMID)

		return VMID, fmt.Errorf("failed to configure instance, marked for removal due to: %w", err)
	}

	renameTask, renameErr := vm.Config(ctx, proxmox.VirtualMachineOption{
		Name:  "name",
		Value: ig.Settings.InstanceNameRunning,
	})
	if renameErr == nil {
		renameErr = waitForProxmoxTask(ctx, renameTask, proxmoxTaskWaitInterval, proxmoxTaskWaitTimeout)
	}

	if renameErr != nil {
		ig.log.Error("failed to rename instance to running, marking for removal", "vmid", VMID, "err", renameErr)
		ig.markFailedVMForRemoval(vm, VMID)

		return VMID, fmt.Errorf("failed to rename instance, marked for removal due to: %w", renameErr)
	}

	ig.setInstanceState(uint64(VMID), provider.StateRunning)

	return VMID, nil
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

func (ig *InstanceGroup) startAndConfigureInstance(ctx context.Context, vm *proxmox.VirtualMachine) error {
	task, err := vm.Start(ctx)
	if err == nil {
		err = waitForProxmoxTask(ctx, task, proxmoxTaskWaitInterval, proxmoxTaskWaitTimeout)
	}

	if err != nil {
		return fmt.Errorf("failed to start newly deployed instance: %w", err)
	}

	if err := ig.waitForInstanceReady(ctx, vm); err != nil {
		return fmt.Errorf("failed when waiting for qemu agent to start on newly deployed instance: %w", err)
	}

	return nil
}

func (ig *InstanceGroup) waitForInstanceReady(ctx context.Context, vm *proxmox.VirtualMachine) error {
	waitCtx, cancel := context.WithTimeout(ctx, proxmoxAgentStartTimeout)
	defer cancel()

	ticker := time.NewTicker(proxmoxTaskWaitInterval)
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
	ig.setInstanceState(uint64(vmid), provider.StateDeleting)

	cleanupCtx, cancel := context.WithTimeout(context.Background(), proxmoxTaskWaitTimeout)
	defer cancel()

	renameTask, renameErr := vm.Config(cleanupCtx, proxmox.VirtualMachineOption{
		Name:  "name",
		Value: ig.Settings.InstanceNameRemoving,
	})
	if renameErr == nil {
		renameErr = waitForProxmoxTask(cleanupCtx, renameTask, proxmoxTaskWaitInterval, proxmoxTaskWaitTimeout)
	}

	if renameErr != nil {
		ig.log.Error("failed to rename failed instance for removal", "vmid", vmid, "err", renameErr)
	}

	ig.triggerInstanceCollection()
}

func (ig *InstanceGroup) cloneTemplate(ctx context.Context, template *proxmox.VirtualMachine, cloneMu *sync.Mutex) (int, string, error) {
	cloneOptions, err := ig.getTemplateCloneOptions(template)
	if err != nil {
		return -1, "", err
	}

	cloneMu.Lock()
	defer cloneMu.Unlock()

	targetNode := ig.selectTargetNode(ctx, template)
	cloneOptions.Target = targetNode

	VMID, task, err := template.Clone(ctx, cloneOptions)
	if err != nil && targetNode != "" {
		ig.log.Warn("cross-node clone failed, falling back to template node", "target_node", targetNode, "err", err)
		targetNode = ""
		cloneOptions.Target = ""
		cloneOptions.NewID = 0
		VMID, task, err = template.Clone(ctx, cloneOptions)
	}

	if err != nil {
		return -1, "", fmt.Errorf("failed to clone the template: %w", err)
	}

	if targetNode == "" {
		targetNode = template.Node
	}

	ig.log.Info("Deploying new instance", "vmid", VMID, "node", targetNode)
	ig.setInstanceState(uint64(VMID), provider.StateCreating)
	ig.incrementInFlightClones(targetNode)

	if err := waitForProxmoxTask(ctx, task, proxmoxTaskWaitInterval, proxmoxTaskWaitTimeout); err != nil {
		ig.decrementInFlightClones(targetNode)
		ig.setInstanceState(uint64(VMID), provider.StateDeleting)
		ig.triggerInstanceCollection()

		return VMID, targetNode, fmt.Errorf("failed waiting for clone task: %w", err)
	}

	return VMID, targetNode, nil
}

func (ig *InstanceGroup) getTemplateCloneOptions(template *proxmox.VirtualMachine) (*proxmox.VirtualMachineCloneOptions, error) {
	cloneOptions := &proxmox.VirtualMachineCloneOptions{
		Name:    ig.Settings.InstanceNameCreating,
		Pool:    ig.Settings.Pool,
		Storage: ig.Settings.Storage,
		Full:    true,
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

		if member.Name != ig.Settings.InstanceNameCreating {
			continue
		}

		ig.log.Info("Found stale instance, marking for removal", "name", member.Name, "vmid", member.VMID, "node", member.Node)
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
			log := ig.log.With("name", instance.Name, "vmid", instance.VMID, "node", instance.Node)

			vm, err := ig.getProxmoxVMOnNode(ctx, int(instance.VMID), instance.Node)
			if err != nil {
				log.Error("Failed to mark instance for removal", "err", err)
				return fmt.Errorf("failed to mark instance for removal: %w", err)
			}

			task, err := vm.Config(ctx, proxmox.VirtualMachineOption{
				Name:  "name",
				Value: ig.Settings.InstanceNameRemoving,
			})

			if err == nil {
				err = waitForProxmoxTask(ctx, task, proxmoxTaskWaitInterval, proxmoxTaskWaitTimeout)
			}

			if err != nil {
				log.Error("Failed to mark instance for removal", "err", err)
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
	select {
	case ig.instanceCollectionTrigger <- struct{}{}:
	default:
	}
}

func (ig *InstanceGroup) isProxmoxResourceAnInstance(member proxmox.ClusterResource) bool {
	return member.Type == "qemu" && member.VMID != uint64(*ig.Settings.TemplateID)
}

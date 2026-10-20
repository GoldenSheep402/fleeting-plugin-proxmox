package plugin

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"golang.org/x/sync/errgroup"
)

var _ provider.InstanceGroup = (*InstanceGroup)(nil)

const triggerChannelCapacity = 100

type InstanceGroup struct {
	Settings         `json:",inline"`
	FleetingSettings provider.Settings `json:"-"`

	log     hclog.Logger    `json:"-"`
	proxmox *proxmox.Client `json:"-"`

	// This mutex serializes template cloning operations so Proxmox does not
	// fail concurrent clone tasks or allocate duplicate VMIDs.
	instanceCloningMu sync.Mutex `json:"-"`

	// Protects in-memory state tracking for instances and node scheduling counters.
	stateMu             sync.RWMutex              `json:"-"`
	instanceStates      map[uint64]provider.State `json:"-"`
	collectingInstances map[uint64]struct{}       `json:"-"`
	inFlightNodeClones  map[string]int            `json:"-"`

	// Trigger for collector to start removed instances collection.
	instanceCollectionTrigger chan struct{} `json:"-"`

	// Trigger to shutdown collector.
	collectorShutdownTrigger chan struct{} `json:"-"`

	// Wait group for the collector.
	collectorWaitGroup sync.WaitGroup `json:"-"`

	// Trigger to shutdown session ticket refresher.
	sessionTicketRefresherShutdownTrigger chan struct{} `json:"-"`

	// Wait group for session ticket refresher.
	sessionTicketRefresherWaitGroup sync.WaitGroup `json:"-"`
}

// Init implements provider.InstanceGroup.
func (ig *InstanceGroup) Init(ctx context.Context, logger hclog.Logger, settings provider.Settings) (provider.ProviderInfo, error) {
	var err error

	ig.log = logger
	ig.FleetingSettings = settings
	ig.instanceStates = make(map[uint64]provider.State)
	ig.collectingInstances = make(map[uint64]struct{})
	ig.inFlightNodeClones = make(map[string]int)
	ig.instanceCollectionTrigger = make(chan struct{}, triggerChannelCapacity)
	ig.collectorShutdownTrigger = make(chan struct{}, 1)
	ig.sessionTicketRefresherShutdownTrigger = make(chan struct{}, 1)

	if err := ig.Settings.CheckRequiredFields(); err != nil {
		return provider.ProviderInfo{}, err
	}

	ig.Settings.FillWithDefaults()

	if ig.Settings.InsecureSkipTLSVerify {
		ig.log.Warn("TLS verification for Proxmox client is disabled, connections will be insecure")
	}

	ig.proxmox, err = ig.getProxmoxClient()
	if err != nil {
		return provider.ProviderInfo{}, err
	}

	if err := ig.markStaleInstancesForRemoval(ctx); err != nil {
		return provider.ProviderInfo{}, err
	}

	//nolint:contextcheck
	ig.startRemovedInstanceCollector()

	//nolint:contextcheck
	ig.startSessionTicketRefresher()

	return provider.ProviderInfo{
		ID:      ig.Settings.Pool,
		MaxSize: *ig.Settings.MaxInstances,
	}, nil
}

// Shutdown implements provider.InstanceGroup.
func (ig *InstanceGroup) Shutdown(_ context.Context) error {
	ig.collectorShutdownTrigger <- struct{}{}
	ig.sessionTicketRefresherShutdownTrigger <- struct{}{}

	ig.collectorWaitGroup.Wait()
	ig.sessionTicketRefresherWaitGroup.Wait()

	return nil
}

// Increase implements provider.InstanceGroup.
func (ig *InstanceGroup) Increase(ctx context.Context, count int) (int, error) {
	template, err := ig.getProxmoxVM(ctx, *ig.Settings.TemplateID)
	if err != nil {
		return 0, fmt.Errorf("failed to find template with id='%d': %w", *ig.Settings.TemplateID, err)
	}

	var (
		errorGroup  = new(errgroup.Group)
		succeeded   = 0
		succeededMu = new(sync.Mutex)
	)

	for range count {
		errorGroup.Go(func() error {
			vmid, err := ig.deployInstance(ctx, template, &ig.instanceCloningMu)
			if err != nil {
				ig.log.Error("failed to deploy an instance", "vmid", vmid, "err", err)
				return err
			}

			ig.log.Info("successfully deployed instance", "vmid", vmid)
			succeededMu.Lock()
			succeeded++
			succeededMu.Unlock()

			return nil
		})
	}

	if err := errorGroup.Wait(); err != nil {
		return succeeded, fmt.Errorf("failed to create one or more instances: %w", err)
	}

	return succeeded, nil
}

// Update implements provider.InstanceGroup.
func (ig *InstanceGroup) Update(ctx context.Context, update func(instance string, state provider.State)) error {
	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return err
	}

	seenVMIDs := make(map[uint64]struct{}, len(pool.Members))

	for _, member := range pool.Members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		seenVMIDs[member.VMID] = struct{}{}

		state, ok := ig.resolveInstanceState(ctx, member)
		if !ok {
			continue
		}

		update(strconv.FormatUint(member.VMID, 10), state)
	}

	ig.cleanupDeletedInstanceStates(seenVMIDs)

	return nil
}

// ConnectInfo implements provider.InstanceGroup.
func (ig *InstanceGroup) ConnectInfo(ctx context.Context, instance string) (provider.ConnectInfo, error) {
	VMID, err := strconv.Atoi(instance)
	if err != nil {
		return provider.ConnectInfo{}, fmt.Errorf("failed to parse instance name '%s': %w", instance, err)
	}

	vm, err := ig.getProxmoxVM(ctx, VMID)
	if err != nil {
		return provider.ConnectInfo{}, fmt.Errorf("failed to retrieve instance vmid='%d': %w", VMID, err)
	}

	networkInterfaces, err := vm.AgentGetNetworkIFaces(ctx)
	if err != nil {
		return provider.ConnectInfo{}, fmt.Errorf("failed to retrieve instance vmid='%d' interfaces: %w", VMID, err)
	}

	internalAddress, externalAddress, err := determineAddresses(networkInterfaces, ig.InstanceNetworkInterface, ig.InstanceNetworkProtocol)
	if err != nil {
		return provider.ConnectInfo{}, err
	}

	return provider.ConnectInfo{
		ID:              instance,
		InternalAddr:    internalAddress,
		ExternalAddr:    externalAddress,
		ConnectorConfig: ig.FleetingSettings.ConnectorConfig,
	}, nil
}

// Decrease implements provider.InstanceGroup.
func (ig *InstanceGroup) Decrease(ctx context.Context, instancesToRemove []string) ([]string, error) {
	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return []string{}, err
	}

	var (
		errorGroup  = new(errgroup.Group)
		succeeded   = []string{}
		succeededMu = new(sync.Mutex)
	)

	for _, member := range pool.Members {
		member := member

		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		if !slices.Contains(instancesToRemove, strconv.FormatUint(member.VMID, 10)) {
			continue
		}

		if ig.isInstanceCreating(member) {
			// It must finish creation before starting deletion
			continue
		}

		if ig.isInstanceDeleting(member) {
			// Already deleting...
			succeededMu.Lock()
			succeeded = append(succeeded, strconv.FormatUint(member.VMID, 10))
			succeededMu.Unlock()

			continue
		}

		ig.log.Info("removing instance", "vmid", member.VMID)

		errorGroup.Go(func() error {
			if err := ig.markInstancesForRemoval(ctx, &member); err != nil {
				return err
			}

			succeededMu.Lock()
			defer succeededMu.Unlock()
			succeeded = append(succeeded, strconv.FormatUint(member.VMID, 10))

			return nil
		})
	}

	//nolint:wrapcheck
	return succeeded, errorGroup.Wait()
}

func (ig *InstanceGroup) resolveInstanceState(ctx context.Context, member proxmox.ClusterResource) (provider.State, bool) {
	if trackedState, ok := ig.getInstanceState(member.VMID); ok {
		if trackedState == provider.StateRunning && member.Status == proxmox.StatusVirtualMachineStopped {
			if ig.handleUnexpectedlyStoppedInstance(ctx, member) {
				return provider.StateDeleting, true
			}
		}

		return trackedState, true
	}

	switch member.Name {
	case ig.Settings.InstanceNameCreating:
		return provider.StateCreating, true
	case ig.Settings.InstanceNameRunning:
		if member.Status == proxmox.StatusVirtualMachineStopped {
			if ig.handleUnexpectedlyStoppedInstance(ctx, member) {
				return provider.StateDeleting, true
			}
		}

		return provider.StateRunning, true
	case ig.Settings.InstanceNameRemoving:
		return provider.StateDeleting, true
	default:
		return "", false
	}
}

func (ig *InstanceGroup) handleUnexpectedlyStoppedInstance(ctx context.Context, member proxmox.ClusterResource) bool {
	vm, err := ig.getProxmoxVMOnNode(ctx, int(member.VMID), member.Node)
	if err != nil || vm.Status != proxmox.StatusVirtualMachineStopped {
		return false
	}

	ig.log.Warn("instance marked as running is stopped, marking for removal", "vmid", member.VMID, "node", member.Node)
	memberCopy := member

	if markErr := ig.markInstancesForRemoval(ctx, &memberCopy); markErr != nil {
		ig.log.Error("failed to mark stopped instance for removal", "vmid", member.VMID, "err", markErr)
	}

	return true
}

func (ig *InstanceGroup) isInstanceCreating(member proxmox.ClusterResource) bool {
	if state, ok := ig.getInstanceState(member.VMID); ok {
		return state == provider.StateCreating
	}

	return member.Name == ig.Settings.InstanceNameCreating
}

func (ig *InstanceGroup) isInstanceDeleting(member proxmox.ClusterResource) bool {
	if state, ok := ig.getInstanceState(member.VMID); ok {
		return state == provider.StateDeleting
	}

	return member.Name == ig.Settings.InstanceNameRemoving
}

func (ig *InstanceGroup) setInstanceState(vmid uint64, state provider.State) {
	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	if ig.instanceStates == nil {
		ig.instanceStates = make(map[uint64]provider.State)
	}

	ig.instanceStates[vmid] = state
}

func (ig *InstanceGroup) getInstanceState(vmid uint64) (provider.State, bool) {
	ig.stateMu.RLock()
	defer ig.stateMu.RUnlock()

	if ig.instanceStates == nil {
		return "", false
	}

	state, ok := ig.instanceStates[vmid]

	return state, ok
}

func (ig *InstanceGroup) cleanupDeletedInstanceStates(seenVMIDs map[uint64]struct{}) {
	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	for vmid, state := range ig.instanceStates {
		if _, exists := seenVMIDs[vmid]; !exists && state == provider.StateDeleting {
			delete(ig.instanceStates, vmid)
		}
	}
}

func (ig *InstanceGroup) incrementInFlightClones(nodeName string) {
	if nodeName == "" {
		return
	}

	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	if ig.inFlightNodeClones == nil {
		ig.inFlightNodeClones = make(map[string]int)
	}

	ig.inFlightNodeClones[nodeName]++
}

func (ig *InstanceGroup) decrementInFlightClones(nodeName string) {
	if nodeName == "" {
		return
	}

	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	if ig.inFlightNodeClones == nil {
		return
	}

	ig.inFlightNodeClones[nodeName]--
	if ig.inFlightNodeClones[nodeName] <= 0 {
		delete(ig.inFlightNodeClones, nodeName)
	}
}

func (ig *InstanceGroup) getInFlightClones(nodeName string) int {
	ig.stateMu.RLock()
	defer ig.stateMu.RUnlock()

	if ig.inFlightNodeClones == nil {
		return 0
	}

	return ig.inFlightNodeClones[nodeName]
}

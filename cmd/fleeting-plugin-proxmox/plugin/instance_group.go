package plugin

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"golang.org/x/sync/errgroup"
)

var _ provider.InstanceGroup = (*InstanceGroup)(nil)

const triggerChannelCapacity = 100

type trackedInstanceState struct {
	state     provider.State
	updatedAt time.Time
}

type InstanceGroup struct {
	Settings         `json:",inline"`
	FleetingSettings provider.Settings `json:"-"`

	log     hclog.Logger    `json:"-"`
	proxmox *proxmox.Client `json:"-"`

	clientMu        sync.RWMutex      `json:"-"`
	proxmoxClients  []*proxmox.Client `json:"-"`
	proxmoxURLs     []string          `json:"-"`
	activeClientIdx int               `json:"-"`
	useAPIToken     bool              `json:"-"`

	baseCtx    context.Context    `json:"-"`
	baseCancel context.CancelFunc `json:"-"`

	// This mutex serializes the clone allocation critical section so Proxmox
	// does not allocate duplicate VMIDs or race on per-node in-flight counts.
	instanceCloningMu sync.Mutex    `json:"-"`
	cloneSem          chan struct{} `json:"-"`

	// Protects in-memory state tracking for instances and node scheduling counters.
	stateMu             sync.RWMutex                    `json:"-"`
	instanceStates      map[uint64]trackedInstanceState `json:"-"`
	collectingInstances map[uint64]struct{}             `json:"-"`
	inFlightNodeClones  map[string]int                  `json:"-"`
	activeDeploys       map[uint64]struct{}             `json:"-"`
	staleCreatingSeen   map[uint64]time.Time            `json:"-"`

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
	ig.log = logger
	ig.FleetingSettings = settings
	ig.baseCtx, ig.baseCancel = context.WithCancel(context.Background())
	ig.instanceStates = make(map[uint64]trackedInstanceState)
	ig.collectingInstances = make(map[uint64]struct{})
	ig.inFlightNodeClones = make(map[string]int)
	ig.activeDeploys = make(map[uint64]struct{})
	ig.staleCreatingSeen = make(map[uint64]time.Time)
	ig.instanceCollectionTrigger = make(chan struct{}, triggerChannelCapacity)
	ig.collectorShutdownTrigger = make(chan struct{}, 1)
	ig.sessionTicketRefresherShutdownTrigger = make(chan struct{}, 1)

	if err := ig.Settings.CheckRequiredFields(); err != nil {
		return provider.ProviderInfo{}, err
	}

	ig.Settings.FillWithDefaults()
	ig.cloneSem = make(chan struct{}, *ig.Settings.MaxConcurrentClones)

	if ig.Settings.InsecureSkipTLSVerify {
		ig.logWarn("TLS verification for Proxmox client is disabled, connections will be insecure")
	}

	if err := ig.initProxmoxClients(); err != nil {
		return provider.ProviderInfo{}, err
	}

	if err := ig.markStaleInstancesForRemoval(ctx); err != nil {
		return provider.ProviderInfo{}, err
	}

	//nolint:contextcheck
	ig.startRemovedInstanceCollector()

	if ig.useAPIToken {
		ig.logInfo("using proxmox API token authentication, skipping session ticket refresher")
	} else {
		//nolint:contextcheck
		ig.startSessionTicketRefresher()
	}

	return provider.ProviderInfo{
		ID:      ig.Settings.Pool,
		MaxSize: *ig.Settings.MaxInstances,
	}, nil
}

// Shutdown implements provider.InstanceGroup.
func (ig *InstanceGroup) Shutdown(ctx context.Context) error {
	if ig.baseCancel != nil {
		ig.baseCancel()
	}

	if ig.collectorShutdownTrigger != nil {
		select {
		case ig.collectorShutdownTrigger <- struct{}{}:
		default:
		}
	}

	if ig.sessionTicketRefresherShutdownTrigger != nil {
		select {
		case ig.sessionTicketRefresherShutdownTrigger <- struct{}{}:
		default:
		}
	}

	doneCh := make(chan struct{})
	go func() {
		ig.collectorWaitGroup.Wait()
		ig.sessionTicketRefresherWaitGroup.Wait()
		close(doneCh)
	}()

	if ctx == nil {
		<-doneCh
		return nil
	}

	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Increase implements provider.InstanceGroup.
func (ig *InstanceGroup) Increase(ctx context.Context, count int) (int, error) {
	if count <= 0 {
		return 0, nil
	}

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
				ig.logError("failed to deploy an instance", "vmid", vmid, "err", err)
				return err
			}

			ig.logInfo("successfully deployed instance", "vmid", vmid)
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
	if len(instancesToRemove) == 0 {
		return []string{}, nil
	}

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

		if member.Name != "" && !ig.ownsInstanceName(member.Name, member.VMID) {
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

		ig.logInfo("removing instance", "vmid", member.VMID)

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

func (ig *InstanceGroup) lifecycleContext() context.Context {
	if ig.baseCtx != nil {
		return ig.baseCtx
	}

	return context.Background()
}

func (ig *InstanceGroup) instanceNameForState(baseName string, vmid int) string {
	if baseName == "" || !ig.Settings.InstanceNameIncludeVMID || vmid <= 0 {
		return baseName
	}

	return fmt.Sprintf("%s-%d", baseName, vmid)
}

func matchesInstanceName(memberName, configuredName string, vmid uint64) bool {
	if configuredName == "" {
		return false
	}

	if memberName == configuredName {
		return true
	}

	return memberName == fmt.Sprintf("%s-%d", configuredName, vmid)
}

func (ig *InstanceGroup) matchInstanceStateByName(member proxmox.ClusterResource) (provider.State, bool) {
	switch member.Name {
	case ig.Settings.InstanceNameCreating:
		return provider.StateCreating, true
	case ig.Settings.InstanceNameRunning:
		return provider.StateRunning, true
	case ig.Settings.InstanceNameRemoving:
		return provider.StateDeleting, true
	}

	if matchesInstanceName(member.Name, ig.Settings.InstanceNameCreating, member.VMID) {
		return provider.StateCreating, true
	}

	if matchesInstanceName(member.Name, ig.Settings.InstanceNameRunning, member.VMID) {
		return provider.StateRunning, true
	}

	if matchesInstanceName(member.Name, ig.Settings.InstanceNameRemoving, member.VMID) {
		return provider.StateDeleting, true
	}

	return "", false
}

func (ig *InstanceGroup) ownsInstanceName(memberName string, vmid uint64) bool {
	if ig.Settings.InstanceNameCreating == "" && ig.Settings.InstanceNameRunning == "" && ig.Settings.InstanceNameRemoving == "" {
		return true
	}

	_, ok := ig.matchInstanceStateByName(proxmox.ClusterResource{
		VMID: vmid,
		Name: memberName,
	})

	return ok
}

func (ig *InstanceGroup) resolveInstanceState(ctx context.Context, member proxmox.ClusterResource) (provider.State, bool) {
	if member.Name != "" && !ig.ownsInstanceName(member.Name, member.VMID) {
		if !ig.isActiveDeploy(member.VMID) {
			ig.deleteInstanceState(member.VMID)
		}

		return "", false
	}

	if trackedState, ok := ig.getInstanceState(member.VMID); ok {
		if trackedState == provider.StateRunning && member.Status == proxmox.StatusVirtualMachineStopped {
			if ig.handleUnexpectedlyStoppedInstance(ctx, member) {
				return provider.StateDeleting, true
			}
		}

		return trackedState, true
	}

	state, ok := ig.matchInstanceStateByName(member)
	if !ok {
		return "", false
	}

	if state == provider.StateRunning && member.Status == proxmox.StatusVirtualMachineStopped {
		if ig.handleUnexpectedlyStoppedInstance(ctx, member) {
			return provider.StateDeleting, true
		}
	}

	return state, true
}

func (ig *InstanceGroup) handleUnexpectedlyStoppedInstance(ctx context.Context, member proxmox.ClusterResource) bool {
	vm, err := ig.getProxmoxVMOnNode(ctx, int(member.VMID), member.Node)
	if err != nil || vm.Status != proxmox.StatusVirtualMachineStopped {
		return false
	}

	ig.logWarn("instance marked as running is stopped, marking for removal", "vmid", member.VMID, "node", member.Node)
	memberCopy := member

	if markErr := ig.markInstancesForRemoval(ctx, &memberCopy); markErr != nil {
		ig.logError("failed to mark stopped instance for removal", "vmid", member.VMID, "err", markErr)
	}

	return true
}

func (ig *InstanceGroup) isInstanceCreating(member proxmox.ClusterResource) bool {
	if member.Name != "" && !ig.ownsInstanceName(member.Name, member.VMID) {
		return false
	}

	if state, ok := ig.getInstanceState(member.VMID); ok {
		return state == provider.StateCreating
	}

	state, ok := ig.matchInstanceStateByName(member)

	return ok && state == provider.StateCreating
}

func (ig *InstanceGroup) isInstanceDeleting(member proxmox.ClusterResource) bool {
	if member.Name != "" && !ig.ownsInstanceName(member.Name, member.VMID) {
		return false
	}

	if state, ok := ig.getInstanceState(member.VMID); ok {
		return state == provider.StateDeleting
	}

	state, ok := ig.matchInstanceStateByName(member)

	return ok && state == provider.StateDeleting
}

func (ig *InstanceGroup) setInstanceState(vmid uint64, state provider.State) {
	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	if ig.instanceStates == nil {
		ig.instanceStates = make(map[uint64]trackedInstanceState)
	}

	ig.instanceStates[vmid] = trackedInstanceState{
		state:     state,
		updatedAt: time.Now(),
	}

	if state != provider.StateCreating && ig.staleCreatingSeen != nil {
		delete(ig.staleCreatingSeen, vmid)
	}
}

func (ig *InstanceGroup) getInstanceState(vmid uint64) (provider.State, bool) {
	ig.stateMu.RLock()
	defer ig.stateMu.RUnlock()

	if ig.instanceStates == nil {
		return "", false
	}

	entry, ok := ig.instanceStates[vmid]

	return entry.state, ok
}

func (ig *InstanceGroup) deleteInstanceState(vmid uint64) {
	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	if ig.instanceStates != nil {
		delete(ig.instanceStates, vmid)
	}

	if ig.staleCreatingSeen != nil {
		delete(ig.staleCreatingSeen, vmid)
	}
}

func (ig *InstanceGroup) markActiveDeploy(vmid uint64) {
	if vmid == 0 {
		return
	}

	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	if ig.activeDeploys == nil {
		ig.activeDeploys = make(map[uint64]struct{})
	}

	ig.activeDeploys[vmid] = struct{}{}
}

func (ig *InstanceGroup) unmarkActiveDeploy(vmid uint64) {
	if vmid == 0 {
		return
	}

	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	if ig.activeDeploys == nil {
		return
	}

	delete(ig.activeDeploys, vmid)
}

func (ig *InstanceGroup) isActiveDeploy(vmid uint64) bool {
	ig.stateMu.RLock()
	defer ig.stateMu.RUnlock()

	if ig.activeDeploys == nil {
		return false
	}

	_, active := ig.activeDeploys[vmid]

	return active
}

func (ig *InstanceGroup) cleanupDeletedInstanceStates(seenVMIDs map[uint64]struct{}) {
	ig.stateMu.Lock()
	defer ig.stateMu.Unlock()

	for vmid := range ig.instanceStates {
		if _, exists := seenVMIDs[vmid]; exists {
			continue
		}

		if _, active := ig.activeDeploys[vmid]; active {
			continue
		}

		delete(ig.instanceStates, vmid)
	}

	for vmid := range ig.staleCreatingSeen {
		if _, exists := seenVMIDs[vmid]; !exists {
			delete(ig.staleCreatingSeen, vmid)
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

func (ig *InstanceGroup) acquireCloneSlot(ctx context.Context) error {
	if ig.cloneSem == nil {
		return nil
	}

	select {
	case ig.cloneSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ig *InstanceGroup) releaseCloneSlot() {
	if ig.cloneSem == nil {
		return
	}

	select {
	case <-ig.cloneSem:
	default:
	}
}

func (ig *InstanceGroup) logDebug(msg string, args ...any) {
	if ig.log != nil {
		ig.log.Debug(msg, args...)
	}
}

func (ig *InstanceGroup) logInfo(msg string, args ...any) {
	if ig.log != nil {
		ig.log.Info(msg, args...)
	}
}

func (ig *InstanceGroup) logWarn(msg string, args ...any) {
	if ig.log != nil {
		ig.log.Warn(msg, args...)
	}
}

func (ig *InstanceGroup) logError(msg string, args ...any) {
	if ig.log != nil {
		ig.log.Error(msg, args...)
	}
}

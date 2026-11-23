package plugin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

func TestUnmarshallingPluginSettings(t *testing.T) {
	settingsJSON := `{"url":"sample_url","template_id": 5}`
	instance := InstanceGroup{}

	err := json.Unmarshal([]byte(settingsJSON), &instance)
	require.NoError(t, err)

	require.Equal(t, "sample_url", instance.Settings.URL)
	require.Equal(t, 5, *instance.Settings.TemplateID)
}

func TestInstanceGroup_StateTrackingAndNodeClones(t *testing.T) {
	ig := InstanceGroup{
		Settings: Settings{
			InstanceNameCreating: DefaultInstanceNameCreating,
			InstanceNameRunning:  DefaultInstanceNameRunning,
			InstanceNameRemoving: DefaultInstanceNameRemoving,
		},
	}

	ig.setInstanceState(101, provider.StateCreating)
	state, ok := ig.getInstanceState(101)
	require.True(t, ok)
	require.Equal(t, provider.StateCreating, state)

	member := proxmox.ClusterResource{
		VMID: 101,
		Name: DefaultInstanceNameRemoving, // stale name in Proxmox pool
	}
	require.True(t, ig.isInstanceCreating(member))
	require.False(t, ig.isInstanceDeleting(member))

	ig.setInstanceState(101, provider.StateDeleting)
	require.True(t, ig.isInstanceDeleting(member))

	ig.cleanupDeletedInstanceStates(map[uint64]struct{}{})
	_, ok = ig.getInstanceState(101)
	require.False(t, ok)

	ig.incrementInFlightClones("pve1")
	ig.incrementInFlightClones("pve1")
	require.Equal(t, 2, ig.getInFlightClones("pve1"))

	ig.decrementInFlightClones("pve1")
	require.Equal(t, 1, ig.getInFlightClones("pve1"))

	ig.decrementInFlightClones("pve1")
	require.Equal(t, 0, ig.getInFlightClones("pve1"))
}

func TestInstanceGroup_CleanupDeletedInstanceStates_ProtectsActiveDeploys(t *testing.T) {
	ig := InstanceGroup{}

	ig.setInstanceState(101, provider.StateRunning)
	ig.setInstanceState(102, provider.StateCreating)
	ig.setInstanceState(103, provider.StateRunning)
	ig.markActiveDeploy(102)

	// 101 was manually deleted externally (absent from seenVMIDs, not active deploy) -> purged.
	// 102 is active deploy (absent from seenVMIDs) -> protected.
	// 103 is still in seenVMIDs -> retained.
	ig.cleanupDeletedInstanceStates(map[uint64]struct{}{103: {}})

	_, exists101 := ig.getInstanceState(101)
	require.False(t, exists101)

	state102, exists102 := ig.getInstanceState(102)
	require.True(t, exists102)
	require.Equal(t, provider.StateCreating, state102)

	state103, exists103 := ig.getInstanceState(103)
	require.True(t, exists103)
	require.Equal(t, provider.StateRunning, state103)

	// After deploy completes and still absent from seenVMIDs, 102 is purged too.
	ig.unmarkActiveDeploy(102)
	ig.cleanupDeletedInstanceStates(map[uint64]struct{}{103: {}})
	_, exists102 = ig.getInstanceState(102)
	require.False(t, exists102)
}

func TestInstanceGroup_UniqueInstanceNamingAndMatching(t *testing.T) {
	ig := InstanceGroup{
		Settings: Settings{
			InstanceNameCreating:    "fleeting-creating",
			InstanceNameRunning:     "fleeting-running",
			InstanceNameRemoving:    "fleeting-removing",
			InstanceNameIncludeVMID: true,
		},
	}

	require.Equal(t, "fleeting-running-101", ig.instanceNameForState(ig.Settings.InstanceNameRunning, 101))

	require.True(t, matchesInstanceName("fleeting-creating", "fleeting-creating", 101))
	require.True(t, matchesInstanceName("fleeting-creating-101", "fleeting-creating", 101))
	require.False(t, matchesInstanceName("fleeting-creating-1010", "fleeting-creating", 101))

	state, ok := ig.matchInstanceStateByName(proxmox.ClusterResource{VMID: 101, Name: "fleeting-running-101"})
	require.True(t, ok)
	require.Equal(t, provider.StateRunning, state)

	state, ok = ig.matchInstanceStateByName(proxmox.ClusterResource{VMID: 102, Name: "fleeting-removing-102"})
	require.True(t, ok)
	require.Equal(t, provider.StateDeleting, state)

	_, ok = ig.matchInstanceStateByName(proxmox.ClusterResource{VMID: 101, Name: "fleeting-running-999"})
	require.False(t, ok)
}

func TestInstanceGroup_ShutdownRespectsContextDeadline(t *testing.T) {
	ig := InstanceGroup{}
	ig.baseCtx, ig.baseCancel = context.WithCancel(context.Background())

	releaseWorker := make(chan struct{})
	ig.collectorWaitGroup.Add(1)
	go func() {
		defer ig.collectorWaitGroup.Done()
		<-releaseWorker
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := ig.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, ig.baseCtx.Err(), context.Canceled)

	close(releaseWorker)
	ig.collectorWaitGroup.Wait()
}

package plugin

import (
	"encoding/json"
	"testing"

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

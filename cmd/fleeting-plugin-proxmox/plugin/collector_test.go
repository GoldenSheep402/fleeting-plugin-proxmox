package plugin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

func TestCollector_CollectInstance_PausedAndNotFound(t *testing.T) {
	ctx := context.Background()

	var (
		stoppedCalled atomic.Bool
		deletedCalled atomic.Bool
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/api2/json/nodes/pve1/qemu/101/status/current":
			_, _ = writer.Write([]byte(`{"data":{"vmid":101,"name":"fleeting-removing","status":"paused"}}`))
		case "/api2/json/nodes/pve1/qemu/101/config":
			_, _ = writer.Write([]byte(`{"data":{"name":"fleeting-removing"}}`))
		case "/api2/json/nodes/pve1/qemu/101/status/stop":
			stoppedCalled.Store(true)
			_, _ = writer.Write([]byte(`{"data":"UPID:pve1:00001234:00005678:65000001:qmstop:101:root@pam:"}`))
		case "/api2/json/nodes/pve1/qemu/101":
			if req.Method == http.MethodDelete {
				deletedCalled.Store(true)
				_, _ = writer.Write([]byte(`{"data":"UPID:pve1:00001234:00005678:65000002:qmdestroy:101:root@pam:"}`))
				return
			}
			writer.WriteHeader(http.StatusNotFound)
		case "/api2/json/nodes/pve1/tasks/UPID:pve1:00001234:00005678:65000001:qmstop:101:root@pam:/status",
			"/api2/json/nodes/pve1/tasks/UPID:pve1:00001234:00005678:65000002:qmdestroy:101:root@pam:/status":
			_, _ = writer.Write([]byte(`{"data":{"status":"stopped","exitstatus":"OK"}}`))
		case "/api2/json/nodes/pve1/qemu/404/status/current":
			writer.WriteHeader(http.StatusNotFound)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	taskInterval := 1
	ig := InstanceGroup{
		Settings: Settings{
			ProxmoxTaskWaitInterval: &taskInterval,
		},
	}
	tlsConfig, err := ig.buildTLSConfig()
	require.NoError(t, err)
	client, err := ig.buildProxmoxClientForURL(server.URL, &proxmoxCredentials{TokenID: "ci@pve!t", Secret: "s"}, tlsConfig)
	require.NoError(t, err)
	ig.proxmox = client

	// Paused VM must be stopped then deleted.
	ig.collectInstance(ctx, proxmox.ClusterResource{VMID: 101, Node: "pve1", Name: "fleeting-removing"})
	require.True(t, stoppedCalled.Load())
	require.True(t, deletedCalled.Load())

	// Missing VM (404) must complete cleanly without panics or errors.
	ig.collectInstance(ctx, proxmox.ClusterResource{VMID: 404, Node: "pve1", Name: "fleeting-removing"})
}

func TestCollector_ReapOrphanedCreatingInstances(t *testing.T) {
	ctx := context.Background()

	var renamedVM202 atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/api2/json/nodes/pve1/qemu/202/status/current":
			_, _ = writer.Write([]byte(`{"data":{"vmid":202,"name":"fleeting-creating","status":"stopped"}}`))
		case "/api2/json/nodes/pve1/qemu/202/config":
			if req.Method == http.MethodPost || req.Method == http.MethodPut {
				renamedVM202.Store(true)
				_, _ = writer.Write([]byte(`{"data":null}`))
				return
			}
			_, _ = writer.Write([]byte(`{"data":{"name":"fleeting-creating"}}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := proxmox.NewClient(server.URL + "/api2/json")
	staleTimeout := 60
	templateID := 9000

	ig := InstanceGroup{
		proxmox: client,
		Settings: Settings{
			TemplateID:           &templateID,
			InstanceNameCreating: DefaultInstanceNameCreating,
			InstanceNameRunning:  DefaultInstanceNameRunning,
			InstanceNameRemoving: DefaultInstanceNameRemoving,
			StaleCreatingTimeout: &staleTimeout,
		},
	}

	// 201 is actively deploying, 202 is orphaned creating.
	ig.markActiveDeploy(201)

	members := []proxmox.ClusterResource{
		{Type: "qemu", VMID: 201, Node: "pve1", Name: "fleeting-creating"},
		{Type: "qemu", VMID: 202, Node: "pve1", Name: "fleeting-creating"},
	}

	start := time.Now()
	// First observation records timestamp without marking for removal yet.
	ig.reapOrphanedCreatingInstances(ctx, members, start)
	require.False(t, renamedVM202.Load())

	// Before timeout elapses, still not reaped.
	ig.reapOrphanedCreatingInstances(ctx, members, start.Add(30*time.Second))
	require.False(t, renamedVM202.Load())

	// Past timeout: orphaned 202 is marked for removal, active deploy 201 is untouched.
	ig.reapOrphanedCreatingInstances(ctx, members, start.Add(65*time.Second))
	require.True(t, renamedVM202.Load())

	state202, ok := ig.getInstanceState(202)
	require.True(t, ok)
	require.Equal(t, provider.StateDeleting, state202)

	_, has201Deleting := ig.getInstanceState(201)
	require.False(t, has201Deleting)
}

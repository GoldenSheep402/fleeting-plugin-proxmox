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

func TestCollector_CollectInstance_ClearsStateAndSkipsForeignVM(t *testing.T) {
	ctx := context.Background()

	var (
		touchedForeign117 atomic.Bool
		deletedOwn118     atomic.Bool
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/api2/json/nodes/pve1/qemu/117/status/current":
			_, _ = writer.Write([]byte(`{"data":{"vmid":117,"name":"gitlab-fleeting-epyc-running-117","status":"running"}}`))
		case "/api2/json/nodes/pve1/qemu/117/config":
			_, _ = writer.Write([]byte(`{"data":{"name":"gitlab-fleeting-epyc-running-117"}}`))
		case "/api2/json/nodes/pve1/qemu/117/status/stop",
			"/api2/json/nodes/pve1/qemu/117":
			touchedForeign117.Store(true)
			writer.WriteHeader(http.StatusInternalServerError)
		case "/api2/json/nodes/pve1/qemu/118/status/current":
			_, _ = writer.Write([]byte(`{"data":{"vmid":118,"name":"gitlab-fleeting-win-removing-118","status":"stopped"}}`))
		case "/api2/json/nodes/pve1/qemu/118/config":
			_, _ = writer.Write([]byte(`{"data":{"name":"gitlab-fleeting-win-removing-118"}}`))
		case "/api2/json/nodes/pve1/qemu/118":
			if req.Method == http.MethodDelete {
				deletedOwn118.Store(true)
				_, _ = writer.Write([]byte(`{"data":"UPID:pve1:00001234:00005678:65000002:qmdestroy:118:root@pam:"}`))
				return
			}
			writer.WriteHeader(http.StatusNotFound)
		case "/api2/json/nodes/pve1/tasks/UPID:pve1:00001234:00005678:65000002:qmdestroy:118:root@pam:/status":
			_, _ = writer.Write([]byte(`{"data":{"status":"stopped","exitstatus":"OK"}}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	taskInterval := 1
	ig := InstanceGroup{
		proxmox: proxmox.NewClient(server.URL + "/api2/json"),
		Settings: Settings{
			InstanceNameCreating:    "gitlab-fleeting-win-creating",
			InstanceNameRunning:     "gitlab-fleeting-win-running",
			InstanceNameRemoving:    "gitlab-fleeting-win-removing",
			InstanceNameIncludeVMID: true,
			ProxmoxTaskWaitInterval: &taskInterval,
		},
	}

	ig.setInstanceState(117, provider.StateDeleting)
	ig.setInstanceState(118, provider.StateDeleting)

	// Foreign VM 117 must NOT be stopped or deleted, and stale state must be cleared.
	ig.collectInstance(ctx, proxmox.ClusterResource{VMID: 117, Node: "pve1", Name: "gitlab-fleeting-win-removing-117"})
	require.False(t, touchedForeign117.Load())
	_, has117 := ig.getInstanceState(117)
	require.False(t, has117)

	// Owned VM 118 must be deleted AND its state cleared immediately upon deletion.
	ig.collectInstance(ctx, proxmox.ClusterResource{VMID: 118, Node: "pve1", Name: "gitlab-fleeting-win-removing-118"})
	require.True(t, deletedOwn118.Load())
	_, has118 := ig.getInstanceState(118)
	require.False(t, has118)
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

package plugin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

func TestInstanceGroup_getProxmoxClient(t *testing.T) {
	tempDir := t.TempDir()
	ig := InstanceGroup{
		Settings: Settings{
			URL:                   "https://example.com/proxmox",
			InsecureSkipTLSVerify: false,
			CredentialsFilePath:   path.Join(tempDir, "prox_credentials.json"),
		},
	}

	err := os.WriteFile(
		ig.Settings.CredentialsFilePath,
		[]byte(`{"realm": "pve","username": "03Ewl6rENi","password": "-rx£N503o_8(%\"l+=*4,YD"}`),
		0o600,
	)
	require.NoError(t, err)

	_, err = ig.getProxmoxClient()
	require.NoError(t, err)
}

func TestInstanceGroup_getProxmoxCredentials(t *testing.T) {
	tempDir := t.TempDir()
	ig := InstanceGroup{
		Settings: Settings{
			CredentialsFilePath: path.Join(tempDir, "sample_credentials.json"),
		},
	}

	// Missing credentials file
	_, err := ig.getProxmoxCredentials()
	require.ErrorIs(t, err, os.ErrNotExist)

	// Malformed credentials file
	err = os.WriteFile(
		ig.Settings.CredentialsFilePath,
		[]byte(`{"realm": 'pve',`),
		0o600,
	)
	require.NoError(t, err)

	_, err = ig.getProxmoxCredentials()
	require.Error(t, err)

	// Correct credentials file
	err = os.WriteFile(
		ig.Settings.CredentialsFilePath,
		[]byte(`{"realm": "pve","username": "oQcW8N246FODI6Qui","password": "88u3[kKLJ{gU7A£fhWq"}`),
		0o600,
	)
	require.NoError(t, err)

	credentials, err := ig.getProxmoxCredentials()
	require.NoError(t, err)
	require.Equal(t, "pve", credentials.Realm)
	require.Equal(t, "oQcW8N246FODI6Qui", credentials.Username)
	require.Equal(t, `88u3[kKLJ{gU7A£fhWq`, credentials.Password)
}

func TestWaitForProxmoxTask(t *testing.T) {
	ctx := context.Background()

	// nil task should succeed immediately
	require.NoError(t, waitForProxmoxTask(ctx, nil, 10*time.Millisecond, 1*time.Second))

	exitStatus := "OK"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"data":{"status":"stopped","exitstatus":"` + exitStatus + `"}}`))
	}))
	defer server.Close()

	client := proxmox.NewClient(server.URL)
	upid := proxmox.UPID("UPID:pve1:00001234:00005678:65000000:qmclone:100:root@pam:")

	// Successful task
	task := proxmox.NewTask(upid, client)
	require.NoError(t, waitForProxmoxTask(ctx, task, 10*time.Millisecond, 1*time.Second))

	// Failed task must return ErrTaskFailed
	exitStatus = "storage 'local-lvm' is full"
	failedTask := proxmox.NewTask(upid, client)
	err := waitForProxmoxTask(ctx, failedTask, 10*time.Millisecond, 1*time.Second)
	require.ErrorIs(t, err, ErrTaskFailed)
}

func TestInstanceGroup_calculateNodeScore(t *testing.T) {
	ig := InstanceGroup{}

	nodeLow := &proxmox.NodeStatus{
		Node:   "pve1",
		Status: "online",
		Mem:    2 * 1024 * 1024 * 1024,
		MaxMem: 16 * 1024 * 1024 * 1024,
		CPU:    0.1,
	}
	nodeHigh := &proxmox.NodeStatus{
		Node:   "pve2",
		Status: "online",
		Mem:    12 * 1024 * 1024 * 1024,
		MaxMem: 16 * 1024 * 1024 * 1024,
		CPU:    0.6,
	}

	vmMaxMem := uint64(4 * 1024 * 1024 * 1024)
	require.Less(t, ig.calculateNodeScore(nodeLow, vmMaxMem), ig.calculateNodeScore(nodeHigh, vmMaxMem))

	// Adding 4 in-flight clones of 4GB each on pve1 exceeds its 16GB MaxMem and raises its score above pve2
	for range 4 {
		ig.incrementInFlightClones("pve1")
	}
	require.Greater(t, ig.calculateNodeScore(nodeLow, vmMaxMem), ig.calculateNodeScore(nodeHigh, vmMaxMem))
}

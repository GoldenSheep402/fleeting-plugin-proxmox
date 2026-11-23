package plugin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestInstanceGroup_templateCloneOptions(t *testing.T) {
	type testCase struct {
		name              string
		isTemplate        bool
		configuredStorage string
		expectedFull      proxmox.IntOrBool
		expectedErr       error
	}

	testCases := []testCase{
		{
			name:              "VM with unconfigured storage", // Error?
			isTemplate:        false,
			configuredStorage: "",
			expectedFull:      true,
			expectedErr:       ErrCloneVMWithoutConfiguredStorage,
		},
		{
			name:              "VM with configured storage",
			isTemplate:        false,
			configuredStorage: "local",
			expectedFull:      true,
			expectedErr:       nil,
		},
		{
			name:              "Template with unconfigured storage",
			isTemplate:        true,
			configuredStorage: "",
			expectedFull:      false,
			expectedErr:       nil,
		},
		{
			name:              "Template with configured storage",
			isTemplate:        true,
			configuredStorage: "local",
			expectedFull:      true,
			expectedErr:       nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			template := proxmox.VirtualMachine{
				Template: proxmox.IsTemplate(testCase.isTemplate),
			}

			ig := InstanceGroup{
				Settings: Settings{
					Storage: testCase.configuredStorage,
				},
			}

			result, err := ig.getTemplateCloneOptions(&template)
			require.ErrorIs(t, err, testCase.expectedErr)
			if err == nil {
				require.Equal(t, testCase.configuredStorage, result.Storage)
				require.Equal(t, testCase.expectedFull, result.Full)
			}
		})
	}
}

func TestIsVMTemplate_WithVirtualMachineConfig(t *testing.T) {
	// Even if IsTemplate unmarshalled "0" as true, VirtualMachineConfig.Template is authoritative.
	vm := proxmox.VirtualMachine{
		Template: proxmox.IsTemplate(true),
		VirtualMachineConfig: &proxmox.VirtualMachineConfig{
			Template: false,
		},
	}
	require.False(t, isVMTemplate(&vm))

	tpl := proxmox.VirtualMachine{
		Template: proxmox.IsTemplate(false),
		VirtualMachineConfig: &proxmox.VirtualMachineConfig{
			Template: true,
		},
	}
	require.True(t, isVMTemplate(&tpl))
}

func TestInstanceGroup_templateCloneOptions_BWLimit(t *testing.T) {
	bwLimit := uint64(51200)
	ig := InstanceGroup{
		Settings: Settings{
			Pool:                 "ci-pool",
			Storage:              "local-lvm",
			InstanceNameCreating: DefaultInstanceNameCreating,
			CloneBWLimit:         &bwLimit,
		},
	}

	template := proxmox.VirtualMachine{Template: proxmox.IsTemplate(true)}
	opts, err := ig.getTemplateCloneOptions(&template)
	require.NoError(t, err)
	require.NotNil(t, opts.BWLimit)
	require.Equal(t, uint64(51200), *opts.BWLimit)
}

func TestBuildVMConfigOptions(t *testing.T) {
	cores := 4
	memory := 8192

	options := buildVMConfigOptions("fleeting-running-101", "ci;linux", true, &cores, &memory)
	require.Len(t, options, 4)
	require.Equal(t, proxmox.VirtualMachineOption{Name: "name", Value: "fleeting-running-101"}, options[0])
	require.Equal(t, proxmox.VirtualMachineOption{Name: "tags", Value: "ci;linux"}, options[1])
	require.Equal(t, proxmox.VirtualMachineOption{Name: "cores", Value: 4}, options[2])
	require.Equal(t, proxmox.VirtualMachineOption{Name: "memory", Value: 8192}, options[3])
}

func TestInstanceGroup_cloneTemplate_ConcurrentOverlappingTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		nextVMID            atomic.Int32
		cloneCount          atomic.Int32
		bothClonesSubmitted = make(chan struct{})
		closeBarrierOnce    sync.Once
	)
	nextVMID.Store(100)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/api2/json/nodes/pve1/qemu/9000/status/current":
			_, _ = writer.Write([]byte(`{"data":{"vmid":9000,"name":"ci-tpl","status":"stopped","template":1,"maxmem":2147483648}}`))
		case "/api2/json/nodes/pve1/qemu/9000/config":
			_, _ = writer.Write([]byte(`{"data":{"template":1,"memory":"2048"}}`))
		case "/api2/json/nodes/pve1/qemu/9000/feature":
			_, _ = writer.Write([]byte(`{"data":{"hasFeature":true,"nodes":["pve1"]}}`))
		case "/api2/json/nodes":
			_, _ = writer.Write([]byte(`{"data":[{"node":"pve1","status":"online","mem":2147483648,"maxmem":17179869184,"cpu":0.1}]}`))
		case "/api2/json/cluster/status":
			_, _ = writer.Write([]byte(`{"data":[]}`))
		case "/api2/json/cluster/nextid":
			id := nextVMID.Add(1)
			_, _ = writer.Write([]byte(`{"data":"` + string(rune('0'+id/100)) + string(rune('0'+(id/10)%10)) + string(rune('0'+id%10)) + `"}`))
		case "/api2/json/nodes/pve1/qemu/9000/clone":
			count := cloneCount.Add(1)
			if count >= 2 {
				closeBarrierOnce.Do(func() {
					close(bothClonesSubmitted)
				})
			}
			upid := "UPID:pve1:00001234:00005678:6500000" + string(rune('0'+count)) + ":qmclone:9000:root@pam:"
			_, _ = writer.Write([]byte(`{"data":"` + upid + `"}`))
		case "/api2/json/nodes/pve1/tasks/UPID:pve1:00001234:00005678:65000001:qmclone:9000:root@pam:/status",
			"/api2/json/nodes/pve1/tasks/UPID:pve1:00001234:00005678:65000002:qmclone:9000:root@pam:/status":
			// Block task completion until BOTH clone POST requests have arrived.
			// If cloneMu were held while polling tasks, the 2nd clone could never start and this would time out.
			select {
			case <-bothClonesSubmitted:
				_, _ = writer.Write([]byte(`{"data":{"status":"stopped","exitstatus":"OK"}}`))
			case <-req.Context().Done():
				return
			}
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := proxmox.NewClient(server.URL + "/api2/json")
	template, err := (&proxmox.Node{}).New(client, "pve1").VirtualMachine(ctx, 9000)
	require.NoError(t, err)

	taskWaitInterval := 1
	maxConcurrent := 2
	ig := InstanceGroup{
		proxmox:  client,
		cloneSem: make(chan struct{}, maxConcurrent),
		Settings: Settings{
			Pool:                    "ci-pool",
			Storage:                 "local-lvm",
			InstanceNameCreating:    DefaultInstanceNameCreating,
			ProxmoxTaskWaitInterval: &taskWaitInterval,
			MaxConcurrentClones:     &maxConcurrent,
		},
	}

	var (
		mu       sync.Mutex
		vmids    []int
		cloneMu  sync.Mutex
		errGroup errgroup.Group
	)

	for range 2 {
		errGroup.Go(func() error {
			vmid, node, cloneErr := ig.cloneTemplate(ctx, template, &cloneMu)
			if cloneErr != nil {
				return cloneErr
			}

			require.Equal(t, "pve1", node)
			mu.Lock()
			vmids = append(vmids, vmid)
			mu.Unlock()

			return nil
		})
	}

	require.NoError(t, errGroup.Wait())
	require.ElementsMatch(t, []int{101, 102}, vmids)
	require.Equal(t, 2, ig.getInFlightClones("pve1"))
	require.True(t, ig.isActiveDeploy(101))
	require.True(t, ig.isActiveDeploy(102))
}

func TestInstanceGroup_cloneTemplate_NodeFallback(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/api2/json/nodes/pve1/qemu/9000/status/current":
			_, _ = writer.Write([]byte(`{"data":{"vmid":9000,"name":"ci-tpl","status":"stopped","template":1,"maxmem":2147483648}}`))
		case "/api2/json/nodes/pve1/qemu/9000/config":
			_, _ = writer.Write([]byte(`{"data":{"template":1,"memory":"2048"}}`))
		case "/api2/json/nodes/pve1/qemu/9000/feature":
			_, _ = writer.Write([]byte(`{"data":{"hasFeature":true,"nodes":["pve1","pve2"]}}`))
		case "/api2/json/nodes":
			// pve2 has lower load so it is ranked first, pve1 is second.
			_, _ = writer.Write([]byte(`{"data":[
				{"node":"pve1","status":"online","mem":8589934592,"maxmem":17179869184,"cpu":0.5},
				{"node":"pve2","status":"online","mem":2147483648,"maxmem":17179869184,"cpu":0.1}
			]}`))
		case "/api2/json/cluster/status":
			_, _ = writer.Write([]byte(`{"data":[]}`))
		case "/api2/json/cluster/nextid":
			_, _ = writer.Write([]byte(`{"data":"201"}`))
		case "/api2/json/nodes/pve1/qemu/9000/clone":
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			var payload map[string]any
			require.NoError(t, json.Unmarshal(body, &payload))
			if payload["target"] == "pve2" {
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = writer.Write([]byte(`{"data":"UPID:pve1:00001234:00005678:65000001:qmclone:9000:root@pam:"}`))
		case "/api2/json/nodes/pve1/tasks/UPID:pve1:00001234:00005678:65000001:qmclone:9000:root@pam:/status":
			_, _ = writer.Write([]byte(`{"data":{"status":"stopped","exitstatus":"OK"}}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := proxmox.NewClient(server.URL + "/api2/json")
	template, err := (&proxmox.Node{}).New(client, "pve1").VirtualMachine(ctx, 9000)
	require.NoError(t, err)

	taskWaitInterval := 1
	ig := InstanceGroup{
		proxmox: client,
		Settings: Settings{
			Pool:                    "ci-pool",
			Storage:                 "local-lvm",
			InstanceNameCreating:    DefaultInstanceNameCreating,
			ProxmoxTaskWaitInterval: &taskWaitInterval,
		},
	}

	var cloneMu sync.Mutex
	vmid, node, err := ig.cloneTemplate(ctx, template, &cloneMu)
	require.NoError(t, err)
	require.Equal(t, 201, vmid)
	require.Equal(t, "pve1", node)
}

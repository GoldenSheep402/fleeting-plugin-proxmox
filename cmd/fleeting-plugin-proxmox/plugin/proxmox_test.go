package plugin

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync/atomic"
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

func TestInstanceGroup_CredentialsAPITokenAndValidation(t *testing.T) {
	tempDir := t.TempDir()
	credsPath := path.Join(tempDir, "creds.json")
	ig := InstanceGroup{
		Settings: Settings{
			CredentialsFilePath: credsPath,
		},
	}

	// Valid API token credentials
	err := os.WriteFile(credsPath, []byte(`{"token_id": "ci@pve!runner", "secret": "super-secret-uuid"}`), 0o600)
	require.NoError(t, err)

	creds, err := ig.getProxmoxCredentials()
	require.NoError(t, err)
	require.True(t, creds.isAPIToken())
	require.Equal(t, "ci@pve!runner", creds.TokenID)
	require.Equal(t, "super-secret-uuid", creds.Secret)

	// Partial token credentials must fail
	err = os.WriteFile(credsPath, []byte(`{"token_id": "ci@pve!runner"}`), 0o600)
	require.NoError(t, err)

	_, err = ig.getProxmoxCredentials()
	require.ErrorIs(t, err, ErrInvalidCredentials)

	// Empty credentials must fail
	err = os.WriteFile(credsPath, []byte(`{}`), 0o600)
	require.NoError(t, err)

	_, err = ig.getProxmoxCredentials()
	require.ErrorIs(t, err, ErrInvalidCredentials)
}

func TestInstanceGroup_getProxmoxClient_CACertAndAPIToken(t *testing.T) {
	tempDir := t.TempDir()
	credsPath := path.Join(tempDir, "token.json")
	require.NoError(t, os.WriteFile(credsPath, []byte(`{"token_id":"ci@pve!tok","secret":"sec"}`), 0o600))

	caPath := path.Join(tempDir, "ca.pem")
	require.NoError(t, os.WriteFile(caPath, generateTestCACertPEM(t), 0o600))

	ig := InstanceGroup{
		Settings: Settings{
			URL:                 "https://pve.example.local:8006",
			CredentialsFilePath: credsPath,
			CACertPath:          caPath,
		},
	}

	client, err := ig.getProxmoxClient()
	require.NoError(t, err)
	require.NotNil(t, client)
	require.True(t, ig.useAPIToken)

	// Invalid PEM content must fail with ErrInvalidCACert
	badCAPath := path.Join(tempDir, "bad-ca.pem")
	require.NoError(t, os.WriteFile(badCAPath, []byte("not-a-cert"), 0o600))
	ig.Settings.CACertPath = badCAPath

	_, err = ig.getProxmoxClient()
	require.ErrorIs(t, err, ErrInvalidCACert)
}

func TestInstanceGroup_MultiURLFailover(t *testing.T) {
	tempDir := t.TempDir()
	credsPath := path.Join(tempDir, "token.json")
	require.NoError(t, os.WriteFile(credsPath, []byte(`{"token_id":"ci@pve!tok","secret":"sec"}`), 0o600))

	var primaryHits atomic.Int32
	primaryServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		primaryHits.Add(1)
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer primaryServer.Close()

	var secondaryHits atomic.Int32
	secondaryServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		secondaryHits.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/api2/json/version":
			_, _ = writer.Write([]byte(`{"data":{"version":"9.0","release":"9.0"}}`))
		case "/api2/json/pools/":
			_, _ = writer.Write([]byte(`{"data":[{"poolid":"ci-pool","members":[{"id":"qemu/101","type":"qemu","vmid":101,"node":"pve2","name":"fleeting-running","status":"running"}]}]}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer secondaryServer.Close()

	ig := InstanceGroup{
		Settings: Settings{
			URL:                 primaryServer.URL,
			URLs:                []string{secondaryServer.URL},
			CredentialsFilePath: credsPath,
			Pool:                "ci-pool",
		},
	}
	require.NoError(t, ig.initProxmoxClients())

	pool, err := ig.getProxmoxPool(context.Background())
	require.NoError(t, err)
	require.Equal(t, "ci-pool", pool.PoolID)
	require.Len(t, pool.Members, 1)
	require.GreaterOrEqual(t, primaryHits.Load(), int32(1))
	require.GreaterOrEqual(t, secondaryHits.Load(), int32(2))
	require.Equal(t, 1, ig.activeClientIdx)
}

func TestInstanceGroup_rankTargetNodes(t *testing.T) {
	ctx := context.Background()

	var storageStatus atomic.Int32
	storageStatus.Store(http.StatusOK)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/api2/json/nodes/pve1/qemu/9000/status/current":
			_, _ = writer.Write([]byte(`{"data":{"vmid":9000,"name":"ci-tpl","status":"stopped","template":1,"maxmem":2147483648}}`))
		case "/api2/json/nodes/pve1/qemu/9000/config":
			_, _ = writer.Write([]byte(`{"data":{"template":1,"memory":"2048"}}`))
		case "/api2/json/nodes/pve1/qemu/9000/feature":
			_, _ = writer.Write([]byte(`{"data":{"hasFeature":true,"nodes":["pve1","pve2","pve3"]}}`))
		case "/api2/json/nodes":
			_, _ = writer.Write([]byte(`{"data":[
				{"node":"pve1","status":"online","mem":2147483648,"maxmem":17179869184,"cpu":0.1},
				{"node":"pve2","status":"online","mem":4294967296,"maxmem":17179869184,"cpu":0.2},
				{"node":"pve3","status":"online","mem":15032385536,"maxmem":17179869184,"cpu":0.85}
			]}`))
		case "/api2/json/storage/fast-nvme":
			if storageStatus.Load() != http.StatusOK {
				writer.WriteHeader(int(storageStatus.Load()))
				return
			}
			_, _ = writer.Write([]byte(`{"data":{"storage":"fast-nvme","nodes":"pve2,pve3"}}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	boundClient := proxmox.NewClient(server.URL + "/api2/json")
	fetchedTemplate, err := (&proxmox.Node{}).New(boundClient, "pve1").VirtualMachine(ctx, 9000)
	require.NoError(t, err)

	maxMemRatio := 0.80
	maxCPURatio := 0.80

	// Storage excludes pve1, mem/cpu hard caps exclude pve3 -> only pve2 remains.
	ig := InstanceGroup{
		proxmox: boundClient,
		Settings: Settings{
			Storage:         "fast-nvme",
			NodeMaxMemRatio: &maxMemRatio,
			NodeMaxCPURatio: &maxCPURatio,
		},
	}

	candidates, err := ig.rankTargetNodes(ctx, fetchedTemplate)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "pve2", candidates[0].name)

	// Storage returns 403 -> degrades gracefully, pve1 becomes top candidate ahead of pve2.
	storageStatus.Store(http.StatusForbidden)
	candidates, err = ig.rankTargetNodes(ctx, fetchedTemplate)
	require.NoError(t, err)
	require.Len(t, candidates, 2)
	require.Equal(t, "pve1", candidates[0].name)
	require.Equal(t, "pve2", candidates[1].name)

	// ExcludedNodes filters out pve1 and pve2 -> ErrNoEligibleNode.
	ig.Settings.ExcludedNodes = []string{"pve1", "pve2"}
	_, err = ig.rankTargetNodes(ctx, fetchedTemplate)
	require.ErrorIs(t, err, ErrNoEligibleNode)
}

func TestTemplateMemoryBytes(t *testing.T) {
	override := 4096
	tpl := &proxmox.VirtualMachine{
		MaxMem: 2 * 1024 * 1024 * 1024,
		VirtualMachineConfig: &proxmox.VirtualMachineConfig{
			Memory: 1024,
		},
	}

	require.Equal(t, uint64(4096)*bytesPerMiB, templateMemoryBytes(tpl, &override))
	require.Equal(t, uint64(2*1024*1024*1024), templateMemoryBytes(tpl, nil))

	tpl.MaxMem = 0
	require.Equal(t, uint64(1024)*bytesPerMiB, templateMemoryBytes(tpl, nil))
}

func TestRetryOnProxmoxLockAndErrorHelpers(t *testing.T) {
	attempts := 0
	err := retryOnProxmoxLock(context.Background(), 3, 5*time.Millisecond, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("500 can't lock file '/var/lock/qemu-server/lock-101.conf' - got timeout")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)

	// Non-lock error stops immediately without retrying
	nonLockAttempts := 0
	err = retryOnProxmoxLock(context.Background(), 3, 5*time.Millisecond, func() error {
		nonLockAttempts++
		return errors.New("403 permission denied")
	})
	require.Error(t, err)
	require.Equal(t, 1, nonLockAttempts)

	require.True(t, isVMNotFoundError(proxmox.ErrNotFound))
	require.True(t, isVMNotFoundError(ErrNotFound))
	require.True(t, isVMNotFoundError(&proxmoxHTTPStatusError{StatusCode: http.StatusNotFound, Status: "404 Not Found"}))
	require.True(t, isVMNotFoundError(errors.New("500 Configuration file 'nodes/pve1/qemu-server/101.conf' does not exist")))
	require.False(t, isVMNotFoundError(errors.New("500 internal error")))
}

func generateTestCACertPEM(t *testing.T) []byte {
	t.Helper()

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "test-proxmox-ca",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &privKey.PublicKey, privKey)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: derBytes,
	})
}

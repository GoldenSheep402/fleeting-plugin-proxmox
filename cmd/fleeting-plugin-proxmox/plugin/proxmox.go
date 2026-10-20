package plugin

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"slices"
	"time"

	"github.com/luthermonson/go-proxmox"
)

var (
	ErrNotFound   = errors.New("not found")
	ErrTaskFailed = errors.New("proxmox task failed")
)

const (
	memWeight = 0.7
	cpuWeight = 0.3
)

func (ig *InstanceGroup) getProxmoxPool(ctx context.Context) (*proxmox.Pool, error) {
	pool, err := ig.proxmox.Pool(ctx, ig.Settings.Pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get pool id='%s': %w", ig.Settings.Pool, err)
	}

	return pool, nil
}

// Where possible, use getProxmoxVMOnNode instead as it makes less calls to API.
func (ig *InstanceGroup) getProxmoxVM(ctx context.Context, vmid int) (*proxmox.VirtualMachine, error) {
	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return nil, err
	}

	for _, member := range pool.Members {
		if member.Type != "qemu" {
			continue
		}

		if member.VMID == uint64(vmid) {
			return ig.getProxmoxVMOnNode(ctx, vmid, member.Node)
		}
	}

	return nil, ErrNotFound
}

func (ig *InstanceGroup) getProxmoxVMOnNode(ctx context.Context, vmid int, nodeName string) (*proxmox.VirtualMachine, error) {
	node := (&proxmox.Node{}).New(ig.proxmox, nodeName)

	vm, err := node.VirtualMachine(ctx, vmid)
	if err != nil {
		return nil, fmt.Errorf("failed to get vm='%d' on node='%s': %w", vmid, nodeName, err)
	}

	return vm, nil
}

func waitForProxmoxTask(ctx context.Context, task *proxmox.Task, interval, timeout time.Duration) error {
	if task == nil {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := task.Ping(waitCtx); err != nil {
			return fmt.Errorf("failed to ping task '%s': %w", task.UPID, err)
		}

		if task.Status != proxmox.TaskRunning {
			if task.IsFailed || (task.ExitStatus != "" && task.ExitStatus != "OK") {
				return fmt.Errorf("%w: task='%s' exit_status='%s'", ErrTaskFailed, task.UPID, task.ExitStatus)
			}

			return nil
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("waiting for task '%s' interrupted: %w", task.UPID, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

// selectTargetNode picks the least loaded online node among those eligible to clone the template.
// Returns "" if multi-node scheduling is not applicable or unavailable (falling back to template's node).
func (ig *InstanceGroup) selectTargetNode(ctx context.Context, template *proxmox.VirtualMachine) string {
	if ig.proxmox == nil {
		return ""
	}

	feature, err := template.Feature(ctx, "clone", "")
	if err != nil || !feature.HasFeature || len(feature.Nodes) <= 1 {
		return ""
	}

	nodes, err := ig.proxmox.Nodes(ctx)
	if err != nil || len(nodes) <= 1 {
		return ""
	}

	bestNode := ""
	bestScore := math.MaxFloat64

	for _, node := range nodes {
		if node == nil || node.Status != "online" || node.MaxMem == 0 {
			continue
		}

		if !slices.Contains(feature.Nodes, node.Node) {
			continue
		}

		score := ig.calculateNodeScore(node, template.MaxMem)
		if score < bestScore {
			bestScore = score
			bestNode = node.Node
		}
	}

	if bestNode == template.Node {
		return ""
	}

	return bestNode
}

func (ig *InstanceGroup) calculateNodeScore(node *proxmox.NodeStatus, vmMaxMem uint64) float64 {
	inFlight := ig.getInFlightClones(node.Node)
	estimatedMem := node.Mem + uint64(inFlight)*vmMaxMem
	memRatio := float64(estimatedMem) / float64(node.MaxMem)

	score := memRatio*memWeight + node.CPU*cpuWeight
	if estimatedMem > node.MaxMem {
		// Heavily penalize nodes that would exceed physical memory capacity
		score += 1.0
	}

	return score
}

func (ig *InstanceGroup) getProxmoxClient() (*proxmox.Client, error) {
	url, err := url.Parse(ig.Settings.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL='%s': %w", ig.Settings.URL, err)
	}

	credentials, err := ig.getProxmoxCredentials()
	if err != nil {
		return nil, err
	}

	httpClient := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				//nolint:gosec
				InsecureSkipVerify: ig.Settings.InsecureSkipTLSVerify,
			},
		},
	}

	return proxmox.NewClient(
		url.JoinPath("/api2/json").String(),
		proxmox.WithCredentials(credentials),
		proxmox.WithHTTPClient(&httpClient),
	), nil
}

func (ig *InstanceGroup) getProxmoxCredentials() (*proxmox.Credentials, error) {
	credentialsFile, err := os.Open(ig.Settings.CredentialsFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open credentials file from path='%s': %w", ig.Settings.CredentialsFilePath, err)
	}
	defer credentialsFile.Close()

	credentials := proxmox.Credentials{}
	if err := json.NewDecoder(credentialsFile).Decode(&credentials); err != nil {
		return nil, fmt.Errorf("failed to decode credentials file from path='%s': %w", ig.Settings.CredentialsFilePath, err)
	}

	return &credentials, nil
}

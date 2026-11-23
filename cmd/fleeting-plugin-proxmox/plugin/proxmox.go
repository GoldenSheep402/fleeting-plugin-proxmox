package plugin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/luthermonson/go-proxmox"
)

var (
	ErrNotFound           = errors.New("not found")
	ErrTaskFailed         = errors.New("proxmox task failed")
	ErrInvalidCredentials = errors.New("invalid proxmox credentials")
	ErrInvalidCACert      = errors.New("invalid CA certificate")
	ErrNoEligibleNode     = errors.New("no eligible proxmox node available for scheduling")
)

const (
	memWeight = 0.7
	cpuWeight = 0.3

	proxmoxHTTPRetryMax        = 3
	proxmoxHTTPRetryInitial    = 200 * time.Millisecond
	proxmoxHTTPRetryMaxBackoff = 2 * time.Second
	failoverProbeTimeout       = 5 * time.Second
	bytesPerMiB                = 1024 * 1024
)

type proxmoxCredentials struct {
	Realm    string `json:"realm"`
	Username string `json:"username"`
	Password string `json:"password"`
	OTP      string `json:"otp"`
	TokenID  string `json:"token_id"`
	Secret   string `json:"secret"`
}

func (creds *proxmoxCredentials) isAPIToken() bool {
	return creds != nil && creds.TokenID != "" && creds.Secret != ""
}

type nodeCandidate struct {
	name  string
	score float64
}

type proxmoxHTTPStatusError struct {
	StatusCode int
	Status     string
}

func (statusErr *proxmoxHTTPStatusError) Error() string {
	return fmt.Sprintf("proxmox HTTP %d: %s", statusErr.StatusCode, statusErr.Status)
}

func (statusErr *proxmoxHTTPStatusError) Unwrap() error {
	if statusErr.StatusCode == http.StatusNotFound {
		return proxmox.ErrNotFound
	}

	return nil
}

type proxmoxStatusRoundTripper struct {
	base http.RoundTripper
}

func (roundTripper *proxmoxStatusRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := roundTripper.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	switch res.StatusCode {
	case http.StatusNotFound,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()

		return nil, &proxmoxHTTPStatusError{
			StatusCode: res.StatusCode,
			Status:     res.Status,
		}
	default:
		return res, nil
	}
}

func shouldRetryProxmoxHTTP(res *http.Response, err error) bool {
	if err != nil {
		var statusErr *proxmoxHTTPStatusError
		if errors.As(err, &statusErr) {
			return statusErr.StatusCode == http.StatusBadGateway ||
				statusErr.StatusCode == http.StatusServiceUnavailable ||
				statusErr.StatusCode == http.StatusGatewayTimeout
		}

		return true
	}

	if res == nil {
		return false
	}

	return res.StatusCode == http.StatusTooManyRequests ||
		res.StatusCode == http.StatusBadGateway ||
		res.StatusCode == http.StatusServiceUnavailable ||
		res.StatusCode == http.StatusGatewayTimeout
}

func (ig *InstanceGroup) currentClient() *proxmox.Client {
	ig.clientMu.RLock()
	defer ig.clientMu.RUnlock()

	if len(ig.proxmoxClients) > 0 && ig.activeClientIdx >= 0 && ig.activeClientIdx < len(ig.proxmoxClients) {
		return ig.proxmoxClients[ig.activeClientIdx]
	}

	return ig.proxmox
}

func (ig *InstanceGroup) withFailover(ctx context.Context, op func(*proxmox.Client) error) error {
	client := ig.currentClient()
	err := op(client)
	if err == nil || !isFailoverEligibleError(err) {
		return err
	}

	if !ig.tryFailoverProxmoxClient(ctx, client, err) {
		return err
	}

	return op(ig.currentClient())
}

func (ig *InstanceGroup) tryFailoverProxmoxClient(ctx context.Context, failedClient *proxmox.Client, cause error) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}

	ig.clientMu.Lock()
	defer ig.clientMu.Unlock()

	if len(ig.proxmoxClients) <= 1 {
		return false
	}

	// Another goroutine already switched away from failedClient.
	if ig.activeClientIdx >= 0 && ig.activeClientIdx < len(ig.proxmoxClients) && ig.proxmoxClients[ig.activeClientIdx] != failedClient {
		return true
	}

	total := len(ig.proxmoxClients)
	for offset := 1; offset < total; offset++ {
		idx := (ig.activeClientIdx + offset) % total
		candidate := ig.proxmoxClients[idx]

		probeCtx, cancel := context.WithTimeout(ig.lifecycleContext(), failoverProbeTimeout)
		_, err := candidate.Version(probeCtx)
		cancel()

		if err != nil {
			ig.logWarn("proxmox failover candidate unhealthy", "url", ig.proxmoxURLs[idx], "err", err)
			continue
		}

		ig.activeClientIdx = idx
		ig.proxmox = candidate
		ig.logWarn("switched active proxmox API endpoint", "url", ig.proxmoxURLs[idx], "cause", cause)

		return true
	}

	return false
}

func isFailoverEligibleError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) || proxmox.IsNotAuthorized(err) || isVMNotFoundError(err) {
		return false
	}

	var statusErr *proxmoxHTTPStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode == http.StatusBadGateway ||
			statusErr.StatusCode == http.StatusServiceUnavailable ||
			statusErr.StatusCode == http.StatusGatewayTimeout
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	msg := strings.ToLower(err.Error())

	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "tls handshake timeout") ||
		strings.Contains(msg, "502 bad gateway") ||
		strings.Contains(msg, "503 service unavailable") ||
		strings.Contains(msg, "504 gateway timeout")
}

func isVMNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if proxmox.IsNotFound(err) || errors.Is(err, ErrNotFound) {
		return true
	}

	var statusErr *proxmoxHTTPStatusError
	if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound {
		return true
	}

	msg := strings.ToLower(err.Error())

	return strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "no such vm") ||
		strings.Contains(msg, "404 not found")
}

func isProxmoxLockError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())

	return strings.Contains(msg, "can't lock file") ||
		strings.Contains(msg, "lock timeout") ||
		strings.Contains(msg, "vm is locked") ||
		strings.Contains(msg, "got timeout")
}

func isDuplicateVMIDError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())

	return strings.Contains(msg, "already exists")
}

func retryOnProxmoxLock(ctx context.Context, maxAttempts int, baseDelay time.Duration, operation func() error) error {
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	delay := baseDelay

	for attempt := 0; attempt < maxAttempts; attempt++ {
		lastErr = operation()
		if lastErr == nil || !isProxmoxLockError(lastErr) || attempt == maxAttempts-1 {
			return lastErr
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lastErr
		case <-timer.C:
		}

		delay *= 2
	}

	return lastErr
}

func (ig *InstanceGroup) getProxmoxPool(ctx context.Context) (*proxmox.Pool, error) {
	var pool *proxmox.Pool

	err := ig.withFailover(ctx, func(client *proxmox.Client) error {
		var callErr error
		pool, callErr = client.Pool(ctx, ig.Settings.Pool)
		return callErr
	})
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
	var vm *proxmox.VirtualMachine

	err := ig.withFailover(ctx, func(client *proxmox.Client) error {
		node := (&proxmox.Node{}).New(client, nodeName)

		var callErr error
		vm, callErr = node.VirtualMachine(ctx, vmid)
		return callErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get vm='%d' on node='%s': %w", vmid, nodeName, err)
	}

	return vm, nil
}

func (ig *InstanceGroup) getProxmoxNodes(ctx context.Context) (proxmox.NodeStatuses, error) {
	var nodes proxmox.NodeStatuses

	err := ig.withFailover(ctx, func(client *proxmox.Client) error {
		var callErr error
		nodes, callErr = client.Nodes(ctx)
		return callErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list proxmox nodes: %w", err)
	}

	return nodes, nil
}

func (ig *InstanceGroup) taskWaitInterval() time.Duration {
	if ig.Settings.ProxmoxTaskWaitInterval != nil && *ig.Settings.ProxmoxTaskWaitInterval > 0 {
		return time.Duration(*ig.Settings.ProxmoxTaskWaitInterval) * time.Second
	}

	return proxmoxTaskWaitInterval
}

func (ig *InstanceGroup) taskWaitTimeout() time.Duration {
	if ig.Settings.ProxmoxTaskWaitTimeout != nil && *ig.Settings.ProxmoxTaskWaitTimeout > 0 {
		return time.Duration(*ig.Settings.ProxmoxTaskWaitTimeout) * time.Second
	}

	return proxmoxTaskWaitTimeout
}

func (ig *InstanceGroup) agentStartTimeout() time.Duration {
	if ig.Settings.ProxmoxAgentStartTimeout != nil && *ig.Settings.ProxmoxAgentStartTimeout > 0 {
		return time.Duration(*ig.Settings.ProxmoxAgentStartTimeout) * time.Second
	}

	return proxmoxAgentStartTimeout
}

func (ig *InstanceGroup) httpTimeout() time.Duration {
	if ig.Settings.ProxmoxHTTPTimeout != nil && *ig.Settings.ProxmoxHTTPTimeout > 0 {
		return time.Duration(*ig.Settings.ProxmoxHTTPTimeout) * time.Second
	}

	return time.Duration(DefaultProxmoxHTTPTimeout) * time.Second
}

func (ig *InstanceGroup) staleCreatingTimeout() time.Duration {
	if ig.Settings.StaleCreatingTimeout != nil && *ig.Settings.StaleCreatingTimeout > 0 {
		return time.Duration(*ig.Settings.StaleCreatingTimeout) * time.Second
	}

	return time.Duration(DefaultStaleCreatingTimeout) * time.Second
}

func (ig *InstanceGroup) configureVMOptions(ctx context.Context, vm *proxmox.VirtualMachine, name, tags string, applyEmptyTags bool, cores, memory *int) error {
	options := buildVMConfigOptions(name, tags, applyEmptyTags, cores, memory)
	if len(options) == 0 {
		return nil
	}

	return retryOnProxmoxLock(ctx, 3, 500*time.Millisecond, func() error {
		task, err := vm.Config(ctx, options...)
		if err != nil {
			return err
		}

		return waitForProxmoxTask(ctx, task, ig.taskWaitInterval(), ig.taskWaitTimeout())
	})
}

func buildVMConfigOptions(name, tags string, applyEmptyTags bool, cores, memory *int) []proxmox.VirtualMachineOption {
	options := make([]proxmox.VirtualMachineOption, 0, 4)
	if name != "" {
		options = append(options, proxmox.VirtualMachineOption{
			Name:  "name",
			Value: name,
		})
	}

	if tags != "" || applyEmptyTags {
		options = append(options, proxmox.VirtualMachineOption{
			Name:  "tags",
			Value: tags,
		})
	}

	if cores != nil && *cores > 0 {
		options = append(options, proxmox.VirtualMachineOption{
			Name:  "cores",
			Value: *cores,
		})
	}

	if memory != nil && *memory > 0 {
		options = append(options, proxmox.VirtualMachineOption{
			Name:  "memory",
			Value: *memory,
		})
	}

	return options
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

func (ig *InstanceGroup) hasSchedulingConstraints() bool {
	return len(ig.Settings.AllowedNodes) > 0 ||
		len(ig.Settings.ExcludedNodes) > 0 ||
		ig.Settings.NodeMaxMemRatio != nil ||
		ig.Settings.NodeMaxCPURatio != nil
}

func isNodeAllowed(nodeName string, allowed, excluded []string) bool {
	if slices.Contains(excluded, nodeName) {
		return false
	}

	if len(allowed) > 0 && !slices.Contains(allowed, nodeName) {
		return false
	}

	return true
}

func templateMemoryBytes(template *proxmox.VirtualMachine, overrideMiB *int) uint64 {
	if overrideMiB != nil && *overrideMiB > 0 {
		return uint64(*overrideMiB) * bytesPerMiB
	}

	if template == nil {
		return 0
	}

	if template.MaxMem > 0 {
		return template.MaxMem
	}

	if template.VirtualMachineConfig != nil && template.VirtualMachineConfig.Memory > 0 {
		return uint64(template.VirtualMachineConfig.Memory) * bytesPerMiB
	}

	return 0
}

func (ig *InstanceGroup) getStorageAllowedNodes(ctx context.Context) map[string]struct{} {
	if ig.Settings.Storage == "" {
		return nil
	}

	client := ig.currentClient()
	if client == nil {
		return nil
	}

	storage, err := client.ClusterStorage(ctx, ig.Settings.Storage)
	if err != nil {
		ig.logDebug("skipping storage node affinity check", "storage", ig.Settings.Storage, "err", err)
		return nil
	}

	if storage == nil || strings.TrimSpace(storage.Nodes) == "" {
		return nil
	}

	allowed := make(map[string]struct{})
	for _, rawNode := range strings.Split(storage.Nodes, ",") {
		nodeName := strings.TrimSpace(rawNode)
		if nodeName != "" {
			allowed[nodeName] = struct{}{}
		}
	}

	return allowed
}

func (ig *InstanceGroup) withinNodeHardLimits(node *proxmox.NodeStatus, vmMaxMem uint64) bool {
	if ig.Settings.NodeMaxMemRatio != nil {
		inFlight := ig.getInFlightClones(node.Node)
		estimatedMem := node.Mem + uint64(inFlight)*vmMaxMem
		memRatio := float64(estimatedMem) / float64(node.MaxMem)

		if memRatio > *ig.Settings.NodeMaxMemRatio {
			return false
		}
	}

	if ig.Settings.NodeMaxCPURatio != nil && node.CPU > *ig.Settings.NodeMaxCPURatio {
		return false
	}

	return true
}

// rankTargetNodes returns online eligible nodes sorted ascending by load score.
// Returns nil, nil when multi-node scheduling is unavailable and no hard constraints exist.
func (ig *InstanceGroup) rankTargetNodes(ctx context.Context, template *proxmox.VirtualMachine) ([]nodeCandidate, error) {
	if ig.currentClient() == nil {
		return nil, nil
	}

	hasConstraints := ig.hasSchedulingConstraints()

	nodes, err := ig.getProxmoxNodes(ctx)
	if err != nil {
		if hasConstraints {
			return nil, err
		}

		return nil, nil
	}

	feature, featErr := template.Feature(ctx, "clone", "")
	if !hasConstraints && !canScheduleMultiNode(nodes, feature, featErr) {
		return nil, nil
	}

	candidates := ig.collectEligibleNodeCandidates(ctx, nodes, template, &feature, featErr)

	if len(candidates) == 0 {
		if hasConstraints || (featErr == nil && feature.HasFeature && len(feature.Nodes) > 0) {
			return nil, ErrNoEligibleNode
		}

		return nil, nil
	}

	ig.logDebug("ranked target nodes for clone", "selected_node", candidates[0].name, "score", candidates[0].score, "candidates", len(candidates))

	return candidates, nil
}

func canScheduleMultiNode(nodes proxmox.NodeStatuses, feature proxmox.VirtualMachineFeature, featErr error) bool {
	return len(nodes) > 1 && featErr == nil && feature.HasFeature && len(feature.Nodes) > 1
}

func (ig *InstanceGroup) collectEligibleNodeCandidates(
	ctx context.Context,
	nodes proxmox.NodeStatuses,
	template *proxmox.VirtualMachine,
	feature *proxmox.VirtualMachineFeature,
	featErr error,
) []nodeCandidate {
	storageNodes := ig.getStorageAllowedNodes(ctx)
	vmMemBytes := templateMemoryBytes(template, ig.Settings.VMMemory)
	candidates := make([]nodeCandidate, 0, len(nodes))

	for _, node := range nodes {
		if !ig.isNodeEligible(node, feature, featErr, storageNodes, vmMemBytes) {
			continue
		}

		candidates = append(candidates, nodeCandidate{
			name:  node.Node,
			score: ig.calculateNodeScore(node, vmMemBytes),
		})
	}

	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].score != candidates[right].score {
			return candidates[left].score < candidates[right].score
		}

		return candidates[left].name < candidates[right].name
	})

	return candidates
}

func (ig *InstanceGroup) isNodeEligible(
	node *proxmox.NodeStatus,
	feature *proxmox.VirtualMachineFeature,
	featErr error,
	storageNodes map[string]struct{},
	vmMemBytes uint64,
) bool {
	if node == nil || node.Status != "online" || node.MaxMem == 0 {
		return false
	}

	if featErr == nil && feature != nil && feature.HasFeature && len(feature.Nodes) > 0 {
		if !slices.Contains(feature.Nodes, node.Node) {
			return false
		}
	}

	if len(storageNodes) > 0 {
		if _, ok := storageNodes[node.Node]; !ok {
			return false
		}
	}

	if !isNodeAllowed(node.Node, ig.Settings.AllowedNodes, ig.Settings.ExcludedNodes) {
		return false
	}

	return ig.withinNodeHardLimits(node, vmMemBytes)
}

// selectTargetNode picks the least loaded online node among those eligible to clone the template.
// Returns "" if multi-node scheduling is not applicable or unavailable (falling back to template's node).
func (ig *InstanceGroup) selectTargetNode(ctx context.Context, template *proxmox.VirtualMachine) string {
	candidates, err := ig.rankTargetNodes(ctx, template)
	if err != nil || len(candidates) == 0 {
		return ""
	}

	if template != nil && candidates[0].name == template.Node {
		return ""
	}

	return candidates[0].name
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

func (ig *InstanceGroup) initProxmoxClients() error {
	urls := ig.Settings.ConfiguredURLs()
	if len(urls) == 0 {
		return fmt.Errorf("%w: url", ErrRequiredSettingMissing)
	}

	credentials, err := ig.getProxmoxCredentials()
	if err != nil {
		return err
	}

	tlsConfig, err := ig.buildTLSConfig()
	if err != nil {
		return err
	}

	clients := make([]*proxmox.Client, 0, len(urls))
	for _, rawURL := range urls {
		client, err := ig.buildProxmoxClientForURL(rawURL, credentials, tlsConfig)
		if err != nil {
			return err
		}

		clients = append(clients, client)
	}

	ig.clientMu.Lock()
	ig.proxmoxClients = clients
	ig.proxmoxURLs = urls
	ig.activeClientIdx = 0
	ig.proxmox = clients[0]
	ig.useAPIToken = credentials.isAPIToken()
	ig.clientMu.Unlock()

	return nil
}

func (ig *InstanceGroup) getProxmoxClient() (*proxmox.Client, error) {
	if err := ig.initProxmoxClients(); err != nil {
		return nil, err
	}

	return ig.currentClient(), nil
}

func (ig *InstanceGroup) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if ig.Settings.InsecureSkipTLSVerify {
		//nolint:gosec
		tlsConfig.InsecureSkipVerify = true
		return tlsConfig, nil
	}

	if ig.Settings.CACertPath == "" {
		return tlsConfig, nil
	}

	pemData, err := os.ReadFile(ig.Settings.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read ca_cert_path='%s': %w", ig.Settings.CACertPath, err)
	}

	rootCAs, err := x509.SystemCertPool()
	if err != nil || rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	if !rootCAs.AppendCertsFromPEM(pemData) {
		return nil, fmt.Errorf("%w: ca_cert_path='%s' contained no valid PEM certificates", ErrInvalidCACert, ig.Settings.CACertPath)
	}

	tlsConfig.RootCAs = rootCAs

	return tlsConfig, nil
}

func (ig *InstanceGroup) buildProxmoxClientForURL(rawURL string, credentials *proxmoxCredentials, tlsConfig *tls.Config) (*proxmox.Client, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL='%s': %w", rawURL, err)
	}

	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("unexpected default HTTP transport type")
	}

	clonedTransport := transport.Clone()
	clonedTransport.Proxy = http.ProxyFromEnvironment
	clonedTransport.TLSClientConfig = tlsConfig

	httpClient := &http.Client{
		Transport: &proxmoxStatusRoundTripper{base: clonedTransport},
	}

	opts := []proxmox.Option{
		proxmox.WithHTTPClient(httpClient),
		proxmox.WithTimeout(ig.httpTimeout()),
		proxmox.WithRetry(
			proxmox.WithRetryMax(proxmoxHTTPRetryMax),
			proxmox.WithRetryBackoff(proxmoxHTTPRetryInitial, proxmoxHTTPRetryMaxBackoff),
			proxmox.WithRetryCondition(shouldRetryProxmoxHTTP),
		),
	}

	if credentials.isAPIToken() {
		opts = append(opts, proxmox.WithAPIToken(credentials.TokenID, credentials.Secret))
	} else {
		opts = append(opts, proxmox.WithCredentials(&proxmox.Credentials{
			Realm:    credentials.Realm,
			Username: credentials.Username,
			Password: credentials.Password,
		}))
		if credentials.OTP != "" {
			opts = append(opts, proxmox.WithOTP(credentials.OTP))
		}
	}

	return proxmox.NewClient(
		parsedURL.JoinPath("/api2/json").String(),
		opts...,
	), nil
}

func (ig *InstanceGroup) getProxmoxCredentials() (*proxmoxCredentials, error) {
	credentialsFile, err := os.Open(ig.Settings.CredentialsFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open credentials file from path='%s': %w", ig.Settings.CredentialsFilePath, err)
	}
	defer credentialsFile.Close()

	credentials := proxmoxCredentials{}
	if err := json.NewDecoder(credentialsFile).Decode(&credentials); err != nil {
		return nil, fmt.Errorf("failed to decode credentials file from path='%s': %w", ig.Settings.CredentialsFilePath, err)
	}

	credentials.TokenID = strings.TrimSpace(credentials.TokenID)
	credentials.Secret = strings.TrimSpace(credentials.Secret)

	tokenIDSet := credentials.TokenID != ""
	secretSet := credentials.Secret != ""

	if tokenIDSet != secretSet {
		return nil, fmt.Errorf("%w: both token_id and secret must be provided for API token auth", ErrInvalidCredentials)
	}

	if tokenIDSet && secretSet {
		return &credentials, nil
	}

	if strings.TrimSpace(credentials.Username) == "" || credentials.Password == "" {
		return nil, fmt.Errorf("%w: username and password (or token_id and secret) are required", ErrInvalidCredentials)
	}

	return &credentials, nil
}

package plugin

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrRequiredSettingMissing  = errors.New("required setting is missing")
	ErrSettingInvalidParameter = errors.New("setting has invalid parameter")
)

// Available network protocols to look for when discovering instance's IP address.
type NetworkProtocol = string

const (
	// Tries to find one internal and one external IPv4 address.
	NetworkProtocolIPv4 NetworkProtocol = "ipv4"

	// Tries to find one internal (ULA) and one global (GUA) IPv6 address.
	NetworkProtocolIPv6 NetworkProtocol = "ipv6"

	// Will prioritize IPv6 but return IPv4 if there is no IPv6.
	NetworkProtocolAny NetworkProtocol = "any"
)

// Default values for plugin settings.
const (
	DefaultInstanceNetworkInterface = "ens18"
	DefaultInstanceNetworkProtocol  = NetworkProtocolIPv4

	DefaultInstanceNameCreating = "fleeting-creating"
	DefaultInstanceNameRunning  = "fleeting-running"
	DefaultInstanceNameRemoving = "fleeting-removing"

	DefaultProxmoxTaskWaitInterval   = 1
	DefaultProxmoxTaskWaitTimeout    = 300
	DefaultProxmoxAgentStartTimeout  = 120
	DefaultProxmoxHTTPTimeout        = 30
	DefaultMaxConcurrentClones       = 1
	DefaultStaleCreatingTimeout      = 600
	DefaultNetworkInterfaceAutoValue = "auto"
)

// Settings holds the configuration for the Proxmox fleeting plugin.
type Settings struct {
	// Proxmox VE URL.
	URL string `json:"url"`

	// Optional list of Proxmox VE URLs for API failover (used alongside URL if both are set).
	URLs []string `json:"urls,omitempty"`

	// If true then TLS certificate verification is disabled.
	InsecureSkipTLSVerify bool `json:"insecure_skip_tls_verify"`

	// Optional path to PEM-encoded CA certificate bundle for verifying Proxmox TLS certificates.
	CACertPath string `json:"ca_cert_path,omitempty"`

	// Path to Proxmox VE credentials file.
	CredentialsFilePath string `json:"credentials_file_path"`

	// Name of the Proxmox VE pool to use.
	Pool string `json:"pool"`

	// Name of the Proxmox VE storage to use.
	Storage string `json:"storage"`

	// ID of the Proxmox VE VM to create instances from.
	TemplateID *int `json:"template_id,omitempty"`

	// Maximum instances than can be deployed.
	MaxInstances *int `json:"max_instances,omitempty"`

	// Maximum concurrent clone tasks allowed at a time.
	MaxConcurrentClones *int `json:"max_concurrent_clones,omitempty"`

	// Optional clone disk bandwidth limit in KiB/s.
	CloneBWLimit *uint64 `json:"clone_bwlimit,omitempty"`

	// Optional allowlist of Proxmox cluster node names eligible for instance placement.
	AllowedNodes []string `json:"allowed_nodes,omitempty"`

	// Optional denylist of Proxmox cluster node names excluded from instance placement.
	ExcludedNodes []string `json:"excluded_nodes,omitempty"`

	// Optional hard upper bound for node memory utilization ratio (0, 1] during scheduling.
	NodeMaxMemRatio *float64 `json:"node_max_mem_ratio,omitempty"`

	// Optional hard upper bound for node CPU utilization ratio (0, 1] during scheduling.
	NodeMaxCPURatio *float64 `json:"node_max_cpu_ratio,omitempty"`

	// Optional CPU cores override applied to cloned VMs before boot.
	VMCores *int `json:"vm_cores,omitempty"`

	// Optional memory size override in MiB applied to cloned VMs before boot.
	VMMemory *int `json:"vm_memory,omitempty"`

	// Network interface to read instance's IP address from ("auto" or comma-separated fallback list supported).
	InstanceNetworkInterface string `json:"instance_network_interface"`

	// Network protocol to look for when discovering instance's IP address.
	InstanceNetworkProtocol NetworkProtocol `json:"instance_network_protocol"`

	// Name to set for instances during creation.
	InstanceNameCreating string `json:"instance_name_creating"`

	// Name to set for running instances.
	InstanceNameRunning string `json:"instance_name_running"`

	// Name to set for instances during removal.
	InstanceNameRemoving string `json:"instance_name_removing"`

	// If true, append "-<vmid>" to instance names so each VM has a unique name in Proxmox.
	InstanceNameIncludeVMID bool `json:"instance_name_include_vmid,omitempty"`

	// Tags to set for instances during creation, semicolon delimited.
	InstanceTagsCreating string `json:"instance_tags_creating"`

	// Tags to set for running instances, semicolon delimited.
	InstanceTagsRunning string `json:"instance_tags_running"`

	// Tags to set for instances during removal, semicolon delimited.
	InstanceTagsRemoving string `json:"instance_tags_removing"`

	// How often (in seconds) task status should be queried.
	ProxmoxTaskWaitInterval *int `json:"proxmox_task_wait_interval,omitempty"`

	// Maximum time (in seconds) to wait for a Proxmox task (e.g. clone/start/config).
	ProxmoxTaskWaitTimeout *int `json:"proxmox_task_wait_timeout,omitempty"`

	// Maximum time (in seconds) to wait for QEMU guest agent and IP discovery.
	ProxmoxAgentStartTimeout *int `json:"proxmox_agent_start_timeout,omitempty"`

	// Per-request HTTP timeout (in seconds) for Proxmox API calls.
	ProxmoxHTTPTimeout *int `json:"proxmox_http_timeout,omitempty"`

	// Time (in seconds) before an orphaned creating VM with no active deploy is auto-reaped.
	StaleCreatingTimeout *int `json:"stale_creating_timeout,omitempty"`
}

// ConfiguredURLs returns deduplicated Proxmox API URLs from URL and URLs in priority order.
func (s *Settings) ConfiguredURLs() []string {
	seen := make(map[string]struct{}, 1+len(s.URLs))
	urls := make([]string, 0, 1+len(s.URLs))

	addURL := func(raw string) {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return
		}
		if _, exists := seen[trimmed]; exists {
			return
		}
		seen[trimmed] = struct{}{}
		urls = append(urls, trimmed)
	}

	addURL(s.URL)
	for _, rawURL := range s.URLs {
		addURL(rawURL)
	}

	return urls
}

func (s *Settings) FillWithDefaults() {
	if s.InstanceNetworkInterface == "" {
		s.InstanceNetworkInterface = DefaultInstanceNetworkInterface
	}

	if s.InstanceNetworkProtocol == "" {
		s.InstanceNetworkProtocol = DefaultInstanceNetworkProtocol
	}

	if s.InstanceNameCreating == "" {
		s.InstanceNameCreating = DefaultInstanceNameCreating
	}

	if s.InstanceNameRunning == "" {
		s.InstanceNameRunning = DefaultInstanceNameRunning
	}

	if s.InstanceNameRemoving == "" {
		s.InstanceNameRemoving = DefaultInstanceNameRemoving
	}

	fillIntDefault(&s.ProxmoxTaskWaitInterval, DefaultProxmoxTaskWaitInterval)
	fillIntDefault(&s.ProxmoxTaskWaitTimeout, DefaultProxmoxTaskWaitTimeout)
	fillIntDefault(&s.ProxmoxAgentStartTimeout, DefaultProxmoxAgentStartTimeout)
	fillIntDefault(&s.ProxmoxHTTPTimeout, DefaultProxmoxHTTPTimeout)
	fillIntDefault(&s.MaxConcurrentClones, DefaultMaxConcurrentClones)
	fillIntDefault(&s.StaleCreatingTimeout, DefaultStaleCreatingTimeout)
}

func fillIntDefault(target **int, defaultVal int) {
	if *target == nil || **target <= 0 {
		val := defaultVal
		*target = &val
	}
}

func (s *Settings) CheckRequiredFields() error {
	if len(s.ConfiguredURLs()) == 0 {
		return fmt.Errorf("%w: url", ErrRequiredSettingMissing)
	}

	if s.CredentialsFilePath == "" {
		return fmt.Errorf("%w: credentials_file_path", ErrRequiredSettingMissing)
	}

	if s.Pool == "" {
		return fmt.Errorf("%w: pool", ErrRequiredSettingMissing)
	}

	if s.TemplateID == nil {
		return fmt.Errorf("%w: template_id", ErrRequiredSettingMissing)
	}

	if s.MaxInstances == nil {
		return fmt.Errorf("%w: max_instances", ErrRequiredSettingMissing)
	}

	return s.validateOptionalParameters()
}

func (s *Settings) validateOptionalParameters() error {
	if s.InstanceNetworkProtocol != "" && s.InstanceNetworkProtocol != NetworkProtocolIPv4 && s.InstanceNetworkProtocol != NetworkProtocolIPv6 && s.InstanceNetworkProtocol != NetworkProtocolAny {
		return fmt.Errorf("%w: instance_network_protocol: must be ipv4, ipv6 or any", ErrSettingInvalidParameter)
	}

	if err := validatePositiveIntPtr("max_concurrent_clones", s.MaxConcurrentClones); err != nil {
		return err
	}
	if err := validatePositiveIntPtr("vm_cores", s.VMCores); err != nil {
		return err
	}
	if err := validatePositiveIntPtr("vm_memory", s.VMMemory); err != nil {
		return err
	}
	if err := validateRatioPtr("node_max_mem_ratio", s.NodeMaxMemRatio); err != nil {
		return err
	}
	if err := validateRatioPtr("node_max_cpu_ratio", s.NodeMaxCPURatio); err != nil {
		return err
	}

	return nil
}

func validatePositiveIntPtr(field string, val *int) error {
	if val != nil && *val <= 0 {
		return fmt.Errorf("%w: %s: must be greater than 0", ErrSettingInvalidParameter, field)
	}

	return nil
}

func validateRatioPtr(field string, val *float64) error {
	if val != nil && (*val <= 0 || *val > 1) {
		return fmt.Errorf("%w: %s: must be in (0, 1]", ErrSettingInvalidParameter, field)
	}

	return nil
}

package plugin

import (
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:gochecknoglobals
var (
	sampleURL = "https://example.com"
	//nolint:gosec
	sampleCredentialsPath      = "/tmp/proxmox_credentials.json"
	samplePool                 = "sample_pool"
	sampleStorage              = "sample_storage"
	sampleTemplateID           = 20
	sampleMaxInstances         = 7
	sampleInstanceNameCreating = "proxmox-creating"
	sampleInstanceNameRunning  = "running-prox"
	sampleInstanceNameRemoving = "proxve-removing"
)

func TestSettings_fillWithDefaults(t *testing.T) {
	settings := Settings{}
	settings.FillWithDefaults()

	// Don't use consts here, we want to ensure they are not changed
	require.False(t, settings.InsecureSkipTLSVerify)
	require.Equal(t, "fleeting-creating", settings.InstanceNameCreating)
	require.Equal(t, "fleeting-running", settings.InstanceNameRunning)
	require.Equal(t, "fleeting-removing", settings.InstanceNameRemoving)
	require.Equal(t, "ens18", settings.InstanceNetworkInterface)
	require.Equal(t, "ipv4", settings.InstanceNetworkProtocol)
	require.NotNil(t, settings.ProxmoxTaskWaitInterval)
	require.Equal(t, 1, *settings.ProxmoxTaskWaitInterval)
	require.NotNil(t, settings.ProxmoxTaskWaitTimeout)
	require.Equal(t, 300, *settings.ProxmoxTaskWaitTimeout)
	require.NotNil(t, settings.ProxmoxAgentStartTimeout)
	require.Equal(t, 120, *settings.ProxmoxAgentStartTimeout)
	require.NotNil(t, settings.ProxmoxHTTPTimeout)
	require.Equal(t, 30, *settings.ProxmoxHTTPTimeout)
	require.NotNil(t, settings.MaxConcurrentClones)
	require.Equal(t, 1, *settings.MaxConcurrentClones)
	require.NotNil(t, settings.StaleCreatingTimeout)
	require.Equal(t, 600, *settings.StaleCreatingTimeout)

	customInterval := 3
	customTaskTimeout := 600
	customConcurrent := 4
	settings2 := Settings{
		InstanceNameCreating:    sampleInstanceNameCreating,
		InstanceNameRunning:     sampleInstanceNameRunning,
		InstanceNameRemoving:    sampleInstanceNameRemoving,
		InstanceTagsRunning:     "gitlab-runner",
		ProxmoxTaskWaitInterval: &customInterval,
		ProxmoxTaskWaitTimeout:  &customTaskTimeout,
		MaxConcurrentClones:     &customConcurrent,
	}
	settings2.FillWithDefaults()

	require.Equal(t, sampleInstanceNameCreating, settings2.InstanceNameCreating)
	require.Equal(t, sampleInstanceNameRunning, settings2.InstanceNameRunning)
	require.Equal(t, sampleInstanceNameRemoving, settings2.InstanceNameRemoving)
	require.Equal(t, "gitlab-runner", settings2.InstanceTagsRunning)
	require.Equal(t, 3, *settings2.ProxmoxTaskWaitInterval)
	require.Equal(t, 600, *settings2.ProxmoxTaskWaitTimeout)
	require.Equal(t, 4, *settings2.MaxConcurrentClones)
}

func TestSettings_ConfiguredURLs(t *testing.T) {
	settings := Settings{
		URL:  " https://pve1.example.com:8006 ",
		URLs: []string{"", "https://pve2.example.com:8006", "https://pve1.example.com:8006", "  https://pve3.example.com:8006"},
	}

	require.Equal(t, []string{
		"https://pve1.example.com:8006",
		"https://pve2.example.com:8006",
		"https://pve3.example.com:8006",
	}, settings.ConfiguredURLs())
}

func TestSettings_checkRequiredFields(t *testing.T) {
	zero := 0
	negative := -1
	ratioZero := 0.0
	ratioOverOne := 1.1
	ratioValid := 0.85

	tests := []struct {
		name          string
		settings      Settings
		expectedError error
	}{
		{
			name: "Missing URL",
			settings: Settings{
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				Storage:             sampleStorage,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			name: "URLs only without URL",
			settings: Settings{
				URLs:                []string{sampleURL},
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				Storage:             sampleStorage,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
			},
			expectedError: nil,
		},
		{
			name: "Missing credentials file path",
			settings: Settings{
				URL:          sampleURL,
				Pool:         samplePool,
				Storage:      sampleStorage,
				TemplateID:   &sampleTemplateID,
				MaxInstances: &sampleMaxInstances,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			name: "Missing pool",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Storage:             sampleStorage,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			name: "Missing storage",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
			},
			expectedError: nil,
		},
		{
			name: "Missing template id",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				Storage:             sampleStorage,
				TemplateID:          nil,
				MaxInstances:        &sampleMaxInstances,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			name: "Missing max instances",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				Storage:             sampleStorage,
				TemplateID:          nil,
				MaxInstances:        nil,
			},
			expectedError: ErrRequiredSettingMissing,
		},
		{
			name: "No missing parameters",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				Storage:             sampleStorage,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
				NodeMaxMemRatio:     &ratioValid,
				NodeMaxCPURatio:     &ratioValid,
			},
			expectedError: nil,
		},
		{
			name: "Invalid protocol",
			settings: Settings{
				URL:                     sampleURL,
				CredentialsFilePath:     sampleCredentialsPath,
				Pool:                    samplePool,
				Storage:                 sampleStorage,
				TemplateID:              &sampleTemplateID,
				MaxInstances:            &sampleMaxInstances,
				InstanceNetworkProtocol: "invalid-protocol",
			},
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name: "Invalid max_concurrent_clones",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
				MaxConcurrentClones: &zero,
			},
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name: "Invalid vm_cores",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
				VMCores:             &negative,
			},
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name: "Invalid vm_memory",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
				VMMemory:            &zero,
			},
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name: "Invalid node_max_mem_ratio zero",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
				NodeMaxMemRatio:     &ratioZero,
			},
			expectedError: ErrSettingInvalidParameter,
		},
		{
			name: "Invalid node_max_cpu_ratio above one",
			settings: Settings{
				URL:                 sampleURL,
				CredentialsFilePath: sampleCredentialsPath,
				Pool:                samplePool,
				TemplateID:          &sampleTemplateID,
				MaxInstances:        &sampleMaxInstances,
				NodeMaxCPURatio:     &ratioOverOne,
			},
			expectedError: ErrSettingInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.settings.CheckRequiredFields()
			require.ErrorIs(t, err, tt.expectedError)
		})
	}
}

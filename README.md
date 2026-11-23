# Fleeting plugin for Proxmox Virtual Environment

This is a [fleeting](https://gitlab.com/gitlab-org/fleeting/fleeting) plugin for [Proxmox Virtual Environment](https://www.proxmox.com/en/proxmox-virtual-environment/overview).

## Installation

See [Releases](https://github.com/LukasK32/fleeting-plugin-proxmox/releases) for available versions and installation instructions.

## Configuration

### Plugin settings

| Parameter                     | Type                      | Default value                      | Description                                                                                                          |
| ----------------------------- | ------------------------- | ---------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `url`                         | string                    | N/A (required unless `urls` set)   | Proxmox VE URL.                                                                                                      |
| `urls`                        | []string                  | `[]`                               | Optional list of Proxmox VE URLs for API failover (used alongside `url` if both are set).                            |
| `insecure_skip_tls_verify`    | bool                      | `false`                            | If `true` then TLS certificate verification is disabled.                                                             |
| `ca_cert_path`                | string                    | `""`                               | Optional path to a PEM-encoded CA bundle for verifying Proxmox TLS certificates.                                     |
| `credentials_file_path`       | string                    | N/A (required)                     | Path to Proxmox VE credentials file.                                                                                 |
| `pool`                        | string                    | N/A (required)                     | Name of the Proxmox VE pool to use.                                                                                  |
| `storage`                     | string                    | N/A (required if template is a VM) | Name of the Proxmox VE storage to use.                                                                               |
| `template_id`                 | int                       | N/A (required)                     | ID of the Proxmox VE VM to create instances from.                                                                    |
| `max_instances`               | int                       | N/A (required)                     | Maximum instances than can be deployed.                                                                              |
| `max_concurrent_clones`       | int                       | `1`                                | Maximum concurrent clone tasks allowed at a time (`>= 1`).                                                           |
| `clone_bwlimit`               | uint64                    | `nil` (PVE default)                | Optional disk clone bandwidth limit in KiB/s.                                                                        |
| `allowed_nodes`               | []string                  | `[]`                               | Optional allowlist of Proxmox cluster nodes eligible for instance placement.                                         |
| `excluded_nodes`              | []string                  | `[]`                               | Optional denylist of Proxmox cluster nodes excluded from instance placement (takes precedence over `allowed_nodes`). |
| `node_max_mem_ratio`          | float64                   | `nil` (no hard cap)                | Optional hard upper bound `(0, 1]` on estimated node memory utilization during scheduling.                           |
| `node_max_cpu_ratio`          | float64                   | `nil` (no hard cap)                | Optional hard upper bound `(0, 1]` on node CPU utilization during scheduling.                                        |
| `vm_cores`                    | int                       | `nil` (template default)           | Optional CPU cores override applied to cloned VMs before boot.                                                       |
| `vm_memory`                   | int                       | `nil` (template default)           | Optional RAM size override in MiB applied to cloned VMs before boot.                                                 |
| `instance_network_interface`  | string                    | `ens18`                            | Network interface to read instance's IP from (`auto` or comma-separated fallback list supported).                    |
| `instance_network_protocol`   | `any` or `ipv4` or `ipv6` | `ipv4`                             | Network protocol to look for when discovering instance's IP address. `any` prioritizes IPv6.                         |
| `instance_name_creating`      | string                    | `fleeting-creating`                | Name to set for instances during creation.                                                                           |
| `instance_name_running`       | string                    | `fleeting-running`                 | Name to set for running instances.                                                                                   |
| `instance_name_removing`      | string                    | `fleeting-removing`                | Name to set for instances during removal.                                                                            |
| `instance_name_include_vmid`  | bool                      | `false`                            | If `true`, appends `-<vmid>` to instance names (e.g. `fleeting-running-105`).                                        |
| `instance_tags_creating`      | string                    | `""`                               | Semicolon-delimited tags to set for instances during creation.                                                       |
| `instance_tags_running`       | string                    | `""`                               | Semicolon-delimited tags to set for running instances.                                                               |
| `instance_tags_removing`      | string                    | `""`                               | Semicolon-delimited tags to set for instances during removal.                                                        |
| `proxmox_task_wait_interval`  | int                       | `1`                                | Interval in seconds between Proxmox task status checks.                                                              |
| `proxmox_task_wait_timeout`   | int                       | `300`                              | Maximum timeout in seconds for waiting on a Proxmox task.                                                            |
| `proxmox_agent_start_timeout` | int                       | `120`                              | Maximum timeout in seconds for waiting on QEMU guest agent and IP discovery.                                         |
| `proxmox_http_timeout`        | int                       | `30`                               | Per-request HTTP timeout in seconds for Proxmox API calls.                                                           |
| `stale_creating_timeout`      | int                       | `600`                              | Seconds before an orphaned creating VM without an active deploy is automatically marked for removal.                 |

### Credentials file

Both **API Token** (recommended) and **Username/Password** authentication are supported.

#### API Token (recommended)

```json
{
  "token_id": "gitlab-runner@pve!fleeting",
  "secret": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

#### Username / Password

<!-- TODO: Document `path` and `privs`  -->
| Parameter  | Type   | Description                                       |
| ---------- | ------ | ------------------------------------------------- |
| `token_id` | string | Proxmox API token ID (e.g. `user@realm!tokenname`) |
| `secret`   | string | Proxmox API token secret                          |
| `realm`    | string | Authentication Realm (for username/password auth) |
| `username` | string | User name                                         |
| `password` | string | User password                                     |
| `otp`      | string | One-time password for 2FA                         |

### Template VM configuration

The template must be a bootable VM with enabled DHCP and QEMU guest agent installed. See [Proxmox documentation](https://pve.proxmox.com/wiki/Qemu-guest-agent) for more details.

### Proxmox configuration

You **MUST** create a **DEDICATED** user, pool and storage for usage with this plugin. Any other configuration is untested and unsupported.

After creating a **DEDICATED** user, pool and storage follow procedure below to add required permissions:

1. Add template VM as a member to the pool.
2. Add storage as a member to the pool.
3. Add following roles for the user to the pool:
   * `PVEVMAdmin`,
   * `PVEPoolUser`,
   * `PVEDatastoreUser`.
4. Add following role for the user to the network that you will use for deployed VMs:
    * `PVESDNAdmin`.
5. Add following role for the user to the node with the storage, network, template etc.:
    * `PVEAuditor` without propagation.

## Development

### Integration tests

1. Create and fill out `credentials.json` file in project root (see [Credentials file](#credentials-file) for details)
    ```bash
    cp credentials.example.json credentials.json
    ```
2. Create and fill out `config.json` file in project root (see [Plugin settings](#plugin-settings) for details)
    ```bash
    cp config.example.json config.json
    ```
3. Run integration tests:
    ```bash
    make integration-test
    ```

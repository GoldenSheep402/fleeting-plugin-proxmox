package plugin

import (
	"errors"
	"net"
	"strings"

	"github.com/luthermonson/go-proxmox"
)

var ErrNoIPAddress = errors.New("failed to determine IP address for instance")

// Determines internal and external address for given interfaces.
func determineAddresses(networkInterfaces []*proxmox.AgentNetworkIface, requestedInterface string, requestedProtocol NetworkProtocol) (string, string, error) {
	trimmed := strings.TrimSpace(requestedInterface)

	if strings.EqualFold(trimmed, DefaultNetworkInterfaceAutoValue) {
		for _, networkInterface := range networkInterfaces {
			if networkInterface == nil || isIgnoredAutoInterface(networkInterface.Name) {
				continue
			}

			internalAddr, externalAddr, ok := matchInterfaceForProtocol(networkInterface, requestedProtocol)
			if ok {
				return internalAddr, externalAddr, nil
			}
		}

		return "", "", ErrNoIPAddress
	}

	if strings.Contains(trimmed, ",") {
		for _, candidate := range strings.Split(trimmed, ",") {
			candidateName := strings.TrimSpace(candidate)
			if candidateName == "" {
				continue
			}

			internalAddr, externalAddr, ok := findNamedInterfaceForProtocol(networkInterfaces, candidateName, requestedProtocol)
			if ok {
				return internalAddr, externalAddr, nil
			}
		}

		return "", "", ErrNoIPAddress
	}

	internalAddr, externalAddr, ok := findNamedInterfaceForProtocol(networkInterfaces, trimmed, requestedProtocol)
	if ok {
		return internalAddr, externalAddr, nil
	}

	// Did not find the address in requested protocol
	return "", "", ErrNoIPAddress
}

func findNamedInterfaceForProtocol(
	networkInterfaces []*proxmox.AgentNetworkIface,
	requestedInterface string,
	requestedProtocol NetworkProtocol,
) (string, string, bool) {
	internalIPv4, externalIPv4, internalIPv6, externalIPv6 := determinePossibleAddresses(networkInterfaces, requestedInterface)

	return selectAddressesByProtocol(internalIPv4, externalIPv4, internalIPv6, externalIPv6, requestedProtocol)
}

func matchInterfaceForProtocol(networkInterface *proxmox.AgentNetworkIface, requestedProtocol NetworkProtocol) (string, string, bool) {
	internalIPv4, externalIPv4, internalIPv6, externalIPv6 := extractInterfaceAddresses(networkInterface)

	return selectAddressesByProtocol(internalIPv4, externalIPv4, internalIPv6, externalIPv6, requestedProtocol)
}

func selectAddressesByProtocol(
	internalIPv4, externalIPv4, internalIPv6, externalIPv6 string,
	requestedProtocol NetworkProtocol,
) (string, string, bool) {
	// IPv6 (or Any)
	if requestedProtocol == NetworkProtocolIPv6 || requestedProtocol == NetworkProtocolAny {
		// External address is required so use internal if needed
		if externalIPv6 == "" {
			externalIPv6 = internalIPv6
		}

		if externalIPv6 != "" {
			return internalIPv6, externalIPv6, true
		}
	}

	// IPv4 (or Any)
	if requestedProtocol == NetworkProtocolIPv4 || requestedProtocol == NetworkProtocolAny {
		// External address is required so use internal if needed
		if externalIPv4 == "" {
			externalIPv4 = internalIPv4
		}

		if externalIPv4 != "" {
			return internalIPv4, externalIPv4, true
		}
	}

	return "", "", false
}

func isIgnoredAutoInterface(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	if lower == "" || lower == "lo" {
		return true
	}

	ignoredPrefixes := []string{
		"docker",
		"br-",
		"veth",
		"cni",
		"flannel",
		"cali",
		"virbr",
		"podman",
		"tailscale",
		"wg",
		"kube",
	}

	for _, prefix := range ignoredPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}

	return false
}

// Finds possible IPv4 and IPv6 addresses for given interfaces.
//
//nolint:nakedret,nonamedreturns
func determinePossibleAddresses(networkInterfaces []*proxmox.AgentNetworkIface, requestedInterface string) (internalIPv4, externalIPv4, internalIPv6, externalIPv6 string) {
	for _, networkInterface := range networkInterfaces {
		if networkInterface == nil || networkInterface.Name != requestedInterface {
			continue
		}

		return extractInterfaceAddresses(networkInterface)
	}

	return
}

//nolint:nakedret,nonamedreturns
func extractInterfaceAddresses(networkInterface *proxmox.AgentNetworkIface) (internalIPv4, externalIPv4, internalIPv6, externalIPv6 string) {
	for _, address := range networkInterface.IPAddresses {
		if address == nil {
			continue
		}

		parsedAddress := net.ParseIP(address.IPAddress)
		if parsedAddress == nil {
			continue
		}

		if parsedAddress.IsLoopback() || parsedAddress.IsUnspecified() {
			continue
		}

		if address.IPAddressType == "ipv4" {
			if parsedAddress.IsPrivate() {
				internalIPv4 = address.IPAddress
			} else if parsedAddress.IsGlobalUnicast() {
				externalIPv4 = address.IPAddress
			}
		}

		if address.IPAddressType == "ipv6" {
			if parsedAddress.IsPrivate() {
				internalIPv6 = address.IPAddress
			} else if parsedAddress.IsGlobalUnicast() {
				externalIPv6 = address.IPAddress
			}
		}
	}

	return
}

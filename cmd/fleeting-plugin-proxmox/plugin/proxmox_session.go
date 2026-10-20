package plugin

import (
	"context"
	"time"
)

const (
	sessionTicketRefreshInterval = 1 * time.Hour
	sessionTicketRefreshTimeout  = 15 * time.Second
)

func (ig *InstanceGroup) startSessionTicketRefresher() {
	ig.sessionTicketRefresherWaitGroup.Add(1)

	go func() {
		defer ig.sessionTicketRefresherWaitGroup.Done()
		ig.runSessionTicketRefresher()
	}()
}

func (ig *InstanceGroup) runSessionTicketRefresher() {
	for {
		select {
		case <-ig.sessionTicketRefresherShutdownTrigger:
			return
		case <-time.After(sessionTicketRefreshInterval):
			ig.refreshProxmoxSession()
		}
	}
}

func (ig *InstanceGroup) refreshProxmoxSession() {
	ctx, cancel := context.WithTimeout(context.Background(), sessionTicketRefreshTimeout)
	defer cancel()

	if err := ig.proxmox.RefreshTicket(ctx); err == nil {
		ig.log.Info("refreshed proxmox session")
		return
	}

	credentials, err := ig.getProxmoxCredentials()
	if err != nil {
		ig.log.Error("failed to refresh proxmox session, could not read credentials", "err", err)
		return
	}

	if _, err := ig.proxmox.Ticket(ctx, credentials); err != nil {
		ig.log.Error("failed to refresh proxmox session", "err", err)
		return
	}

	ig.log.Info("refreshed proxmox session")
}

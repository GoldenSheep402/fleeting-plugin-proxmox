package plugin

import (
	"context"
	"time"

	"github.com/luthermonson/go-proxmox"
)

const (
	sessionTicketRefreshInterval   = 1 * time.Hour
	sessionTicketRefreshTimeout    = 15 * time.Second
	sessionTicketRetryInitialDelay = 10 * time.Second
	sessionTicketRetryMaxDelay     = 5 * time.Minute
)

func (ig *InstanceGroup) startSessionTicketRefresher() {
	ig.sessionTicketRefresherWaitGroup.Add(1)

	go func() {
		defer ig.sessionTicketRefresherWaitGroup.Done()
		ig.runSessionTicketRefresher()
	}()
}

func (ig *InstanceGroup) runSessionTicketRefresher() {
	baseCtx := ig.lifecycleContext()
	waitDuration := sessionTicketRefreshInterval

	for {
		timer := time.NewTimer(waitDuration)
		select {
		case <-baseCtx.Done():
			timer.Stop()
			return
		case <-ig.sessionTicketRefresherShutdownTrigger:
			timer.Stop()
			return
		case <-timer.C:
			if ig.refreshProxmoxSession(baseCtx) {
				waitDuration = sessionTicketRefreshInterval
				continue
			}

			waitDuration = nextSessionRetryDelay(waitDuration)
		}
	}
}

func nextSessionRetryDelay(current time.Duration) time.Duration {
	if current >= sessionTicketRefreshInterval || current < sessionTicketRetryInitialDelay {
		return sessionTicketRetryInitialDelay
	}

	next := current * 2
	if next > sessionTicketRetryMaxDelay {
		return sessionTicketRetryMaxDelay
	}

	return next
}

func (ig *InstanceGroup) refreshProxmoxSession(baseCtx context.Context) bool {
	if ig.useAPIToken {
		return true
	}

	client := ig.currentClient()
	if client == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(baseCtx, sessionTicketRefreshTimeout)
	defer cancel()

	if err := client.RefreshTicket(ctx); err == nil {
		ig.logInfo("refreshed proxmox session")
		return true
	}

	credentials, err := ig.getProxmoxCredentials()
	if err != nil {
		ig.logError("failed to refresh proxmox session, could not read credentials", "err", err)
		return false
	}

	if credentials.isAPIToken() {
		return true
	}

	pveCreds := &proxmox.Credentials{
		Realm:    credentials.Realm,
		Username: credentials.Username,
		Password: credentials.Password,
	}

	if _, err := client.Ticket(ctx, pveCreds); err != nil {
		ig.logError("failed to refresh proxmox session", "err", err)
		return false
	}

	ig.logInfo("refreshed proxmox session")

	return true
}

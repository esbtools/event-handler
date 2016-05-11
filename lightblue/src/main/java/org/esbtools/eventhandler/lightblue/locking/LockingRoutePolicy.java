package org.esbtools.eventhandler.lightblue.locking;

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.support.RoutePolicySupport;

public class LockingRoutePolicy extends RoutePolicySupport {

    private final String identifier;

    private final LockStrategy lockStrategy;

    private @Nullable LockedResource<String> lock;

    public LockingRoutePolicy(String identifier, LockStrategy lockStrategy) {
        this.identifier = identifier;
        this.lockStrategy = lockStrategy;
    }

    @Override
    public void onStop(Route route) {
        releaseLock();
    }

    @Override
    public void onSuspend(Route route) {
        releaseLock();
    }

    @Override
    public synchronized void onExchangeBegin(Route route, Exchange exchange) {
        if (lock != null) {
            try {
                lock.ensureAcquiredOrThrow("Lost lock");
                return;
            } catch (LostLockException e) {
                log.warn("Lost lock w id: " + identifier + ", trying to reacquire...", e);
                lock = null;
            }
        }

        try {
            lock = lockStrategy.tryAcquire(identifier);
        } catch (LockNotAvailableException e) {
            log.debug("Lock not available, assuming " +
                    "another thread is holding lock w/ id: " + identifier, e);
            exchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
        }
    }

    private synchronized void releaseLock() {
        if (lock == null) return;

        try {
            lock.close();
        } catch (IOException e) {
            log.warn("IOException trying to release lock w/ identifier " + identifier, e);
        }

        lock = null;
    }
}
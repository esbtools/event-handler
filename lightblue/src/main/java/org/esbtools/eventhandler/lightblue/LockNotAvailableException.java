package org.esbtools.eventhandler.lightblue;

public class LockNotAvailableException extends Exception {
    public LockNotAvailableException(String callerId, String resourceId) {
        super("callerId: " + callerId + ", resourceId: " + resourceId);
    }
}

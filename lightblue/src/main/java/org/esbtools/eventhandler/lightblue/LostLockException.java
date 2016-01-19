package org.esbtools.eventhandler.lightblue;

public class LostLockException extends Exception {
    private final LightblueLock lock;

    public LostLockException(LightblueLock lock, String message) {
        super(message + " [Lock: " + lock + "]");

        this.lock = lock;
    }
}

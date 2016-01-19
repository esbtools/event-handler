package org.esbtools.eventhandler.lightblue;

import com.redhat.lightblue.client.LightblueException;

import java.io.Closeable;

public interface LightblueLock extends Closeable {
    boolean ping() throws LightblueException;
}

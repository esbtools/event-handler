package org.esbtools.eventhandler.lightblue;

import com.google.common.util.concurrent.Futures;
import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

import java.util.Collection;
import java.util.concurrent.Future;

public class UnparseableNotification implements LightblueNotification {
    private final Exception exception;
    private final NotificationEntity entity;

    public UnparseableNotification(Exception exception, NotificationEntity entity) {
        this.exception = exception;
        this.entity = entity;
    }

    @Override
    public NotificationEntity wrappedNotificationEntity() {
        return entity;
    }

    @Override
    public Future<Collection<DocumentEvent>> toDocumentEvents() {
        return Futures.immediateFailedFuture(exception);
    }
}

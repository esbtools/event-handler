package org.esbtools.eventhandler.lightblue;

import org.esbtools.eventhandler.DocumentEvent;

import com.google.common.util.concurrent.Futures;

import java.util.concurrent.Future;

public class UnparseableDocumentEvent implements LightblueDocumentEvent {
    private final Exception exception;
    private final DocumentEventEntity entity;

    public UnparseableDocumentEvent(Exception exception, DocumentEventEntity entity) {
        this.exception = exception;
        this.entity = entity;
    }

    @Override
    public Future<?> lookupDocument() {
        return Futures.immediateFailedFuture(exception);
    }

    @Override
    public boolean isSupersededBy(DocumentEvent event) {
        return false;
    }

    @Override
    public boolean couldMergeWith(DocumentEvent event) {
        return false;
    }

    @Override
    public LightblueDocumentEvent merge(DocumentEvent event) {
        throw new UnsupportedOperationException("Cannot merge with failed event!");
    }

    @Override
    public Identity identity() {
        return new TypeAndValueIdentity(UnparseableDocumentEvent.class, entity.get_id());
    }

    @Override
    public DocumentEventEntity wrappedDocumentEventEntity() {
        return entity;
    }
}

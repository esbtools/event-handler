/*
 *  Copyright 2015 esbtools Contributors and/or its affiliates.
 *
 *  This file is part of esbtools.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.esbtools.eventhandler.lightblue;

import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.DocumentRepository;
import org.esbtools.eventhandler.LookupResult;
import org.esbtools.eventhandler.SimpleLookupResult;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.model.DataError;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.request.DataBulkRequest;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.response.LightblueBulkDataResponse;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;
import com.redhat.lightblue.client.response.LightblueException;
import com.redhat.lightblue.client.response.LightblueResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LightblueDocumentRepository implements DocumentRepository {
    private final LightblueClient lightblue;

    public LightblueDocumentRepository(LightblueClient lightblue) {
        this.lightblue = lightblue;
    }

    @Override
    public Collection<LookupResult> lookupDocumentsForEvents(
            Collection<DocumentEvent> documentEvents) throws LightblueException {
        DataBulkRequest bulkRequest = new DataBulkRequest();
        Map<LightblueDocumentEvent, List<DataFindRequest>> requestsForEvents = new HashMap<>();

        for (DocumentEvent documentEvent : documentEvents) {
            LightblueDocumentEvent lightblueDocEvent = asLightblueDocumentEvent(documentEvent);
            List<DataFindRequest> requests = lightblueDocEvent.dataRequests();
            requestsForEvents.put(lightblueDocEvent, requests);
            bulkRequest.addAll(requests);
        }

        LightblueBulkDataResponse bulkResponse = lightblue.bulkData(bulkRequest);
        List<LookupResult> lookupResults = new ArrayList<>(documentEvents.size());

        for (Map.Entry<LightblueDocumentEvent, List<DataFindRequest>> entry :
                requestsForEvents.entrySet()) {
            LightblueDocumentEvent event = entry.getKey();
            List<DataFindRequest> eventRequests = entry.getValue();
            List<LightblueResponse> eventResponses = eventRequests.stream()
                    .map(bulkResponse::getResponse)
                    .collect(Collectors.toList());

            LookupResult result = makeLookupResult(event, eventResponses);
            lookupResults.add(result);
        }

        return lookupResults;
    }

    private static LookupResult makeLookupResult(LightblueDocumentEvent event,
            List<LightblueResponse> eventResponses) {
        List<Error> errors = new ArrayList<>();
        List<DataError> dataErrors = new ArrayList<>();
        List<LightblueDataResponse> successfulResponses = new ArrayList<>(eventResponses.size());

        for (LightblueResponse eventResponse : eventResponses) {
            if (eventResponse instanceof LightblueErrorResponse) {
                LightblueErrorResponse errorResponse = (LightblueErrorResponse) eventResponse;
                if (errorResponse.hasError()) {
                    Collections.addAll(errors, errorResponse.getErrors());
                    continue;
                }

                if (errorResponse.hasDataErrors()) {
                    Collections.addAll(dataErrors, errorResponse.getDataErrors());
                    continue;
                }
            }

            if (!(eventResponse instanceof LightblueDataResponse)) {
                throw new IllegalArgumentException("Expected successful lightblue response to be " +
                        "a LightblueDataResponse but was: " + eventResponse);
            }

            successfulResponses.add((LightblueDataResponse) eventResponse);
        }

        if (errors.isEmpty() && dataErrors.isEmpty()) {
            LightblueDocument document = event.makeDocumentFromResponses(successfulResponses);
            return SimpleLookupResult.withErrors(document, event);
        }

        LightblueErrorDocument document = new LightblueErrorDocument(errors, dataErrors,
                successfulResponses);
        return SimpleLookupResult.withoutErrors(document, event);
    }

    private static LightblueDocumentEvent asLightblueDocumentEvent(DocumentEvent documentEvent) {
        if (!(documentEvent instanceof LightblueDocumentEvent)) {
            throw new IllegalArgumentException("TODO: make nice message");
        }

        return (LightblueDocumentEvent) documentEvent;
    }
}

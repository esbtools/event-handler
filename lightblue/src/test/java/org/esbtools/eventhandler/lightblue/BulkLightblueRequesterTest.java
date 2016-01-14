package org.esbtools.eventhandler.lightblue;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.response.LightblueException;

import org.esbtools.eventhandler.lightblue.testing.LightblueClientConfigurations;
import org.esbtools.eventhandler.lightblue.testing.LightblueClients;
import org.esbtools.eventhandler.lightblue.testing.TestMetadataJson;
import org.esbtools.eventhandler.lightblue.testing.TestUser;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class BulkLightblueRequesterTest {
    @ClassRule
    public static LightblueExternalResource lightblueExternalResource
            = new LightblueExternalResource(TestMetadataJson.forEntity(TestUser.class));

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    LightblueClient client;

    BulkLightblueRequester requester;

    @Before
    public void initializeClient() {
        client = LightblueClients.withJavaTimeSerializationSupport(
                LightblueClientConfigurations.fromLightblueExternalResource(lightblueExternalResource));
        requester = new BulkLightblueRequester(client);
    }

    @Before
    public void dropTestUsers() throws UnknownHostException {
        lightblueExternalResource.cleanupMongoCollections(TestUser.ENTITY_NAME);
    }

    @Test
    public void shouldMakeRequestsAndProvideResponsesForSpecificRequests() throws LightblueException,
            ExecutionException, InterruptedException {
        insertUser("cooltester2000");
        insertUser("aw3som3cod3r");

        DataFindRequest findTester = new DataFindRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);
        findTester.where(Query.withValue("username", Query.BinOp.eq, "cooltester2000"));
        findTester.select(Projection.includeFieldRecursively("*"));

        DataFindRequest findCoder = new DataFindRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);
        findCoder.where(Query.withValue("username", Query.BinOp.eq, "aw3som3cod3r"));
        findCoder.select(Projection.includeFieldRecursively("*"));

        List<TestUser> returned = requester.request(findCoder, findTester).then((responses) -> {
            return Arrays.asList(
                    responses.forRequest(findTester).parseProcessed(TestUser.class),
                    responses.forRequest(findCoder).parseProcessed(TestUser.class));
        }).get();

        TestUser shouldBeTester = returned.get(0);
        TestUser shouldBeCoder = returned.get(1);

        assertEquals("cooltester2000", shouldBeTester.getUsername());
        assertEquals("aw3som3cod3r", shouldBeCoder.getUsername());
    }

    @Test
    public void shouldThrowNoSuchElementExceptionIfResponseNotFound() throws ExecutionException,
            InterruptedException, LightblueException {
        insertUser("cooltester2000");

        DataFindRequest findTester = new DataFindRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);
        findTester.where(Query.withValue("username", Query.BinOp.eq, "cooltester2000"));
        findTester.select(Projection.includeFieldRecursively("*"));

        DataFindRequest otherRequest = new DataFindRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);

        expectedException.expectCause(Matchers.instanceOf(NoSuchElementException.class));

        requester.request(findTester).then((responses) -> {
            return responses.forRequest(otherRequest).parseProcessed(TestUser.class);
        }).get();
    }

    @Test
    public void shouldCacheRequestsUntilFutureIsResolvedThenPerformAllSynchronouslyInOneBulkRequest()
            throws LightblueException, ExecutionException, InterruptedException {
        insertUser("cooltester2000");
        insertUser("aw3som3cod3r");

        List<String> requestsHandled = new ArrayList<>();

        DataFindRequest findTester = new DataFindRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);
        findTester.where(Query.withValue("username", Query.BinOp.eq, "cooltester2000"));
        findTester.select(Projection.includeFieldRecursively("*"));

        DataFindRequest findCoder = new DataFindRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);
        findCoder.where(Query.withValue("username", Query.BinOp.eq, "aw3som3cod3r"));
        findCoder.select(Projection.includeFieldRecursively("*"));

        Future<TestUser> futureTester = requester.request(findTester).then((responses -> {
            requestsHandled.add("tester");
            return responses.forRequest(findTester).parseProcessed(TestUser.class);
        }));

        Future<TestUser> futureCoder = requester.request(findCoder).then((responses -> {
            requestsHandled.add("coder");
            return responses.forRequest(findCoder).parseProcessed(TestUser.class);
        }));

        assertThat(requestsHandled).isEmpty();

        TestUser shouldBeCoder = futureCoder.get();

        assertThat(requestsHandled).containsExactly("tester", "coder");

        TestUser shouldBeTester = futureTester.get();

        assertThat(requestsHandled).containsExactly("tester", "coder");

        assertEquals("cooltester2000", shouldBeTester.getUsername());
        assertEquals("aw3som3cod3r", shouldBeCoder.getUsername());
    }

    // TODO: Test error scenarios once lightblue bulk response error handling is fixed
    // See: https://github.com/lightblue-platform/lightblue-client/issues/202

    private void insertUser(String username) throws LightblueException {
        DataInsertRequest insertRequest = new DataInsertRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);
        TestUser user = new TestUser();
        user.setUsername(username);
        insertRequest.create(user);
        client.data(insertRequest);
    }
}

package org.esbtools.eventhandler.lightblue;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.esbtools.eventhandler.lightblue.client.BulkLightblueRequester;
import org.esbtools.eventhandler.lightblue.client.BulkLightblueResponseException;
import org.esbtools.eventhandler.lightblue.testing.LightblueClientConfigurations;
import org.esbtools.eventhandler.lightblue.testing.LightblueClients;
import org.esbtools.eventhandler.lightblue.testing.TestMetadataJson;
import org.esbtools.eventhandler.lightblue.testing.TestUser;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.response.LightblueParseException;
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

        DataFindRequest findTester = findUserByUsername("cooltester2000");
        DataFindRequest findCoder = findUserByUsername("aw3som3cod3r");

        List<TestUser> returned = requester.request(findCoder, findTester).transformSync((responses) -> {
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
    public void shouldMakeRequestsAndProvideResponsesForSpecificRequestsByIndex()
            throws LightblueException, ExecutionException, InterruptedException {
        insertUser("cooltester2000");
        insertUser("aw3som3cod3r");

        DataFindRequest findCoder = findUserByUsername("aw3som3cod3r");
        DataFindRequest findTester = findUserByUsername("cooltester2000");

        List<TestUser> returned = requester.request(findCoder, findTester).transformSync((responses) -> {
            return Arrays.asList(
                    responses.forRequest(0).parseProcessed(TestUser.class),
                    responses.forRequest(1).parseProcessed(TestUser.class));
        }).get();

        TestUser shouldBeCoder = returned.get(0);
        TestUser shouldBeTester = returned.get(1);

        assertEquals("cooltester2000", shouldBeTester.getUsername());
        assertEquals("aw3som3cod3r", shouldBeCoder.getUsername());
    }

    @Test
    public void shouldThrowNoSuchElementExceptionIfResponseNotFound() throws ExecutionException,
            InterruptedException, LightblueException {
        insertUser("cooltester2000");

        DataFindRequest findTester = findUserByUsername("cooltester2000");
        DataFindRequest otherRequest = new DataFindRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);

        expectedException.expectCause(Matchers.instanceOf(NoSuchElementException.class));

        requester.request(findTester).transformSync((responses) -> {
            return responses.forRequest(otherRequest).parseProcessed(TestUser.class);
        }).get();
    }

    @Test
    public void shouldThrowIndexOutOfBoundsExceptionIfResponseNotFoundByIndex() throws ExecutionException,
            InterruptedException, LightblueException {
        insertUser("cooltester2000");

        DataFindRequest findTester = findUserByUsername("cooltester2000");

        expectedException.expectCause(Matchers.instanceOf(IndexOutOfBoundsException.class));

        requester.request(findTester).transformSync((responses) -> {
            return responses.forRequest(1).parseProcessed(TestUser.class);
        }).get();
    }

    @Test
    public void shouldCacheRequestsUntilFutureIsResolvedThenPerformAllSynchronouslyInOneBulkRequest()
            throws LightblueException, ExecutionException, InterruptedException {
        DataFindRequest findTester = findUserByUsername("cooltester2000");
        DataFindRequest findCoder = findUserByUsername("aw3som3cod3r");

        Future<TestUser> futureTester = requester.request(findTester).transformSync((responses -> {
            return responses.forRequest(findTester).parseProcessed(TestUser.class);
        }));

        Future<TestUser> futureCoder = requester.request(findCoder).transformSync((responses -> {
            return responses.forRequest(findCoder).parseProcessed(TestUser.class);
        }));

        insertUser("cooltester2000");
        insertUser("aw3som3cod3r");

        TestUser shouldBeCoder = futureCoder.get();

        // Insert another tester; if the request for tester is made on next .get() we will get
        // parse exception.
        insertUser("cooltester2000");

        try {
            TestUser shouldBeTester = futureTester.get();

            assertNotNull("No user found: request were run eagerly instead of lazily", shouldBeCoder);
            assertNotNull("No user found: request were run eagerly instead of lazily", shouldBeTester);
            assertEquals("cooltester2000", shouldBeTester.getUsername());
            assertEquals("aw3som3cod3r", shouldBeCoder.getUsername());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof LightblueParseException) {
                fail("Found multiple users: requests were not batched.");
            }

            throw e;
        }
    }

    @Test
    public void shouldCompleteFutureWithAllErrorsInFutureExceptionIfFailed() throws Exception {
        DataFindRequest badFindTester = new DataFindRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);
        badFindTester.where(Query.withValue("badField", Query.BinOp.eq, "cooltester2000"));
        badFindTester.select(Projection.includeFieldRecursively("*"));

        DataFindRequest badFindCoder = new DataFindRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);
        badFindCoder.where(Query.withValue("badField", Query.BinOp.eq, "aw3som3cod3r"));
        badFindCoder.select(Projection.includeFieldRecursively("*"));

        Future<String> future = requester.request(badFindCoder, badFindTester)
                .transformSync((responses -> "should not get here"));

        try {
            future.get();
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(BulkLightblueResponseException.class);

            BulkLightblueResponseException cause = (BulkLightblueResponseException) e.getCause();

            assertThat(cause.errors()).hasSize(2);
        }
    }

    @Test
    public void shouldOnlyFailFuturesWhichHaveFailedResponses() throws Exception {
        insertUser("cooltester2000");

        DataFindRequest findTester = findUserByUsername("cooltester2000");

        DataFindRequest badRequest = new DataFindRequest("badRequest");
        badRequest.select(Projection.includeFieldRecursively("*"));
        badRequest.where(Query.withValue("foo", Query.BinOp.eq, "bar"));

        Future<String> shouldFail = requester.request(badRequest).transformSync(responses -> "fail");
        Future<TestUser> shouldSucceed = requester.request(findTester).transformSync(responses -> {
            return responses.forRequest(findTester).parseProcessed(TestUser.class);
        });

        assertThat(shouldSucceed.get().getUsername()).isEqualTo("cooltester2000");

        expectedException.expect(ExecutionException.class);

        shouldFail.get();
    }

    @Test
    public void shouldAllowChainingMultipleRequestsAndPerformEachStagesRequestsInBulk()
            throws Exception {
        DataFindRequest findTester = findUserByUsername("cooltester2000");
        DataFindRequest findCoder = findUserByUsername("aw3som3cod3r");
        DataFindRequest findAnotherCoder = findUserByUsername("moreawesomecoder");

        List<String> log = new ArrayList<>();

        // Demonstrates both styles of chaining requests.
        // Note that if you have both requests up front, you should just do them both at the same
        // time.

        // This style demonstrates nesting callbacks. Notice each level of "depth" becomes
        // increasingly indented. But sometimes this is necessary.
        Future<?> futureTester = requester.request(findTester).transformAsync(responses -> {
            log.add("findTester");

            // Here we can define a new request in the transform callback.
            DataFindRequest findAnotherTester = findUserByUsername("muchcoolertester");

            return requester.request(findAnotherTester).transformSync(moreResponses -> {
                log.add("findAnotherTester");
                return moreResponses.forRequest(findAnotherTester);
            });
        });

        // This style is more typical of modern asynchronous programming because there is no
        // continuous nesting. However, scope doesn't allow you to define a request object within
        // the response transform, so the request objects have to be in scope, which is generally
        // not as intuitive or natural.
        Future<?> futureCoder = requester.request(findCoder).transformAsync(responses -> {
            log.add("findCoder");
            return requester.request(findAnotherCoder);
        }).transformSync(responses -> {
            log.add("findAnotherCoder");
            return responses.forRequest(findAnotherCoder);
        });

        futureTester.get();
        futureCoder.get();

        assertThat(log.subList(0,2))
                .containsExactly("findTester", "findCoder");
        assertThat(log.subList(2,4))
                .containsExactly("findAnotherTester", "findAnotherCoder");
    }

    private void insertUser(String username) throws LightblueException {
        DataInsertRequest insertRequest = new DataInsertRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);
        TestUser user = new TestUser();
        user.setUsername(username);
        insertRequest.create(user);
        client.data(insertRequest);
    }

    private static DataFindRequest findUserByUsername(String username) {
        DataFindRequest findUser = new DataFindRequest(TestUser.ENTITY_NAME, TestUser.ENTITY_VERSION);
        findUser.where(Query.withValue("username", Query.BinOp.eq, username));
        findUser.select(Projection.includeFieldRecursively("*"));
        return findUser;
    }
}

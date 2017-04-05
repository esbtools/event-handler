[![Build Status](https://travis-ci.org/esbtools/event-handler.svg?branch=master)](https://travis-ci.org/esbtools/event-handler.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/esbtools/event-handler/badge.svg?branch=master&service=github)](https://coveralls.io/github/esbtools/event-handler?branch=master)
[![Stories in Ready](https://badge.waffle.io/esbtools/event-handler.png?label=ready&title=Ready)](https://waffle.io/esbtools/event-handler)

# event-handler

Notification in, document out.

Specifically, this event handler is designed to handle events (in the form of _notifications_ and/or
_document events_) from one system and produce canonical document messages (per the EIP) to be
shared with any interested systems asynchronously. This library provides a pattern for doing this
kind of thing which standardizes some solutions for the types of concerns you may have with document
messages like throttling, priority, and stale data.

A document event is an event which corresponds with some canonical document message. Sometimes it is
useful to perform certain optimizations on these messages by processing them in batches, and merging
some together or removing duplicates. Generally you want to associate them with a priority.
Processing a document event means looking up the data for that entity, building a canonical message,
and sending it off to some endpoint to be shared. Generally, document events should be _pulled_ with
some throttle when their associated data is able to be shared. You don't want to push document
messages because if they are unable to be shared, that document's data may no longer be current by
the time it is actually distributed.

Notifications are minimal documents which tell about an insert or update which occurred on some
data with which you wish to share with external consumers. Based on what has changed and/or the
current state of related data, you may wish to produce one or more document events from a given
notification. This can be as simple as a one-to-one relationship: a user's email was updated, so
create a user document event (which would correspond to a user message). It may be as complex as an
order was placed, so create document events for the order, the user, and that user's company if they
have one, etc. In other words, processing a notification means applying some logic specific to your
business. Generally, these resulting document events should be _pushed_ to some document event
store.

It's worth noting that you don't necessarily have to use both event types, but we've found it
generally useful to have these two concepts if it makes sense. Push notifications, push document
events, but pull document messages.

## Sure. So how do I use it?

Current status: Beta

To consume this library, you will want to write (or reuse) an implementation of a standard set of
interfaces to get at notifications and/or document events. Additionally, you will need to write
implementations of notifications and/or document events which house your business logic.

## Modules

### lib

High level code and interfaces for outlining these patterns.

### lightblue

An implementation of an event handler which reads notifications from an cooperating instance of
[lightblue](https://lightblue.io), specifically one with entities configured to use the
[lightblue-notification-hook](https://github.com/esbtools/lightblue-notification-hook).

## Releasing

1. Make sure you can push to org.esbtools respositories. Open a ticket up with the [Sonatype 
Jira](https://issues.sonatype.org/secure/Dashboard.jspa) (you will need to create an 
account), with the OSSRH community project, choose "Task" type (not "New Project"), and simply 
ask for permissions to push to org.esbtools releases and snapshots. A currently permitted 
member may be asked to comment to confirm.
2. Make sure lightblue entity class versions are not SNAPSHOT-ed. (documentEvent and
eventHandlerConfig)
3. mvn release:prepare -P release
4. Set versions appropriately, and name the tag simply "V#.#.#" (instead of
event-handler-parent-#.#.#)
5. mvn release:perform -P release
6. Resnapshot lightblue entity class versions

For more information, see [the Maven release plugin documentation](
http://maven.apache.org/maven-release/maven-release-plugin/examples/prepare-release.html).

### Retrying a release
To re-release a previous tag:

1. Create a release.properties file in root project folder with contents like:
<pre><code>scm.url=scm:git:https://github.com/esbtools/event-handler.git
scm.tag=VERSION TAG HERE</code></pre>
2. From root project folder, run mvn release:perform -P release

# event-handler-lightblue

Event handler repository implementations using lightblue for persistence.

## Generating lightblue entity metadata

This library relies on a lightblue instance with up to three configured entities:

- documentEvent
- notification
- eventHandlerConfig

The source of truth for these schemas is the Java model objects. To automate
lightblue configuration, we generate metadata for these entities on build.

The metadata, by default, is outputted to target/classes.

You can change this by setting metadata.outputDirectory system property.

If metadata already exists in the output directory with the same name as 
these entities, existing metadata will be kept other than schema. In this way,
you can generate metadata, add your deployment specific datastore and access
configuration, and retain this even when the schemas inevitably change.

This metadata is generated on mvn install.

You can also generate it without compiling or running tests by running simply
`mvn exec:java`.

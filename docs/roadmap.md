# (Incubated) Stellio Roadmap

This product is an Incubated FIWARE Generic Enabler. If you would like to learn about the
overall Roadmap of FIWARE, please check section "Roadmap" on the FIWARE Catalogue.

## Introduction

This section elaborates on proposed new features or tasks which are expected to be added to the product in the
foreseeable future. There should be no assumption of a commitment to deliver these features on specific dates or in the
order given. The development team will be doing their best to follow the proposed dates and priorities, but please bear
in mind that plans to work on a given feature or task may be revised. All information is provided as a general
guidelines only, and this section may be revised to provide newer information at any time.

## Short term

The following list of features are planned to be addressed in the short term, and incorporated in the next release of
the product planned for end of March 2021:

- Implement multi-attributes support for GeoProperties [#101](https://github.com/stellio-hub/stellio-context-broker/issues/101)
- Finish implementation of some missing common cross-cutting behaviors as defined in the NGSI-LD specification [#11](https://github.com/stellio-hub/stellio-context-broker/issues/11), [#12](https://github.com/stellio-hub/stellio-context-broker/issues/12), [#52](https://github.com/stellio-hub/stellio-context-broker/issues/52), [#146](https://github.com/stellio-hub/stellio-context-broker/issues/146), [#206](https://github.com/stellio-hub/stellio-context-broker/issues/206), [#287](https://github.com/stellio-hub/stellio-context-broker/issues/287)
- Implement the discovery endpoints introduced in version 1.3.1 of the NGSI-LD specification [#268](https://github.com/stellio-hub/stellio-context-broker/issues/268)
- Implement support for the batch entities update endpoint [#62](https://github.com/stellio-hub/stellio-context-broker/issues/62)
- Fix the currently [identified issues](https://github.com/stellio-hub/stellio-context-broker/issues?q=is%3Aissue+is%3Aopen+label%3Afix)
- Implement support for the aggregated temporal representation of entities introduced in version 1.4.1 of the NGSI-LD specification
- Upgrade frameworks and libraries to their last released version (Spring Boot 2.4.x, Timescale 2.x, ...)

## Medium term

The following list of features are planned to be addressed in the medium term, typically within the subsequent
release(s) generated in the next **9 months** after next planned release:

- Implement full support for geospatial features (geo-queries on entities, support for all geometries, GeoJSON rendering, ...)
- Implement support for the all the supported data types (e.g. structured property value)
- Implement distributed capabilities (via support of Context Source as defined in the NGSI-LD specification)
- Experiment with an alternative Graph database (namely Janus Graph)
- Complete the requirements to become an approved full Generic Enabler
- Expose an API allowing the management of authorizations inside the information context

## Long term

The following list of features are proposals regarding the longer-term evolution of the product even though development
of these features has not yet been scheduled for a release in the near future. Please feel free to contact us if you
wish to get involved in the implementation or influence the roadmap.

- Full implementation of the NGSI-LD specification


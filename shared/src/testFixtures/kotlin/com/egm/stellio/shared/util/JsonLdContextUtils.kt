package com.egm.stellio.shared.util

const val EGM_TEST_BASE_CONTEXT_URL = "http://localhost:8093/jsonld-contexts"
const val NGSILD_TEST_CORE_CONTEXT = "$EGM_TEST_BASE_CONTEXT_URL/ngsi-ld-core-context-v1.8.jsonld"
val NGSILD_TEST_CORE_CONTEXTS = listOf(NGSILD_TEST_CORE_CONTEXT)

const val AQUAC_COMPOUND_CONTEXT = "$EGM_TEST_BASE_CONTEXT_URL/aquac-compound.jsonld"
val AQUAC_HEADER_LINK = buildContextLinkHeader(AQUAC_COMPOUND_CONTEXT)

const val APIC_CONTEXT = "$EGM_TEST_BASE_CONTEXT_URL/apic.jsonld"
const val APIC_COMPOUND_CONTEXT = "$EGM_TEST_BASE_CONTEXT_URL/apic-compound.jsonld"
val APIC_COMPOUND_CONTEXTS = listOf(APIC_COMPOUND_CONTEXT)
val APIC_HEADER_LINK = buildContextLinkHeader(APIC_COMPOUND_CONTEXT)

const val AUTHZ_TEST_CONTEXT = "$EGM_TEST_BASE_CONTEXT_URL/authorization.jsonld"
const val AUTHZ_TEST_COMPOUND_CONTEXT = "$EGM_TEST_BASE_CONTEXT_URL/authorization-compound.jsonld"
val AUTHZ_TEST_COMPOUND_CONTEXTS = listOf(AUTHZ_TEST_COMPOUND_CONTEXT)
val AUTHZ_HEADER_LINK = buildContextLinkHeader(AUTHZ_TEST_CONTEXT)

const val SENSOR_COMPACT_TYPE = "Sensor"
const val SENSOR_TYPE = "https://ontology.eglobalmark.com/egm#$SENSOR_COMPACT_TYPE"
const val DEVICE_COMPACT_TYPE = "Device"
const val DEVICE_TYPE = "https://ontology.eglobalmark.com/egm#$DEVICE_COMPACT_TYPE"
const val BEEHIVE_COMPACT_TYPE = "BeeHive"
const val BEEHIVE_TYPE = "https://ontology.eglobalmark.com/apic#$BEEHIVE_COMPACT_TYPE"
const val BEEKEEPER_COMPACT_TYPE = "Beekeeper"
const val BEEKEEPER_TYPE = "https://ontology.eglobalmark.com/apic#$BEEKEEPER_COMPACT_TYPE"
const val APIARY_COMPACT_TYPE = "Apiary"
const val APIARY_TYPE = "https://ontology.eglobalmark.com/apic#$APIARY_COMPACT_TYPE"
const val INCOMING_COMPACT_PROPERTY = "incoming"
const val INCOMING_PROPERTY = "https://ontology.eglobalmark.com/apic#$INCOMING_COMPACT_PROPERTY"
const val OUTGOING_COMPACT_PROPERTY = "outgoing"
const val OUTGOING_PROPERTY = "https://ontology.eglobalmark.com/apic#$OUTGOING_COMPACT_PROPERTY"
const val TEMPERATURE_COMPACT_PROPERTY = "temperature"
const val TEMPERATURE_PROPERTY = "https://ontology.eglobalmark.com/apic#$TEMPERATURE_COMPACT_PROPERTY"

const val MANAGED_BY_COMPACT_RELATIONSHIP = "managedBy"
const val MANAGED_BY_RELATIONSHIP = "https://ontology.eglobalmark.com/egm#$MANAGED_BY_COMPACT_RELATIONSHIP"

const val NGSILD_NAME_TERM = "name"
const val NGSILD_NAME_PROPERTY = "https://schema.org/name"

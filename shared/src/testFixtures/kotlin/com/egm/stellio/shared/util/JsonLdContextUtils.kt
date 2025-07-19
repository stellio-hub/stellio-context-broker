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

const val SENSOR_TERM = "Sensor"
const val SENSOR_IRI = "https://ontology.eglobalmark.com/egm#$SENSOR_TERM"
const val DEVICE_TERM = "Device"
const val DEVICE_IRI = "https://ontology.eglobalmark.com/egm#$DEVICE_TERM"
const val BEEHIVE_TERM = "BeeHive"
const val BEEHIVE_IRI = "https://ontology.eglobalmark.com/apic#$BEEHIVE_TERM"
const val BEEKEEPER_TERM = "Beekeeper"
const val BEEKEEPER_IRI = "https://ontology.eglobalmark.com/apic#$BEEKEEPER_TERM"
const val APIARY_TERM = "Apiary"
const val APIARY_IRI = "https://ontology.eglobalmark.com/apic#$APIARY_TERM"
const val LINKED_ENTITY_TERM = "LinkedEntity"

const val INCOMING_TERM = "incoming"
const val INCOMING_IRI = "https://ontology.eglobalmark.com/apic#$INCOMING_TERM"
const val OUTGOING_TERM = "outgoing"
const val OUTGOING_IRI = "https://ontology.eglobalmark.com/apic#$OUTGOING_TERM"
const val TEMPERATURE_TERM = "temperature"
const val TEMPERATURE_IRI = "https://ontology.eglobalmark.com/apic#$TEMPERATURE_TERM"
const val NAME_TERM = "name"
const val NAME_IRI = "https://schema.org/name"
const val LUMINOSITY_TERM = "luminosity"
const val LUMINOSITY_IRI = "https://ontology.eglobalmark.com/apic#$LUMINOSITY_TERM"
const val FRIENDLYNAME_TERM = "friendlyName"
const val FRIENDLYNAME_IRI = "https://ontology.eglobalmark.com/apic#$FRIENDLYNAME_TERM"
const val CATEGORY_TERM = "category"
const val CATEGORY_IRI = "https://ontology.eglobalmark.com/apic#$CATEGORY_TERM"
const val MANAGED_BY_TERM = "managedBy"
const val MANAGED_BY_IRI = "https://ontology.eglobalmark.com/egm#$MANAGED_BY_TERM"

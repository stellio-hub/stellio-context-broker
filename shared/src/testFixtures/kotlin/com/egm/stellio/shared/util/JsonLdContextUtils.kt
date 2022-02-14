package com.egm.stellio.shared.util

val DEFAULT_CONTEXTS = listOf(
    "https://fiware.github.io/data-models/context.jsonld",
    JsonLdUtils.NGSILD_CORE_CONTEXT
)
val AQUAC_COMPOUND_CONTEXT = "${JsonLdUtils.EGM_BASE_CONTEXT_URL}/aquac/jsonld-contexts/aquac-compound.jsonld"
val APIC_COMPOUND_CONTEXT = "${JsonLdUtils.EGM_BASE_CONTEXT_URL}/apic/jsonld-contexts/apic-compound.jsonld"

const val BEEHIVE_TYPE = "https://ontology.eglobalmark.com/apic#BeeHive"
const val INCOMING_PROPERTY = "https://ontology.eglobalmark.com/apic#incoming"
const val OUTGOING_PROPERTY = "https://ontology.eglobalmark.com/apic#outgoing"
const val LUMINOSITY_PROPERTY = "https://ontology.eglobalmark.com/apic#luminosity"
const val TEMPERATURE_PROPERTY = "https://ontology.eglobalmark.com/apic#temperature"
const val NAME_PROPERTY = "https://uri.etsi.org/ngsi-ld/name"
const val CREATED_BY_RELATIONSHIP = "https://ontology.eglobalmark.com/egm#createdBy"
const val MANAGED_BY_RELATIONSHIP = "https://ontology.eglobalmark.com/egm#managedBy"

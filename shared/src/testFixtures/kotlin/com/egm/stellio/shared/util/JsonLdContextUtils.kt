package com.egm.stellio.shared.util

val DEFAULT_CONTEXTS = listOf(
    "https://fiware.github.io/data-models/context.jsonld",
    JsonLdUtils.NGSILD_CORE_CONTEXT
)
const val AQUAC_COMPOUND_CONTEXT = "${JsonLdUtils.EGM_BASE_CONTEXT_URL}/aquac/jsonld-contexts/aquac-compound.jsonld"
const val APIC_COMPOUND_CONTEXT = "${JsonLdUtils.EGM_BASE_CONTEXT_URL}/apic/jsonld-contexts/apic-compound.jsonld"

const val BEEHIVE_COMPACT_TYPE = "BeeHive"
const val BEEHIVE_TYPE = "https://ontology.eglobalmark.com/apic#$BEEHIVE_COMPACT_TYPE"
const val APIARY_COMPACT_TYPE = "Apiary"
const val APIARY_TYPE = "https://ontology.eglobalmark.com/apic#Apiary"
const val INCOMING_COMPACT_PROPERTY = "incoming"
const val INCOMING_PROPERTY = "https://ontology.eglobalmark.com/apic#$INCOMING_COMPACT_PROPERTY"
const val OUTGOING_COMPACT_PROPERTY = "outgoing"
const val OUTGOING_PROPERTY = "https://ontology.eglobalmark.com/apic#$OUTGOING_COMPACT_PROPERTY"
const val LUMINOSITY_PROPERTY = "https://ontology.eglobalmark.com/apic#luminosity"
const val TEMPERATURE_COMPACT_PROPERTY = "temperature"
const val TEMPERATURE_PROPERTY = "https://ontology.eglobalmark.com/apic#$TEMPERATURE_COMPACT_PROPERTY"
const val NAME_PROPERTY = "https://schema.org/name"
const val CREATED_BY_RELATIONSHIP = "https://ontology.eglobalmark.com/egm#createdBy"
const val MANAGED_BY_RELATIONSHIP = "https://ontology.eglobalmark.com/egm#managedBy"

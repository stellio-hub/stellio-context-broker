package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.toUri

// JSON-LD terms and keywords (https://www.w3.org/TR/json-ld11/#keywords)

const val JSONLD_ID_KW = "@id"
const val JSONLD_TYPE_KW = "@type"
const val JSONLD_VALUE_KW = "@value"
const val JSONLD_LIST_KW = "@list"
const val JSONLD_LANGUAGE_KW = "@language"
const val JSONLD_VOCAB_KW = "@vocab"
const val JSONLD_JSON_KW = "@json"
const val JSONLD_CONTEXT_KW = "@context"
const val JSONLD_NONE_KW = "@none"

// Default Core context assignments

const val NGSILD_PREFIX = "https://uri.etsi.org/ngsi-ld/"
const val NGSILD_DEFAULT_VOCAB = "https://uri.etsi.org/ngsi-ld/default-context/"

// Attributes and expanded terms holding the value

const val NGSILD_PROPERTY_TERM = "Property"
const val NGSILD_PROPERTY_VALUE_TERM = "value"
const val NGSILD_PROPERTY_VALUE = "${NGSILD_PREFIX}hasValue"
const val NGSILD_GEOPROPERTY_TERM = "GeoProperty"
const val NGSILD_GEOPROPERTY_VALUE_TERM = "value"
const val NGSILD_GEOPROPERTY_VALUE = "${NGSILD_PREFIX}hasValue"
const val NGSILD_RELATIONSHIP_TERM = "Relationship"
const val NGSILD_RELATIONSHIP_OBJECT_TERM = "object"
const val NGSILD_RELATIONSHIP_OBJECT = "${NGSILD_PREFIX}hasObject"
const val NGSILD_JSONPROPERTY_TERM = "JsonProperty"
const val NGSILD_JSONPROPERTY_JSON_TERM = "json"
const val NGSILD_JSONPROPERTY_JSON = "${NGSILD_PREFIX}hasJSON"
const val NGSILD_LANGUAGEPROPERTY_TERM = "LanguageProperty"
const val NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP_TERM = "languageMap"
const val NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP = "${NGSILD_PREFIX}hasLanguageMap"
const val NGSILD_VOCABPROPERTY_TERM = "VocabProperty"
const val NGSILD_VOCABPROPERTY_VOCAB_TERM = "vocab"
const val NGSILD_VOCABPROPERTY_VOCAB = "${NGSILD_PREFIX}hasVocab"

// Attributes and expanded terms holding the previous value of an attribute

const val NGSILD_PROPERTY_PREVIOUS_VALUE_TERM = "previousValue"
const val NGSILD_GEOPROPERTY_PREVIOUS_VALUE_TERM = "previousValue"
const val NGSILD_RELATIONSHIP_PREVIOUS_OBJECT_TERM = "previousObject"
const val NGSILD_JSONPROPERTY_PREVIOUS_JSON_TERM = "previousJson"
const val NGSILD_LANGUAGEPROPERTY_PREVIOUS_LANGUAGEMAP_TERM = "previousLanguageMap"
const val NGSILD_VOCABPROPERTY_PREVIOUS_VOCAB_TERM = "previousVocab"

enum class AttributesValuesMapping(
    val attributeNameTerm: String,
    val previousValueTerm: String,
    val valueTerm: String
) {
    PROPERTY(NGSILD_PROPERTY_TERM, NGSILD_PROPERTY_PREVIOUS_VALUE_TERM, NGSILD_PROPERTY_VALUE_TERM),
    GEOPROPERTY(NGSILD_GEOPROPERTY_TERM, NGSILD_GEOPROPERTY_PREVIOUS_VALUE_TERM, NGSILD_GEOPROPERTY_VALUE_TERM),
    RELATIONSHIP(NGSILD_RELATIONSHIP_TERM, NGSILD_RELATIONSHIP_PREVIOUS_OBJECT_TERM, NGSILD_RELATIONSHIP_OBJECT_TERM),
    JSONPROPERTY(NGSILD_JSONPROPERTY_TERM, NGSILD_JSONPROPERTY_PREVIOUS_JSON_TERM, NGSILD_JSONPROPERTY_JSON_TERM),
    LANGUAGEPROPERTY(
        NGSILD_LANGUAGEPROPERTY_TERM,
        NGSILD_LANGUAGEPROPERTY_PREVIOUS_LANGUAGEMAP_TERM,
        NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP_TERM
    ),
    VOCABPROPERTY(NGSILD_VOCABPROPERTY_TERM, NGSILD_VOCABPROPERTY_PREVIOUS_VOCAB_TERM, NGSILD_VOCABPROPERTY_VOCAB_TERM);

    companion object {
        fun fromAttributeNameTerm(attributeNameTerm: String): AttributesValuesMapping =
            entries.find { it.attributeNameTerm == attributeNameTerm }!!
    }
}

// Expanded terms holding the temporal representations

const val NGSILD_PROPERTY_VALUES = "${NGSILD_PREFIX}hasValues"
const val NGSILD_GEOPROPERTY_VALUES = "${NGSILD_PREFIX}hasValues"
const val NGSILD_RELATIONSHIP_OBJECTS = "${NGSILD_PREFIX}hasObjects"
const val NGSILD_JSONPROPERTY_JSONS = "${NGSILD_PREFIX}jsons"
const val NGSILD_LANGUAGEPROPERTY_LANGUAGEMAPS = "${NGSILD_PREFIX}hasLanguageMaps"
const val NGSILD_VOCABPROPERTY_VOCABS = "${NGSILD_PREFIX}hasVocabs"

// Core terms and IRIs

const val NGSILD_ID_TERM = "id"
const val NGSILD_TYPE_TERM = "type"
const val NGSILD_VALUE_TERM = "value"
const val NGSILD_OBJECT_TERM = "object"
const val NGSILD_JSON_TERM = "json"
const val NGSILD_LANGUAGEMAP_TERM = "languageMap"
const val NGSILD_VOCAB_TERM = "vocab"
const val NGSILD_SCOPE_TERM = "scope"
const val NGSILD_SCOPE_IRI = "${NGSILD_PREFIX}$NGSILD_SCOPE_TERM"
const val NGSILD_LANG_TERM = "lang"
const val NGSILD_DATASET_TERM = "dataset"
const val NGSILD_ENTITY_TERM = "entity"
const val NGSILD_TITLE_TERM = "title"
const val NGSILD_UNIT_CODE_TERM = "unitCode"
const val NGSILD_UNIT_CODE_IRI = "${NGSILD_PREFIX}$NGSILD_UNIT_CODE_TERM"
const val NGSILD_LOCATION_TERM = "location"
const val NGSILD_LOCATION_IRI = "${NGSILD_PREFIX}$NGSILD_LOCATION_TERM"
const val NGSILD_OBSERVATION_SPACE_TERM = "observationSpace"
const val NGSILD_OBSERVATION_SPACE_IRI = "${NGSILD_PREFIX}$NGSILD_OBSERVATION_SPACE_TERM"
const val NGSILD_OPERATION_SPACE_TERM = "operationSpace"
const val NGSILD_OPERATION_SPACE_IRI = "${NGSILD_PREFIX}$NGSILD_OPERATION_SPACE_TERM"
const val NGSILD_DATASET_ID_TERM = "datasetId"
const val NGSILD_DATASET_ID_IRI = "${NGSILD_PREFIX}$NGSILD_DATASET_ID_TERM"
const val NGSILD_INSTANCE_ID_IRI = "${NGSILD_PREFIX}instanceId"
const val NGSILD_CREATED_AT_TERM = "createdAt"
const val NGSILD_CREATED_AT_IRI = "${NGSILD_PREFIX}$NGSILD_CREATED_AT_TERM"
const val NGSILD_MODIFIED_AT_TERM = "modifiedAt"
const val NGSILD_MODIFIED_AT_IRI = "${NGSILD_PREFIX}$NGSILD_MODIFIED_AT_TERM"
const val NGSILD_DELETED_AT_TERM = "deletedAt"
const val NGSILD_DELETED_AT_IRI = "${NGSILD_PREFIX}$NGSILD_DELETED_AT_TERM"
const val NGSILD_OBSERVED_AT_TERM = "observedAt"
const val NGSILD_OBSERVED_AT_IRI = "${NGSILD_PREFIX}$NGSILD_OBSERVED_AT_TERM"

// Data types (5.2)

const val NGSILD_SUBSCRIPTION_TERM = "Subscription"
const val NGSILD_NOTIFICATION_TERM = "Notification"
const val NGSILD_CSR_TERM = "ContextSourceRegistration"

// Data types for values (4.6.3)

const val NGSILD_DATE_TIME_TYPE = "${NGSILD_PREFIX}DateTime"
const val NGSILD_DATE_TYPE = "${NGSILD_PREFIX}Date"
const val NGSILD_TIME_TYPE = "${NGSILD_PREFIX}Time"

// Special value for NGSI-LD Null (4.5.0)

const val NGSILD_NULL = "urn:ngsi-ld:null"

// Stellio-specific way to identify a local broker (compared to a Context Source)

val NGSILD_LOCAL = "urn:ngsi-ld:local".toUri()

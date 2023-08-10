package com.egm.stellio.search.model

/**
 * Used to manage the different types of updates on existing attributes depending on the current operation.
 */
enum class OperationType {
    UPDATE_ATTRIBUTES,
    APPEND_ATTRIBUTES,
    APPEND_ATTRIBUTES_OVERWRITE_ALLOWED,
    PARTIAL_ATTRIBUTE_UPDATE,
    DELETE_ATTRIBUTE,
    MERGE_ENTITY,
    MERGE_ENTITY_OVERWRITE_ALLOWED,
    REPLACE_ATTRIBUTE
}

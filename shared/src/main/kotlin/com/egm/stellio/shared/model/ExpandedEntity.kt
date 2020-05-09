package com.egm.stellio.shared.model

class ExpandedEntity(
    val attributes: Map<String, Any>,
    val contexts: List<String>
) {
    fun getId(): String = attributes.getOrElse("@id") { "" } as String

    fun getType(): String = (attributes["@type"]!! as List<String>)[0]
}

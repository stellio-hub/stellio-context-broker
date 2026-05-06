package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.DataTypes

/**
 * KeyValuePair type as defined in 5.2.22 of the NGSI-LD specification
 */
data class KeyValuePair(
    val key: String,
    val value: String
) {
    companion object {
        fun deserialize(input: String?): List<KeyValuePair>? {
            return if (input != null && input != "null")
                DataTypes.convertToList(input)
            else null
        }
    }
}

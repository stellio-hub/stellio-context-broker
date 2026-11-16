package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.DataTypes

/**
 * KeyValuePair type as defined in TS 104 175 §5.2.6.6.4
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

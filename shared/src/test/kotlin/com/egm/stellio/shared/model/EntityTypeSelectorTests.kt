package com.egm.stellio.shared.model

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class EntityTypeSelectorTests {

    @ParameterizedTest
    @CsvSource(
        "'type1', 'type1;type2', false",
        "'type1', 'type1,type2', true",
        "'type1', 'type1|type2', true",
        "'type1,type2', 'type1;type2', true",
        "'type1,type2', 'type1,type2', true",
        "'type1,type2', 'type1,(type2;type3)', true",
        "'type1', 'type1,(type2;type3)', true",
        "'type2', 'type1,(type2;type3)', false",
        "'type2', 'type1,(type2;type3)', false",
        "'type4,type5,type2', 'type1,(type2;(type3|(type4;type5)))', true",
        "'type3,type2', 'type1,(type2;(type3|(type4;type5)))', true",
        "'type1', 'type1,(type2;(type3|(type4;type5)))', true",
        "'type2,type5', 'type1,(type2;(type3|(type4;type5)))', false",
        "'type4,type5', 'type1,(type2;(type3|(type4;type5)))', false",
    )
    fun `it should correctly find if a list of types matches an Entity Type Selection`(
        typeString: String,
        entityTypeSelection: EntityTypeSelection,
        result: String
    ) = runTest {
        val types = typeString.split(',')
        assertEquals(result.toBoolean(), areTypesInSelection(types, entityTypeSelection))
    }
}

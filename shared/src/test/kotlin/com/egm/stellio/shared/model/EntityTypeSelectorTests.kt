package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.time.ZonedDateTime

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
    fun `it should process entityTypesSelection correctly`(typeString: String, entityTypeSelection: EntityTypeSelection, result: String) = runTest {
        val types = typeString.split(',')
        assertEquals(result.toBoolean(), areTypesInSelection(types,entityTypeSelection))
    }

}

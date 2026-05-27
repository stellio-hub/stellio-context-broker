package com.egm.stellio.shared.queryparameter

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceedWith
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test

class QParserTests {

    @Test
    fun `it should parse a simple attribute existence check`() {
        parseQQuery("temperature").shouldSucceedWith { node ->
            assertInstanceOf(ExistsNode::class.java, node)
            assertEquals("temperature", (node as ExistsNode).rawPath)
        }
    }

    @Test
    fun `it should parse a negated existence check`() {
        parseQQuery("!temperature").shouldSucceedWith { node ->
            assertInstanceOf(NotExistsNode::class.java, node)
            assertEquals("temperature", (node as NotExistsNode).rawPath)
        }
    }

    @Test
    fun `it should parse a simple equality comparison`() {
        parseQQuery("temperature==42").shouldSucceedWith { node ->
            assertInstanceOf(ComparisonNode::class.java, node)
            node as ComparisonNode
            assertEquals("temperature", node.rawPath)
            assertEquals(ComparisonOperator.EQ, node.operator)
            assertInstanceOf(SingleValue::class.java, node.value)
            val sv = node.value as SingleValue
            assertEquals("42", sv.raw)
            assertEquals(ValueType.NUMBER, sv.type)
        }
    }

    @Test
    fun `it should parse an AND expression`() {
        parseQQuery("temperature==42;humidity>10").shouldSucceedWith { node ->
            assertInstanceOf(AndNode::class.java, node)
            node as AndNode
            assertInstanceOf(ComparisonNode::class.java, node.left)
            assertInstanceOf(ComparisonNode::class.java, node.right)
        }
    }

    @Test
    fun `it should parse an OR expression`() {
        parseQQuery("temperature==42|humidity>10").shouldSucceedWith { node ->
            assertInstanceOf(OrNode::class.java, node)
            node as OrNode
            assertInstanceOf(ComparisonNode::class.java, node.left)
            assertInstanceOf(ComparisonNode::class.java, node.right)
        }
    }

    @Test
    fun `it should parse a grouped expression in parentheses`() {
        parseQQuery("(temperature==42|humidity>10);status==\"active\"").shouldSucceedWith { node ->
            assertInstanceOf(AndNode::class.java, node)
            node as AndNode
            assertInstanceOf(OrNode::class.java, node.left)
        }
    }

    @Test
    fun `it should parse NEQ operator`() {
        parseQQuery("temperature!=42").shouldSucceedWith { node ->
            node as ComparisonNode
            assertEquals(ComparisonOperator.NEQ, node.operator)
        }
    }

    @Test
    fun `it should parse GTE operator`() {
        parseQQuery("temperature>=42").shouldSucceedWith { node ->
            node as ComparisonNode
            assertEquals(ComparisonOperator.GTE, node.operator)
        }
    }

    @Test
    fun `it should parse GT operator`() {
        parseQQuery("temperature>42").shouldSucceedWith { node ->
            node as ComparisonNode
            assertEquals(ComparisonOperator.GT, node.operator)
        }
    }

    @Test
    fun `it should parse LTE operator`() {
        parseQQuery("temperature<=42").shouldSucceedWith { node ->
            node as ComparisonNode
            assertEquals(ComparisonOperator.LTE, node.operator)
        }
    }

    @Test
    fun `it should parse LT operator`() {
        parseQQuery("temperature<42").shouldSucceedWith { node ->
            node as ComparisonNode
            assertEquals(ComparisonOperator.LT, node.operator)
        }
    }

    @Test
    fun `it should parse LIKE_REGEX operator`() {
        parseQQuery("name~=\"foo.*\"").shouldSucceedWith { node ->
            node as ComparisonNode
            assertEquals(ComparisonOperator.LIKE_REGEX, node.operator)
        }
    }

    @Test
    fun `it should parse NOT_LIKE_REGEX operator`() {
        parseQQuery("name!~=\"foo.*\"").shouldSucceedWith { node ->
            node as ComparisonNode
            assertEquals(ComparisonOperator.NOT_LIKE_REGEX, node.operator)
        }
    }

    @Test
    fun `it should parse a range value`() {
        parseQQuery("temperature==10..20").shouldSucceedWith { node ->
            node as ComparisonNode
            assertInstanceOf(RangeValue::class.java, node.value)
            val rv = node.value as RangeValue
            assertEquals("10", rv.low.raw)
            assertEquals("20", rv.high.raw)
        }
    }

    @Test
    fun `it should parse a list value`() {
        parseQQuery("temperature==10,20,30").shouldSucceedWith { node ->
            node as ComparisonNode
            assertInstanceOf(ListValue::class.java, node.value)
            val lv = node.value as ListValue
            assertEquals(3, lv.items.size)
            assertEquals("10", lv.items[0].raw)
            assertEquals("20", lv.items[1].raw)
            assertEquals("30", lv.items[2].raw)
        }
    }

    @Test
    fun `it should parse a string value in double quotes`() {
        parseQQuery("name==\"BeeHive\"").shouldSucceedWith { node ->
            node as ComparisonNode
            val sv = node.value as SingleValue
            assertEquals("\"BeeHive\"", sv.raw)
            assertEquals(ValueType.STRING, sv.type)
        }
    }

    @Test
    fun `it should detect boolean value type`() {
        parseQQuery("active==true").shouldSucceedWith { node ->
            node as ComparisonNode
            val sv = node.value as SingleValue
            assertEquals(ValueType.BOOLEAN, sv.type)
        }
    }

    @Test
    fun `it should detect datetime value type`() {
        parseQQuery("observedAt==\"2023-01-01T00:00:00Z\"").shouldSucceedWith { node ->
            node as ComparisonNode
            val sv = node.value as SingleValue
            assertEquals(ValueType.DATETIME, sv.type)
        }
    }

    @Test
    fun `it should parse attribute path with dot notation`() {
        parseQQuery("incoming.observedAt>\"2023-01-01T00:00:00Z\"").shouldSucceedWith { node ->
            node as ComparisonNode
            assertEquals("incoming.observedAt", node.rawPath)
        }
    }

    @Test
    fun `it should parse attribute path with bracket notation`() {
        parseQQuery("incoming[temperature]==42").shouldSucceedWith { node ->
            node as ComparisonNode
            assertEquals("incoming[temperature]", node.rawPath)
        }
    }

    @Test
    fun `it should return BadRequest for empty query`() {
        parseQQuery("").shouldFail { ex ->
            assertInstanceOf(BadRequestDataException::class.java, ex)
        }
    }

    @Test
    fun `it should return BadRequest for unbalanced parentheses`() {
        parseQQuery("(temperature==42").shouldFail { ex ->
            assertInstanceOf(BadRequestDataException::class.java, ex)
        }
    }

    @Test
    fun `it should return BadRequest for trailing semicolon`() {
        parseQQuery("temperature==42;").shouldFail { ex ->
            assertInstanceOf(BadRequestDataException::class.java, ex)
        }
    }

    @Test
    fun `it should return BadRequest for unclosed string value`() {
        parseQQuery("temperature==\"unclosed").shouldFail { ex ->
            assertInstanceOf(BadRequestDataException::class.java, ex)
        }
    }

    @Test
    fun `it should parse complex nested expression`() {
        parseQQuery("(temperature>10;humidity<90)|status==\"active\"").shouldSucceedWith { node ->
            assertInstanceOf(OrNode::class.java, node)
            node as OrNode
            assertInstanceOf(AndNode::class.java, node.left)
        }
    }
}

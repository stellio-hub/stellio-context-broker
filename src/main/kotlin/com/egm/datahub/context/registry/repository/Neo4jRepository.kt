package com.egm.datahub.context.registry.repository

import org.springframework.stereotype.Component
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

@Component
class Neo4jRepository(
        private val neo4jconnection: Connection
) {

    fun insertJsonLd(jsonld: String) : String{
        val insert = "CALL semantics.importRDFSnippet({1},'JSON-LD',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})"
        val stmt : PreparedStatement = neo4jconnection.prepareStatement(insert)
        stmt.use {
            it.setString(1,jsonld)
            val rs : ResultSet = it.executeQuery()
            val rsmd = rs.metaData
            val columnsNumber = rsmd.getColumnCount()
            while (rs.next()) {
                for (i in 1..columnsNumber) {
                    if (i > 1) print(",  ")
                    val columnValue = rs.getString(i)
                    print(columnValue + " " + rsmd.getColumnName(i))
                }
                println("")
            }
        }
        return "OK"
    }

    fun getEntitiesByLabel(label: String) : MutableList<Map<String, Any>>{
        //val insert = "MATCH (a)-[b]-(c: {1} )  RETURN *"
        val insert = "MATCH (n:"+label+") RETURN n"
        val stmt : PreparedStatement = neo4jconnection.prepareStatement(insert)
        val list : MutableList<Map<String, Any>> = mutableListOf<Map<String, Any>>()
        stmt.use {
            val rs : ResultSet = it.executeQuery()
            val rsmd = rs.metaData

            while (rs.next()) {
                val numColumns = rsmd.columnCount
                val obj = HashMap<String, Any>()
                for (i in 1..numColumns) {
                    if (i > 1) print(",  ")
                    val column_name = rsmd.getColumnName(i)
                    val column_value = rs.getObject(column_name)
                    obj.put(column_name, column_value)
                    list.add(obj)
                    print(column_name + " : " + column_value)
                }
                println("")
            }
        }
        return list
    }

}

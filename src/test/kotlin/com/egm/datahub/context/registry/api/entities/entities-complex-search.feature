Feature: test some more complex queries on a measure entity

Background:
  * url urlBase

Scenario: search an entity by a property and a relationship
  * def creationResult = call read('fixture-complex-entity.feature')

Given path 'entities'
And param type = 'sosa__Observation'
And param q = ['ngsild__observedBy==urn:sosa:Sensor:10e2073a01080065', 'unitCode==CEL']
When method get
Then status 200
Then match response.size() == 1
Then match response[0].id == 'urn:sosa:Observation:111122223333'

Given path 'entities'
And param type = 'sosa__Observation'
And param q = ['ngsild__observedBy==urn:sosa:Sensor:10e2073a01080065', 'unitCode==Db']
When method get
Then status 200
Then match response.size() == 0

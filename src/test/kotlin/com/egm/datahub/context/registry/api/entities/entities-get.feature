Feature: test the correct creation of a beekeeper entity

Background:
  * url urlBase

Scenario: get a beehive by urn
  * def creationResult = call read('fixture-beekeeper.feature')

Given path 'entities/' + creationResult.entityUrn
When method get
Then status 200
Then match response == read('classpath:ngsild/expectations/get_beekeeper_expectation.json')

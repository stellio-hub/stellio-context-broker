Feature: test the correct creation of a beehive entity

Background:
  * url urlBase

Scenario: get a beehive by urn
  * def creationResult = call read('fixture-beehive.feature')

  Given path 'entities/' + creationResult.beehiveUrn
  When method get
  Then status 200
  Then match response == read('classpath:ngsild/expectations/get_beehive_expectation.json')

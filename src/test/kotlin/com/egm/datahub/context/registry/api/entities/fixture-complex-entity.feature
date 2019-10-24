@ignore
Feature: create a "complex" entity (ignore bc only expected to be set as a call arg)

  Background:
    * url urlBase

  Scenario: create a beekeeper and then check response status and header
    * def measure = read('classpath:ngsild/observation.json')

    Given path 'entities'
    And request measure
    When method post
    Then status 201
    And match header Location == '/ngsi-ld/v1/entities/urn:sosa:Observation:111122223333'
    And def entityUrn = 'urn:ssn:Measure:111122223333'

@ignore
Feature: create a "complex" entity (ignore bc only expected to be set as a call arg)

  Background:
    * url urlBase

  Scenario: create a beekeeper and then check response status and header
    * def vehicle1 = read('classpath:ngsild/vehicle_ngsild.json')
    * def vehicle2 = read('classpath:ngsild/vehicle_ngsild_2.json')

    Given path 'entities'
    And request vehicle1
    And header Link = '<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
    When method post
    Then status 201
    And match header Location == '/ngsi-ld/v1/entities/urn:example:Vehicle:A4567'
    And def entityUrn = 'urn:example:Vehicle:A4567'

    Given path 'entities'
    And request vehicle2
    And header Link = '<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
    When method post
    Then status 201
    And match header Location == '/ngsi-ld/v1/entities/urn:example:Vehicle:A1234'
    And def entityUrn = 'urn:example:Vehicle:A1234'

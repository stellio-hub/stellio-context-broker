Feature: test some more complex queries on a measure entity

  Background:
    * url urlBase

  Scenario: search an entity by a property and a relationship
    * def creationResult = call read('fixture-complex-entity.feature')

    Given path 'entities'
    And param type = 'Vehicle'
    And header Link = '<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
    When method get
    Then status 200
    Then match response.size() == 2

    Given path 'entities'
    And param type = 'Vehicle'
    And param q = 'isParked==urn:example:OffStreetParking:Downtown1'
    And header Link = '<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
    When method get
    Then status 200
    Then match response.size() == 1
    Then match response[0].id == 'urn:example:Vehicle:A4567'

    Given path 'entities'
    And param type = 'Vehicle'
    And param q = ['isParked==urn:example:OffStreetParking:Downtown1', 'brandName==Mercedes']
    And header Link = '<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
    When method get
    Then status 200
    Then match response.size() == 1
    Then match response[0].id == 'urn:example:Vehicle:A4567'

    Given path 'entities'
    And param type = 'Vehicle'
    And param q = ['isParked==urn:example:OffStreetParking:Downtown1', 'brandName==Volvo']
    And header Link = '<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
    When method get
    Then status 200
    Then match response.size() == 0

    Given path 'entities'
    And param type = 'Vehicle'
    And param q = ['name==name of vehicle 1', 'brandName==Mercedes']
    And header Link = '<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
    When method get
    Then status 200
    Then match response.size() == 1

    Given path 'entities'
    And param type = 'Vehicle'
    And param q = 'brandName==Mercedes'
    And header Link = '<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
    When method get
    Then status 200
    Then match response.size() == 2

    Given path 'entities'
    And param type = 'Vehicle'
    And param q = ['isParked==urn:example:OffStreetParking:Downtown2', 'hasSensor==urn:sosa:Sensor:1234567890']
    And header Link = '<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
    When method get
    Then status 200
    Then match response.size() == 1

    Given path 'entities'
    And param type = 'Vehicle'
    And param q = ['isParked==urn:example:OffStreetParking:Downtown2', 'hasSensor==urn:sosa:Sensor:1234567890', 'name==name of vehicle 2', 'brandName==Mercedes']
    And header Link = '<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
    When method get
    Then status 200
    Then match response.size() == 1

    Given path 'entities'
    When method get
    Then status 400

Feature: test some more complex queries on a measure entity

  Background:
    * url urlBase

  Scenario: search an entity by a property and a relationship
    * def creationResult = call read('fixture-complex-entity.feature')

    Given path 'entities'
    And param type = 'example__Vehicle'
    When method get
    Then status 200
    Then match response.size() == 2
    Then match response[0].id == 'urn:example:Vehicle:A4567'

    Given path 'entities'
    And param type = 'example__Vehicle'
    And param q = 'example__isParked==urn:example:OffStreetParking:Downtown1'
    When method get
    Then status 200
    Then match response.size() == 1
    Then match response[0].id == 'urn:example:Vehicle:A4567'

    Given path 'entities'
    And param type = 'example__Vehicle'
    And param q = ['example__isParked==urn:example:OffStreetParking:Downtown1', 'brandName==Mercedes']
    When method get
    Then status 200
    Then match response.size() == 1
    Then match response[0].id == 'urn:example:Vehicle:A4567'

    Given path 'entities'
    And param type = 'example__Vehicle'
    And param q = ['example__isParked==urn:example:OffStreetParking:Downtown1', 'brandName==Volvo']
    When method get
    Then status 200
    Then match response.size() == 0

    Given path 'entities'
    And param type = 'example__Vehicle'
    And param q = ['name==name of vehicle 1', 'brandName==Mercedes']
    When method get
    Then status 200
    Then match response.size() == 1

    Given path 'entities'
    And param type = 'example__Vehicle'
    And param q = 'brandName==Mercedes'
    When method get
    Then status 200
    Then match response.size() == 2

    Given path 'entities'
    And param type = 'example__Vehicle'
    And param q = ['example__isParked==urn:example:OffStreetParking:Downtown2', 'example__hasSensor==urn:sosa:Sensor:1234567890']
    When method get
    Then status 200
    Then match response.size() == 1

    Given path 'entities'
    And param type = 'example__Vehicle'
    And param q = ['example__isParked==urn:example:OffStreetParking:Downtown2', 'example__hasSensor==urn:sosa:Sensor:1234567890', 'name==name of vehicle 2', 'brandName==Mercedes']
    When method get
    Then status 200
    Then match response.size() == 1

    Given path 'entities'
    When method get
    Then status 400

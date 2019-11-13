Feature: create a beehive

Background:
  * url urlBase

Scenario: create a beehive and then check response status and header
  * def beehive = read('classpath:ngsild/beehive_with_geoproperty.json')

# Initial creation should succeed
Given path 'entities'
And request beehive
And header Link = '<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json'
When method post
Then status 201
And match header Location == '/ngsi-ld/v1/entities/urn:diat:BeeHive:TESTC'
And def beehiveUrn = 'urn:diat:BeeHive:TESTC'

# Trying to create it again should raise a 409
Given path 'entities'
And request beehive
When method post
Then status 409

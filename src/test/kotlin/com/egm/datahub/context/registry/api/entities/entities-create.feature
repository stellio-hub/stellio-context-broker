Feature: create a beehive

Background:
  * url urlBase

Scenario: create a beehive and then check response status and header
  * def beehive = read('classpath:ngsild/beehive.json')

# Initial creation should succeed
Given path 'entities'
And request beehive
When method post
Then status 201
And match header Location == '/ngsi-ld/v1/entities/urn:diat:BeeHive:TESTC'
And def beehiveUrn = 'urn:diat:BeeHive:TESTC'

# Trying to create it again should raise a 409
# TODO : currently does not raise a 409 (to be fixed after https://bitbucket.org/eglobalmark/context-registry/pull-requests/14)
#Given path 'entities'
#And request beehive
#When method post
#Then status 409

@ignore
Feature: create a beehive (ignore bc only expected to be set as a call arg)

Background:
  * url urlBase

Scenario: create a beehive and then check response status and header
  * def beehive = read('classpath:ngsild/beehive.json')

  Given path 'entities'
  And request beehive
  When method post
  Then status 201
  And match header Location == '/ngsi-ld/v1/entities/urn:diat:BeeHive:TESTC'
  And def beehiveUrn = 'urn:diat:BeeHive:TESTC'

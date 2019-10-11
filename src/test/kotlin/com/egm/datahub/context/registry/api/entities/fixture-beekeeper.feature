@ignore
Feature: create a beekeeper (ignore bc only expected to be set as a call arg)

Background:
  * url urlBase

Scenario: create a beekeeper and then check response status and header
  * def beekeeper = read('classpath:ngsild/beekeeper.json')

  Given path 'entities'
  And request beekeeper
  When method post
  Then status 201
  And match header Location == '/ngsi-ld/v1/entities/urn:diat:Beekeeper:Pascal'
  And def entityUrn = 'urn:diat:Beekeeper:Pascal'

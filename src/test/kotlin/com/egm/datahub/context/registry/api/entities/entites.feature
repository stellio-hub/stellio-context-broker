Feature: test the correct creation of a building entity

  Scenario: create a building and then check its id
    * def beehive =
      """
        {
          "@context": [
            "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
            "https://diatomic.eglobalmark.com/diatomic-context.jsonld"
          ],
          "id": "urn:diat:BeeHive:TESTC",
          "type": "BeeHive",
          "name": "ParisBeehive12",
          "connectsTo": {
            "type": "Relationship",
            "createdAt": "2010-10-26T21:32:52+02:00",
            "object": "urn:diat:Beekeeper:Pascal"
          }
        }
      """

  Given url 'http://localhost:8080/ngsi-ld/v1/entities'
  And request beehive
  When method post
  Then status 201
  Then match header Location == '/ngsi-ld/v1/entities/urn:diat:BeeHive:TESTC'

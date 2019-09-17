package com.egm.datahub.context.registry.web

class AlreadyExistingEntityException(message: String) : Exception(message)
class NotExistingEntityException(message: String) : Exception(message)
class EntityCreationException(message: String) : Exception(message)

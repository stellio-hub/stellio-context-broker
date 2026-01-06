# Upgrading to 2.30.0

This note describes the necessary steps to upgrade to Stellio 2.30.0

## Default ordering

With the implementation of orderBy we changed the default ordering of entities.
The new default ordering use the creation date of the entities instead of the entity id.
The oldest entity will appear first and the newest will appear last.
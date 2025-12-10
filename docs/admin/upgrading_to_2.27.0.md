# Upgrading to 2.27.0

This note describes the necessary steps to upgrade to Stellio 2.27.0

## Status code when creating a permission

When creating a permission which existed as per the business rules (same assignee, action and target as
an existing permission), Stellio previously returned a 409 (Conflict), which did not fully respect the
semantics of the 409 status code.

Instead, Stellio now returns a 303 (See Other) response with a `Location` header pointing to the existing
permission.

## Configuration of the max allowed size for payloads

Previously, the maximum allowed size for payloads was configurable using the `search.payload-max-body-size` property. 
As this property was basically redefining an existing Spring Boot configuration property, Stellio now only supports
using the Spring Boot one: `spring.http.codecs.max-in-memory-size` (see https://stellio.readthedocs.io/en/latest/admin/misc_configuration_tips.html#increase-the-max-allowed-size-for-body for more details).

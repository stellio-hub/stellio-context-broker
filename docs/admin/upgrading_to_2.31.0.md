# Upgrading to 2.31.0

This note describes the necessary steps to upgrade to Stellio 2.31.0

## API gateway configuration 

The services URL are now fully configurable inside the api-gateway. 
As part of this change the `APPLICATION_SEARCH_SERVICE_URL` and `APPLICATION_SUBSCRIPTION_SERVICE_URL`
now contain the entire URL not only the hostname.

For example 
````
APPLICATION_SEARCH_SERVICE_URL=my-hostname
APPLICATION_SUBSCRIPTION_SERVICE_URL=my-hostname
````
will now become 
````
APPLICATION_SEARCH_SERVICE_URL=http://my-hostname:8083
APPLICATION_SUBSCRIPTION_SERVICE_URL=http://my-hostname:8084
````
If you don't use one of this variable, the change will not impact you.
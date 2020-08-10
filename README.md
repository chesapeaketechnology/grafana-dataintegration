# grafana-dataintegration
Integration between Grafana and the Data science pipeline


## Required Environment Variables
* GDI_TOPIC - EventHub topic name
* GDI_MESSAGE_TYPE - The message type as defined in https://messaging.networksurvey.app/
* GDI_MESSAGE_VERSION - The message type version as defined in https://messaging.networksurvey.app/
* GDI_KEY - the EventHub access key
* GDI_NAMESPACE - the fully qualified namespace for the EventHub namespace
* GDI_SHARED_ACCESS_POLICY - The EventHub shared access policy name
* GDI_DB_HOST - The fully qualified host address for the integration database
* GDI_DB_PORT - The port for connecting to the integration database
* GDI_DB_DATABASE - The name of the database within the database server
* GDI_DB_USER - The user name to use when connecting to the database
* GDI_DB_PASSWORD - The password to use when conencting to the database
* GDI_DB_SCHEMA - The database schema

### Optional Environment Variables
* GDI_CONSUMER_GROUP - The EventHub consumer group. Defaults to '$default'.
* GDI_BUFFER_SIZE - The number of messages to receive before writing to the database. Defaults to 1.
* LOG_LEVEL - Can be 

## Changelog
 
##### [0.1.0]() - 2020-05-28
 * 

## Notes

#### Known Issues
 * 
 

## Contact
* **Les Stroud** - [lstroud](https://github.com/lstroud)  

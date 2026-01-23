# nuxeo-api-testing
Scripts for validating the output of Nuxeo API calls for retrieving metadata.

We have had issues with the Nuxeo API returning faulty results. Depending on the query, this could be an inconsistent number of records each time the query is run; or the same number of records but with duplicate results. Since Nuxeo support did not usefully respond to our ticket for several years, we wrote a script, which is deployed as an aws lambda function, to query the database directly as a workaround.

Nuxeo tech support has now finally responded to our ticket and suggests that adding an `ORDER BY` clause on a unique field such as uuid should fix the problem. The scripts in this repo compare the results of a Nuxeo API query vs the results of a direct database query using our in-house workaround script. We would love to be able to rely on the Nuxeo API rather than maintaining our workaround script if we can verify good results.

If this is a fix, then we will need to ensure that developers know that they must always add an "ORDER BY <unique field>" clause to any query.

## Nuxeo API queries to test

https://doc.nuxeo.com/nxdoc/nxql/ - specifically: `ecm:ancestorId = 'some-doc-id'` and `ecm:path STARTSWITH '/some/doc/path'`. Also `ecm:parentId = 'some-doc-id'`. Does adding `ORDER BY ecm:path, ecm:uuid` provide consistent results?

https://doc.nuxeo.com/nxdoc/rest-api-web-adapters/ - specifically the `@children` adapter. Cannot change the ordering.

## CDL code that needs to query nuxeo for metadata

# rikolti nuxeo fetcher

Currently uses the dbquery lambda

# nuxeo-extent-stats

Currently uses the dbquery lambda

# nuxeo_merritt

Currently uses Nuxeo API

# pynux

This is a python library that we wrote to interface with the Nuxeo API, as Nuxeo did not have their own python library at the time (or at least, Brian could not get it to work). It allows the user to issue any query they like, including problematic ones. Not sure how many people, if any, are using it to query nuxeo.



# nuxeo_spreadsheet

Uses Nuxeo API via pynux. Users have experienced problems with inconsistent data exports.

Exporter uses the following pynux utils functions: children(), get_metadata()

Importer uses: get_uid(), update_nuxeo_properties()

# nxcli

This is a utility written in node.js for interacting with the Nuxeo API. Unlike all of our other tooling around nuxeo, it is written in node and uses nuxeo's node library.

The `nx ls` utility uses the `@children` endpoint without a specific order clause.

The `nx q` utility allows the user to issue any query they like.

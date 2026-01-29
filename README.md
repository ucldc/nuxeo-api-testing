# nuxeo-api-testing
Scripts for validating the output of Nuxeo API calls for retrieving metadata.

Background: we have had long-term issues with the Nuxeo API returning faulty results. Depending on the query, this could be an inconsistent number of records each time the query is run; or the same number of records but with duplicate results.

An explanation of the cause of the problem, via Nuxeo tech support: PostgreSQL seems to provide inconsistent results when performing a paginated SELECT ordered on a field that is non-unique, like the nuxeo document name can be. See https://stackoverflow.com/questions/13580826/postgresql-repeating-rows-from-limit-offset.

The solution suggested by Nuxeo tech support: either you provide an additional ordering e.g. ecm:uuid, which is unqiue, or simply use ecm:uuid to order the results if ordering on the document name does not matter to you. In other words, please use ORDER BY ecm:name,ecm:uuid in your NXQL query, or just ORDER BY ecm:uuid.

Our tests confirm that we do get consistent and non-duplicative results if we use an NXQL query statement and always use ecm:uuid as the last field in the ORDER BY statement.

However, [the @children web adapter](https://doc.nuxeo.com/nxdoc/rest-api-web-adapters/) returns duplicate results when using pagination. There is no way to add an ORDER BY clause to @children requests, so we need to avoid using this endpoint altogether.

## Nuxeo API queries to test

https://doc.nuxeo.com/nxdoc/nxql/ - specifically: paginated SELECT with `ecm:ancestorId = 'some-doc-id'` and `ecm:path STARTSWITH '/some/doc/path'`. Also `ecm:parentId = 'some-doc-id'`. Does adding `ORDER BY ecm:path, ecm:uuid` provide consistent results?

https://doc.nuxeo.com/nxdoc/rest-api-web-adapters/ - specifically paginated queries using the `@children` adapter. There is no way for the user to request a specific ordering of results.

## CDL code that needs to query nuxeo for paginated metadata

# nuxeo_merritt

Uses several paginated SELECT statements that are not ordered by ecm:uuid.

# pynux

This is a python library that we wrote to interface with the Nuxeo API, as Nuxeo did not have their own python library at the time (or at least, Brian could not get it to work). It allows the user to issue any query they like, including problematic ones; it makes use the of the @children web adapter endpoint; and it uses a couple of hard-coded recursive paginated SELECT statements that are not ordered by ecm:uuid.

# nuxeo_spreadsheet

Uses Nuxeo API via pynux.

Exporter uses the following pynux utils functions: children(), get_metadata()

Importer uses: get_uid(), update_nuxeo_properties()

# nxcli

This is a utility written in node.js for interacting with the Nuxeo API. Unlike all of our other tooling around nuxeo, it is written in node and uses nuxeo's node library.

The `nx ls` utility uses the `@children` endpoint.

The `nx q` utility allows the user to issue any query they like, including problematic ones.

# rikolti nuxeo fetcher

Currently uses the dbquery lambda, which was written as a workaround for the issues described above before we understood what was causing them.

# nuxeo-extent-stats

Currently uses the dbquery lambda, which was written as a workaround for the issues described above before we understood what was causing them.

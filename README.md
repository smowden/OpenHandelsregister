# Handelsregister Dataset

# Download
coming soon

### Note
The docs below describe the data. To see how the sausage is made import this repo into Databricks and browse through the notebooks.
-------------


This dataset contains information about companies in the public german trade register.
It is aggregated from individual publications published on handelsregisterbekanntmachungen.de
Publications on this site were turned of as of 01.08.2022
At the current stage it is limited to commercial companies (GmbH's, AG's etc).

Data is provided on a best effort basis (see % coverage in brackets) and can include:
- Company name (95%)
- Dates (founding/dissolution dates, also the date the company appeared first and date of the last publication)
- Capital (63%)
- Objective (63%)
- Address (95%)
- People and their positions (81%)
- Reference numbers (99%)
For each dimension also historical data is provided. 
Companies are also tracked if they move between different register courts/cities/states.
Currently the dataset covers over **2.15 million companies**.


Do note that there are some caveats with regards to completeness:
- Only data pulished on handelsregisterbekanntmachungen can be used, however publications there start only in 2006
- Extraction of data is limited to technical capabilities and publications so sometimes fields can be empty
- More complicated company transformations such as mergers are not tracked yet

# Formats
### SQLite
The SQLite database is organized in a star schema fashion. For convinience it also contains FTS tables that enable full text search on names and 
objectives.
See schema.svg for the db schema


### Wide JSON & Avro
Schema below:
```
root
 |-- companyId: string (nullable = true)
 |-- currentName: string (nullable = true)
 |-- currentAddress: string (nullable = true)
 |-- currentObjective: string (nullable = true)
 |-- names: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- globalId: string (nullable = true)
 |    |    |-- validFrom: date (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- validTill: date (nullable = true)
 |    |    |-- isCurrent: boolean (nullable = true)
 |-- objectives: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- globalId: string (nullable = true)
 |    |    |-- validFrom: date (nullable = true)
 |    |    |-- objective: string (nullable = true)
 |    |    |-- validTill: date (nullable = true)
 |    |    |-- isCurrent: boolean (nullable = true)
 |-- addresses: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- globalId: string (nullable = true)
 |    |    |-- validFrom: date (nullable = true)
 |    |    |-- fullAddress: string (nullable = true)
 |    |    |-- address: string (nullable = true)
 |    |    |-- zipCode: string (nullable = true)
 |    |    |-- zipAndPlace: string (nullable = true)
 |    |    |-- validTill: date (nullable = true)
 |    |    |-- isCurrent: boolean (nullable = true)
 |-- positions: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- globalId: string (nullable = true)
 |    |    |-- dateOfPublication: date (nullable = true)
 |    |    |-- foundPosition: string (nullable = true)
 |    |    |-- startDate: date (nullable = true)
 |    |    |-- endDate: date (nullable = true)
 |    |    |-- firstName: string (nullable = true)
 |    |    |-- lastName: string (nullable = true)
 |    |    |-- maidenName: string (nullable = true)
 |    |    |-- birthDate: string (nullable = true)
 |    |    |-- city: string (nullable = true)
 |-- referenceNumbers: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- stdRefNo: string (nullable = true)
 |    |    |-- nativeReferenceNumber: string (nullable = true)
 |    |    |-- courtName: string (nullable = true)
 |    |    |-- courtCode: string (nullable = true)
 |    |    |-- referenceNumberFirstSeen: date (nullable = true)
 |    |    |-- validTill: date (nullable = true)
 |-- capital: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- globalId: string (nullable = true)
 |    |    |-- validFrom: date (nullable = true)
 |    |    |-- capitalAmount: integer (nullable = true)
 |    |    |-- capitalCurrency: string (nullable = true)
 |    |    |-- validTill: date (nullable = true)
 |    |    |-- isCurrent: boolean (nullable = true)
 ```


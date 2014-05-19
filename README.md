# Morph

Morph-RDB (formerly called ODEMapster) is an RDB2RDF engine developed by the Ontology Engineering Group, that follows the R2RML specification (http://www.w3.org/TR/r2rml/). 

Morph-RDB supports two operational modes: data upgrade (generating RDF instances from data in a relational database) and query translation (SPARQL to SQL). Morph-RDB employs various optimisation techniques in order to generate efficient SQL queries, such as self-join elimination and subquery elimination. 

Morph-RDB has been tested with real queries from various Spanish/EU projects and has proven to work faster than other state-of-the-art tools available. At the moment, Morph-RDB works with MySQL, PostgreSQL, and MonetDB.

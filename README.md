**DISCLAIMER**: This software is no longer maintained, although still operational. If you are interested in a materialisation engine instead of a query rewriting engine, you can make use of [Morph-KGC](https://github.com/oeg-upm/morph-kgc)

# Morph-RDB

Morph-RDB (formerly called ODEMapster) is an RDB2RDF engine developed by the Ontology Engineering Group, that follows the R2RML specification (http://www.w3.org/TR/r2rml/). 

Morph-RDB supports two operational modes: data upgrade (generating RDF instances from data in a relational database) and query translation (SPARQL to SQL). Morph-RDB employs various optimisation techniques in order to generate efficient SQL queries, such as self-join elimination and subquery elimination. 

Morph-RDB has been tested with real queries from various Spanish/EU projects and has proven to work faster than other state-of-the-art tools available. At the moment, Morph-RDB works with MySQL, PostgreSQL, H2, CSV files and MonetDB.
<p align="center">
  <img src="https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/architecture.png">
</p>


## How to use
User guides:
For those who want to use this project on an user level, you can find a little guide to on the main branch wiki : https://github.com/oeg-upm/morph-rdb/wiki

If, on the other hand, you want to edit the project or at least work from an IDE such as Eclipse, we suggest you to follow this steps:
 - Download the source code.
 - Once unziped, you may notice that the imports doesn´t match the actual directories. In order to avoid changing all the imports or all the directories, import this way: Import -> Maven -> Existing Maven Project, and select as root the folder where you unziped the project (it may take a few minutes).
 - Now that it´s finally imported, you can run the file es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner.scala (in other to pass the arguments in Eclipse, right click, Run As -> Run Configuration -> Arguments, and remember to imput both the path to the .properties file and it´s full name).
* In case the program doesn´t find the file log4j.properties, move it from "morph-examples" to "morph-r2rml-rdb", thought this file isn´t essential.

## Acknowledgement
From January 2016 to January 2018, the development of morph-RDB has been supported by the Mobile Age project (http://www.mobile-age.eu/). After that period, the development has been supported by the Datos 4.0 Spanish national proect.

## Authors
- Freddy Priyatna

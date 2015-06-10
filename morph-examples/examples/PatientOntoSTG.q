PREFIX : <http://www.semanticweb.org/mrezk#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX quest: <http://obda.org/quest#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT * WHERE
{ 
	?p rdf:type :Patient .
	?p :hasName ?pname .
	?p :hasStage2 ?s .
}

# ONTOP 
# SELECT \n   1 AS \"pQuestType\", NULL AS \"pLang\", ('http://www.semanticweb.org/mrezk#db1/' || REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(QVIEW1.\"PATIENTID\" AS CHAR),' ', '%20'),'!', '%21'),'@', '%40'),'#', '%23'),'$', '%24'),'&', '%26'),'*', '%42'), '(', '%28'), ')', '%29'), '[', '%5B'), ']', '%5D'), ',', '%2C'), ';', '%3B'), ':', '%3A'), '?', '%3F'), '=', '%3D'), '+', '%2B'), '''', '%22'), '/', '%2F')) AS \"p\", \n   3 AS \"pnameQuestType\", NULL AS \"pnameLang\", CAST(QVIEW1.\"NAME\" AS CHAR) AS \"pname\", \n   1 AS \"sQuestType\", NULL AS \"sLang\", ('http://www.semanticweb.org/mrezk#db1/stage-' || REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(QVIEW2.\"STAGENAME\" AS CHAR),' ', '%20'),'!', '%21'),'@', '%40'),'#', '%23'),'$', '%24'),'&', '%26'),'*', '%42'), '(', '%28'), ')', '%29'), '[', '%5B'), ']', '%5D'), ',', '%2C'), ';', '%3B'), ':', '%3A'), '?', '%3F'), '=', '%3D'), '+', '%2B'), '''', '%22'), '/', '%2F')) AS \"s\"\n 
#FROM \n\"tbl_patient\" QVIEW1,\n\"tbl_stage\" QVIEW2
#WHERE \nQVIEW1.\"PATIENTID\" IS NOT NULL AND\nQVIEW1.\"NAME\" IS NOT NULL AND\n(QVIEW1.\"STAGE\" = QVIEW2.\"STAGEID\") AND\nQVIEW2.\"STAGENAME\" IS NOT NULL;


# <http://www.semanticweb.org/mrezk#db1/1>,"Mary",<http://www.semanticweb.org/mrezk#db1/stage-NSCLC%20Stage%20IIIa>
# <http://www.semanticweb.org/mrezk#db1/2>,"John",<http://www.semanticweb.org/mrezk#db1/stage-SCLC%20Stage%20Limited>


# MORPH
# SELECT `T2`.`patientid` AS "p",`T2`.`patientid` AS "tumor",1201466784 AS "mappingid_p",636782475 AS "mappingid_tumor"
# FROM tbl_patient T2
# WHERE `T2`.`name` IS NOT NULL

# <uri>http://mappingpedia.linkeddata.es/resources/Patient/1</uri> , <literal>mary</literal>, <uri>http://mappingpedia.linkeddata.es/resources/Neoplasm/1</uri>
# <uri>http://mappingpedia.linkeddata.es/resources/Patient/2</uri> , <literal>john</literal>, <uri>http://mappingpedia.linkeddata.es/resources/Neoplasm/2</uri>


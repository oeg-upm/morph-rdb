PREFIX : <http://www.semanticweb.org/mrezk#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX quest: <http://obda.org/quest#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT * WHERE
{ 
	?p rdf:type :Patient .
	?p :hasStage2 ?s .
	OPTIONAL {
		?s :hasStageName ?stagename .
	}
	
}

# ONTOP 
# SELECT \n   1 AS \"pQuestType\", NULL AS \"pLang\", ('http://www.semanticweb.org/mrezk#db1/' || REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(QVIEW1.\"PATIENTID\" AS CHAR),' ', '%20'),'!', '%21'),'@', '%40'),'#', '%23'),'$', '%24'),'&', '%26'),'*', '%42'), '(', '%28'), ')', '%29'), '[', '%5B'), ']', '%5D'), ',', '%2C'), ';', '%3B'), ':', '%3A'), '?', '%3F'), '=', '%3D'), '+', '%2B'), '''', '%22'), '/', '%2F')) AS \"p\", \n   1 AS \"sQuestType\", NULL AS \"sLang\", ('http://www.semanticweb.org/mrezk#db1/stage/' || REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(QVIEW1.\"STAGE\" AS CHAR),' ', '%20'),'!', '%21'),'@', '%40'),'#', '%23'),'$', '%24'),'&', '%26'),'*', '%42'), '(', '%28'), ')', '%29'), '[', '%5B'), ']', '%5D'), ',', '%2C'), ';', '%3B'), ':', '%3A'), '?', '%3F'), '=', '%3D'), '+', '%2B'), '''', '%22'), '/', '%2F')) AS \"s\", \n   3 AS \"stagenameQuestType\", NULL AS \"stagenameLang\", CAST(QVIEW2.\"STAGENAME\" AS CHAR) AS \"stagename\"\n 
# FROM \n\n        (\n        \"tbl_patient\" QVIEW1\n        LEFT OUTER JOIN\n        \"tbl_stage\" QVIEW2\n        ON\n        (QVIEW1.\"STAGE\" = QVIEW2.\"STAGEID\") AND\n        QVIEW1.\"STAGE\" IS NOT NULL AND\n        QVIEW2.\"STAGENAME\" IS NOT NULL\n         )
# WHERE \nQVIEW1.\"PATIENTID\" IS NOT NULL AND\nQVIEW1.\"STAGE\" IS NOT NULL;

# <http://www.semanticweb.org/mrezk#db1/1>,<http://www.semanticweb.org/mrezk#db1/stage/4>,"NSCLC Stage IIIa"
# <http://www.semanticweb.org/mrezk#db1/2>,<http://www.semanticweb.org/mrezk#db1/stage/7>,"SCLC Stage Limited"
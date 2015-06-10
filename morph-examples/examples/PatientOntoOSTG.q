PREFIX : <http://www.semanticweb.org/mrezk#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX quest: <http://obda.org/quest#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?p ?name WHERE
{ 
	?p rdf:type :Patient .
	OPTIONAL {
		?p :hasName ?name .
	}
}

# ONTOP
# SELECT \n   1 AS \"pQuestType\", NULL AS \"pLang\", ('http://www.semanticweb.org/mrezk#db1/' || REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(QVIEW1.\"PATIENTID\" AS CHAR),' ', '%20'),'!', '%21'),'@', '%40'),'#', '%23'),'$', '%24'),'&', '%26'),'*', '%42'), '(', '%28'), ')', '%29'), '[', '%5B'), ']', '%5D'), ',', '%2C'), ';', '%3B'), ':', '%3A'), '?', '%3F'), '=', '%3D'), '+', '%2B'), '''', '%22'), '/', '%2F')) AS \"p\", \n   3 AS \"nameQuestType\", NULL AS \"nameLang\", CAST(QVIEW2.\"NAME\" AS CHAR) AS \"name\"\n 
# FROM \n\n        (\n        \"tbl_patient\" QVIEW1\n        
# LEFT OUTER JOIN\n        \"tbl_patient\" QVIEW2\n        ON\n        (QVIEW1.\"PATIENTID\" = QVIEW2.\"PATIENTID\") AND\n        QVIEW1.\"PATIENTID\" IS NOT NULL AND\n        QVIEW2.\"NAME\" IS NOT NULL\n         )\n
# WHERE \nQVIEW1.\"PATIENTID\" IS NOT NULL;

# <http://www.semanticweb.org/mrezk#db1/1>,"Mary"
# <http://www.semanticweb.org/mrezk#db1/2>,"John"

# MORPH



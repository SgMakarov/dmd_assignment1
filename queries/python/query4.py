from neo4j import GraphDatabase
import csv

uri = "bolt://localhost:7687/"
user = "neo4j"
password = "test"

driver = GraphDatabase.driver(uri,auth=(user, password))
session = driver.session()
result = session.run('''
                        MATCH (c:Customer{customer_id: "322"})-[:watch]->(film)
                        WITH count(film) AS overall, c 
                        MATCH (c)-[:watch]->(film2)<-[:watch]-(c2:Customer)	
                        WITH c, overall, count(DISTINCT film2) AS common, c2 
                        MATCH (f:Film)<-[:watch]-(c2) WHERE NOT (f:Film)<-[:watch]-(c)
                        WITH c.first_name AS first_name, c.last_name AS last_name, f.title AS title, sum((common * 1.0)/overall) AS metric
                        RETURN first_name, last_name,  title , metric ORDER BY metric desc LIMIT 10
                        
                        '''
                )
output = [["first_name", "last_name", "title", "metric"]]

for record in result:
    output.append([record["first_name"], record["last_name"], record["title"], record["metric"]])
output_file = open('tables/queries/query4.csv', 'w')
with output_file:
    writer = csv.writer(output_file)
    writer.writerows(output)
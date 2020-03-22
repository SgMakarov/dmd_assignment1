from neo4j import GraphDatabase
import csv

uri = "bolt://localhost:7687/"
user = "neo4j"
password = "test"

driver = GraphDatabase.driver(uri,auth=(user, password))
session = driver.session()
result = session.run('''MATCH  p=shortestPath(
                        (bacon:Actor {actor_id:127})-[*..10]-(actor:Actor)
                        ) WHERE actor.actor_id <> 127
                        RETURN actor.first_name AS name, actor.last_name AS surname, (length(p) / 2) AS distance
                        ORDER BY name'''
                )
output = [["name", "surname", "distance"]]

for record in result:
    output.append([record["name"], record["surname"], record["distance"]])
output_file = open('tables/queries/query5.csv', 'w')
with output_file:
    writer = csv.writer(output_file)
    writer.writerows(output)
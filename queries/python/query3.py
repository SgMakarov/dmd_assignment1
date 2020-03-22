from neo4j import GraphDatabase
import csv

uri = "bolt://localhost:7687/"
user = "neo4j"
password = "test"

driver = GraphDatabase.driver(uri,auth=(user, password))
session = driver.session()
result = session.run('''MATCH(category:Category)-[:film_category]-(film:Film)-[:inventory_film]-()-[:inventory_rental]-(rental:Rental) 
                        RETURN film.title AS film, category.name AS category, count(rental) AS rentals 
                        ORDER BY rentals DESC'''
                )
output = [["film", "category", "rentals"]]

for record in result:
    output.append([record["film"], record["category"], record["rentals"]])
output_file = open('tables/queries/query3.csv', 'w')
with output_file:
    writer = csv.writer(output_file)
    writer.writerows(output)
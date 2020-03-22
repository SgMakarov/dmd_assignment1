from neo4j import GraphDatabase
import csv

uri = "bolt://localhost:7687/"
user = "neo4j"
password = "test"

driver = GraphDatabase.driver(uri,auth=(user, password))
session = driver.session()
result = session.run('''MATCH (customer:Customer)-[:customer_rental]-(rental:Rental)-[:inventory_rental]-()-[:inventory_film]-()-[:film_category]-(category:Category) 
                        WHERE datetime(replace(rental.rental_date, ' ', 'T')).year = 2006 
                        WITH customer,  count(distinct category) AS categories 
                        WHERE categories >=2 RETURN customer.customer_id AS id, customer.first_name AS name, customer.last_name AS surname ORDER BY id'''
                )
output = [["id", "name", "surname"]]

for record in result:
    output.append([record["id"], record["name"], record["surname"]])
output_file = open('tables/queries/query1.csv', 'w')
with output_file:
    writer = csv.writer(output_file)
    writer.writerows(output)
from neo4j import GraphDatabase
import csv
import collections
uri = "bolt://localhost:7687/"
user = "neo4j"
password = "test"

driver = GraphDatabase.driver(uri, auth=(user, password))
session = driver.session()
result = session.run('''
                        MATCH (a1:Actor)-[:film_actor]-(film:Film)-[:film_actor]-(a2:Actor)
                        RETURN [a1.actor_id, a1.first_name, a1.last_name] AS actor1, [a2.actor_id, a2.first_name, a2.last_name] AS actor2, count(film) AS count
                        UNION
                        MATCH (a1:Actor), (a2:Actor) WHERE NOT (a1)-[]-()-[]-(a2)
                        RETURN [a1.actor_id, a1.first_name, a1.last_name] AS actor1, [a2.actor_id, a2.first_name, a2.last_name] AS actor2, 0 AS count
                        '''
                )
res = collections.defaultdict(lambda: collections.defaultdict(list))
for record in result:
    res[record["actor1"][0] + " " + record["actor1"][1] + " " + record["actor1"][2]
        ][record["actor2"][0] + " "+record["actor2"][1] + " " + record["actor2"][2]] = record["count"]
i = 1
output = [[" "] +list(res.keys())]
for actor1 in res.keys():
    output.append([actor1])
    for actor2 in res.keys():
        output[i].append(res[actor1][actor2])
    i += 1
output_file=open('tables/queries/query2.csv', 'w')
with output_file:
    writer=csv.writer(output_file)
    writer.writerows(output)

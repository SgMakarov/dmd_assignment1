MATCH (a1:Actor)-[:film_actor]-(film:Film)-[:film_actor]-(a2:Actor) 
RETURN [a1.actor_id, a1.first_name, a1.last_name] AS actor1, [a2.actor_id, a2.first_name, a2.last_name] AS actor2, count(film) AS count 
UNION 
MATCH (a1:Actor), (a2:Actor) WHERE NOT (a1)-[]-()-[]-(a2) 
RETURN [a1.actor_id, a1.first_name, a1.last_name] AS actor1, [a2.actor_id, a2.first_name, a2.last_name] AS actor2, 0 AS count
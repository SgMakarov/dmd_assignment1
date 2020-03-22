MATCH  p=shortestPath(
(bacon:Actor {actor_id:127})-[*..6]-(actor:Actor)
) WHERE actor.actor_id <> 127
RETURN actor.first_name AS name, actor.last_name AS surname, (length(p) / 2) AS distance
ORDER BY name
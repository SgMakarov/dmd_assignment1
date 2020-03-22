MATCH (c:Customer{customer_id: "322"})-[:watch]->(film)
WITH count(DISTINCT film) AS overall, c  //determine how many films customer to recommend watched 
MATCH (c)-[:watch]->(film2)<-[:watch]-(c2:Customer)	//find another customer
WITH c, overall, count(DISTINCT film2) AS common, c2  // find size of theif intersection
MATCH (f:Film)<-[:watch]-(c2) WHERE NOT (f:Film)<-[:watch]-(c)// find films that second watched and first not
WITH c.first_name AS first_name, c.last_name AS last_name, f.title AS title, sum((common * 1.0)/overall) AS metric //for each such "second" add metric
RETURN first_name, last_name,  title , metric ORDER BY metric desc LIMIT 10
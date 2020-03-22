MATCH (customer:Customer)-[:customer_rental]-(rental:Rental)-[:inventory_rental]-()-[:inventory_film]-()-[:film_category]-(category:Category) 
WHERE rental.rental_date.year = 2006 
WITH customer,  count(distinct category) AS categories 
WHERE categories >=2 RETURN customer.customer_id AS id, customer.first_name AS name, customer.last_name AS surname ORDER BY id
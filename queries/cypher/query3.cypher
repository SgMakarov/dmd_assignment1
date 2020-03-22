MATCH(category:Category)-[:film_category]-(film:Film)-[:inventory_film]-()-[:inventory_rental]-(rental:Rental) 
RETURN film.title AS film, category.name AS category, count(rental) AS rentals 
ORDER BY rentals DESC
from neo4j import GraphDatabase
import psycopg2
import sys

uri = "bolt://localhost:7687/"
user = "neo4j"
password = "test"
PATH = "./tables/db/" #path for csv to import 

tables = ("actor", "film", "language", "category", "city", "country",
          "address", "customer", "rental", "payment", "inventory", "staff", "store")
labels = ("Actor", "Film", "Language", "Category", "City", "Country",
          "Address", "Customer", "Rental", "Payment", "Inventory", "Staff", "Store")


class driver(object):
    '''
    during initialization we create driver and then create index for every label
    '''
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri,
                                            auth=(user, password))

        self.execute_query("CREATE INDEX ON :Language(language_id)")
        self.execute_query("CREATE INDEX ON :Film(film_id)")
        self.execute_query("CREATE INDEX ON :Category(category_id)")
        self.execute_query("CREATE INDEX ON :Address(address_id)")
        self.execute_query("CREATE INDEX ON :Payment(payment_id)")
        self.execute_query("CREATE INDEX ON :Inventory(inventory_id)")
        self.execute_query("CREATE INDEX ON :Rental(rental_id)")
        self.execute_query("CREATE INDEX ON :Actor(actor_id)")
        self.execute_query("CREATE INDEX ON :Staff(staff_id)")
        self.execute_query("CREATE INDEX ON :Customer(customer_id)")
        self.execute_query("CREATE INDEX ON :Country(country_id)")
        self.execute_query("CREATE INDEX ON :City(city_id)")

    def close(self):
        self._driver.close()
    
    '''
    This function can execute any given query
    '''
    def execute_query(self, query):
        with self._driver.session() as session:
            session.write_transaction(self._tx, query)

    @staticmethod
    def _tx(tx, query):
        tx.run(query)

    '''
    Here is a function that takes table, load csv file with this name and create node with given label
    '''
    def import_table(self, table, label):
        self.execute_query('''
            LOAD CSV WITH HEADERS FROM 'file:///'''+table+'''.csv' AS row FIELDTERMINATOR ','
            CREATE (a:'''+label+''')    
            SET a=row        
        '''
                           )
    ''' 
    film_actor and film_category is a special case, as we have 
    a table for these relations, so I process them in special functions
    '''
    def connect_actor(self):
        self.execute_query('''
            LOAD CSV WITH HEADERS FROM 'file:///film_actor.csv' AS row FIELDTERMINATOR ','
            MATCH (f:Film), (a:Actor)
            WHERE f.film_id = row.film_id AND a.actor_id = row.actor_id
            CREATE (f)-[r:film_actor{last_update: row.last_update}]->(a)
        '''
                           )

    def connect_category(self):
        self.execute_query('''
            LOAD CSV WITH HEADERS FROM 'file:///film_category.csv' AS row FIELDTERMINATOR ','
            MATCH (f:Film), (a:Category)
            WHERE f.film_id = row.film_id AND a.category_id = row.category_id
            CREATE (a)-[r:film_category{last_update: row.last_update}]->(f)
        '''
                           )

    '''
    quite a big function where we process all foreign keys
    we match each node to every node where primary key is equal to foreign key of a node
    and then create connection between them. 
    '''
    def make_connections(self):

        self.execute_query('''MATCH (f:Film),(l:Language)
            WHERE f.language_id = l.language_id ''' +
                           ''' CREATE (l)-[r:film_language]->(f)''')
        self.execute_query('''MATCH (ci:City),(co:Country)
            WHERE ci.country_id = co.country_id''' +
                           ''' CREATE (co)-[r:city_in_country]->(ci)''')
        self.execute_query('''MATCH (c:City),(a:Address)
            WHERE a.city_id = c.city_id''' +
                           ''' CREATE (c)-[r:address_in_city]->(a)''')
        self.execute_query('''MATCH (c:Store),(a:Address)
            WHERE c.address_id = a.address_id''' +
                           ''' CREATE (a)-[r:store_address]->(c)''')
        self.execute_query('''MATCH (c:Customer),(a:Address)
            WHERE c.address_id = a.address_id''' +
                           ''' CREATE (a)-[r:customer_address]->(c)''')
        self.execute_query('''MATCH (s:Store),(c:Customer)
            WHERE c.store_id = s.store_id''' +
                           ''' CREATE (s)-[r:store_customer]->(c)''')
        self.execute_query('''MATCH (c:Store   ),(i:Inventory)
            WHERE i.store_id = c.store_id''' +
                           ''' CREATE (i)-[r:store_inventory]->(c)''')
        self.execute_query('''MATCH (c: Film   ),(i:Inventory)
            WHERE i.film_id = c.film_id''' +
                           ''' CREATE (i)-[r:inventory_film]->(c)''')
        self.execute_query('''MATCH (c:Staff),(a:Address)
            WHERE c.address_id = a.address_id''' +
                           ''' CREATE (a)-[r:staff_address]->(c)''')
        self.execute_query('''MATCH (c:Staff),(a:Store)
            WHERE c.store_id = a.store_id''' +
                           ''' CREATE (a)-[r:store_staff]->(c)''')
        self.execute_query('''MATCH (c:Rental),(i:Inventory)
            WHERE c.inventory_id = i.inventory_id''' +
                           ''' CREATE (i)-[r:inventory_rental]->(c)''')
        self.execute_query('''MATCH (c:Customer   ),(i:Rental)
                WHERE c.customer_id = i.customer_id''' +
                           ''' CREATE (i)-[r:customer_rental]->(c)''')
        self.execute_query('''MATCH (c:Staff   ),(i:Rental)
            WHERE c.staff_id = i.staff_id''' +
                           ''' CREATE (i)-[r:staff_rental]->(c)''')
        self.execute_query('''MATCH (r:Rental   ),(p:Payment)
        WHERE r.rental_id = p.rental_id''' +
                           ''' CREATE (p)-[f:payment_rental]->(r)''')
        self.execute_query('''MATCH (r:Customer   ),(p:Payment)
            WHERE r.customer_id = p.customer_id''' +
                           ''' CREATE (p)-[f:payment_customer]->(r)''')
        self.execute_query('''MATCH (c:Staff   ),(i:Payment)
            WHERE c.staff_id = i.staff_id''' +
                           ''' CREATE (i)-[r:payment_staff]->(c)''')
        self.execute_query('''
            MATCH (customer:Customer)-[:customer_rental]-()-[:inventory_rental]-()-[:inventory_film]-(film) MERGE (customer) -[r:watch]->(film);
        
        ''')

'''
    here is a function to export table as csv by table's name
'''
def export_table(cur, table): 
    f = open(f"{PATH}{table}.csv", "w")
    cur.copy_expert(f"copy {table} to STDOUT with (FORMAT CSV, HEADER, DELIMITER ',') ", f)

if __name__ == "__main__":
    con = psycopg2.connect(database="dvdrental", user="postgres",
                           password="postgres", host="127.0.0.1", port="5432")
    neo_driver = driver(uri, user, password)

    cur = con.cursor()
    for i in range(len(tables)):
        export_table(cur, tables[i])
        neo_driver.import_table(tables[i], labels[i])
    export_table(cur, "film_actor")
    export_table(cur, "film_category")
    neo_driver.connect_actor()
    neo_driver.connect_category()
    neo_driver.make_connections()
    neo_driver.close()

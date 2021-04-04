
#import neo4j
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class App:


    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    def close(self):
            # Don't forget to close the driver connection when you are finished with it
            self.driver.close()
    def create_friendship(self,file_name,field_value,field_name,info_):
            with self.driver.session() as session:
                # Write transactions allow the driver to handle retries and transient errors
                result = session.write_transaction(
                    self._create_and_return_friendship, file_name, field_value,field_name,info_)
                print(result)
                for row in result:
                    print("Created relation between: {n}, {m} ".format(n=row['n'], m=row['m']))
                    print("Created relation between: {n}, {e} ".format(n=row['n'], e=row['e']))
                    print("Created relation between: {e}, {m} ".format(e=row['e'], m=row['m']))
                    print("Created relation between: {m}, {w} ".format(m=row['m'], w=row['w']))
    @staticmethod
    def _create_and_return_friendship(tx, file_name, field_value,field_name,info_):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = """
        merge (n:File {Name: $file_name})
        merge (m:FIELD {Name: $field_name})
        merge (e:VALUE {value: $field_value})
        merge (w:DLP_Classification {NAME: $info_})

        merge (n)-[p:CONTAINS_FIELD]->(m)
        merge (n)-[q:CONTAINS_VALUE]->(e)
        merge (e)-[r:TYPE_IS]->(m)
        merge (m)-[s:DATA_Classification]->(w)


        
        RETURN n, m, e, w, p, q, r, s
        """
        result = tx.run(query, file_name=file_name, field_value=field_value,field_name=field_name,info_=info_)
        try:
            return [{"n": row["n"]["name"], "e": row["e"]["address"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

f_value=['hammad','saad','usaman']
info_name=['phn_nom','emil','skool']
f_name=['0300','yahoo@gmail','bcs']
import itertools
for (a,b,c) in zip(f_value,f_name,info_name):
    print(a+','+b+','+c)
    bolt_url = "neo4j+s://cfb079ca.databases.neo4j.io"
    user = "neo4j"
    password = "hqr34pE89cwcrVxyYM6IpUmWjSZfK3pQ7D02i5nwe44"
    app = App(bolt_url, user, password)
    app.create_friendship('nothing.pdf', a,b,c)
    app.close()
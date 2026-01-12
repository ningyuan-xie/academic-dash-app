from neo4j import GraphDatabase
import os

_uri = os.environ["NEO4J_URI"]
_user = os.environ["NEO4J_USERNAME"]
_password = os.environ["NEO4J_PASSWORD"]

driver = GraphDatabase.driver(_uri, auth=(_user, _password))

with driver.session() as session:
    session.run("""
        MERGE (h:Heartbeat {name: 'keepalive'})
        SET h.lastSeen = datetime()
    """)

driver.close()

print("Neo4j Aura heartbeat write completed successfully.")

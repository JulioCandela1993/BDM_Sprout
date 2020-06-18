from neo4j import GraphDatabase, basic_auth
from elasticsearch import Elasticsearch

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo"))
es = Elasticsearch(
        cloud_id="bdm_sdm_test2:ZXVyb3BlLXdlc3QxLmdjcC5jbG91ZC5lcy5pbyQwNDY5NjU0MTkyYjY0MTgwYjYwZWFkNjM3NjlkYmUzYiQ5NGFlMDgzZDA2NjI0MDkzYWUxNWIzZDQwMWY5MWY2NA==",
        http_auth=("elastic", "Nn0UZWuHWM2pBxVSMTKQQd99"),
)
def delete_all(tx):
    tx.run("match (n) with n DETACH DELETE n; ")

def reset_bestseller(tx):
    tx.run("MATCH (p:product) SET p.bestseller = false")

def get_recommended_product(tx):
    result = tx.run("MATCH (c:customer)<-[:recommended_for]-(p:product) \
            WHERE ID(c) = {userID} \
            RETURN p as recommendedProduct", userID=userID)
    return list(result)

def recommend_from_order(tx):
    tx.run("MATCH (c:customer)-[:buys]->(o1:order)-[:contains]->(p1:product)<-[:contains]-(o2:order)-[:contains]->(p2:product) \
            WHERE ID(c) = {userID} \
            WITH p2 as RecommendedProduct, count(o2) as Score \
            ORDER BY Score DESC \
            LIMIT 10 \
            WITH RecommendedProduct \
            MATCH (c:customer) WHERE ID(c) = {userID} \
            MERGE (RecommendedProduct)-[:recommended_for]->(c)", userID=userID)

def recommend_from_rating(tx):
    tx.run("MATCH (c1:customer)-[f1:feedback]->(p1:product)<-[f2:feedback]-(c2:customer)-[f3:feedback]->(p2:product) \
            WHERE ID(c1) = {userID} \
            AND f1.sentimentalAnalysis = 'POSITIVE' \
            AND f2.sentimentalAnalysis = 'POSITIVE' \
            AND f3.sentimentalAnalysis = 'POSITIVE' \
            WITH p2 as RecommendedProduct, f3.rating as Score \
            ORDER BY Score DESC \
            LIMIT 10 \
            WITH RecommendedProduct \
            MATCH (c:customer) WHERE ID(c) = {userID} \
            MERGE (RecommendedProduct)-[:recommended_for]->(c)", userID=userID)

def get_best_seller(tx):
    result = tx.run("MATCH (p)<-[:contains]-(o) \
                            RETURN p as bestSeller, count(o) as Score \
                            ORDER BY Score DESC \
                            LIMIT 10")
    return list(result)

with driver.session() as session:
    userID = 2962
    listRecommendedProducts = {}

    # Execute recommender to generate some [:recommended_to] relationships
    session.write_transaction(recommend_from_order)
    session.write_transaction(recommend_from_rating)

    # Query recommended products for the user
    recommendedProducts = session.read_transaction(get_recommended_product)
    for record in recommendedProducts:
        id = record["recommendedProduct"].id
        listRecommendedProducts[id] = record["recommendedProduct"]

    print(listRecommendedProducts)

    # Query feedbacks from the user from ElasticSearch
    query_body = {"query": {"bool": {"must": {"match": {"userid": userID}}}}}
    feedbacks = es.search(index="cust_index_hung", body=query_body)

    # Filter out products with negative feedbacks
    for feedback in feedbacks['hits']['hits']:
        if(feedback['_source']['sentAnalysis']=='NEGATIVE'):
            listRecommendedProducts.pop(feedback['_source']['productid'], None)

    print(listRecommendedProducts)




import time
import os
import pandas as pd
import random

# os.chdir("G:\\Documentos\\MasterDegree\\BDMA\\Classes\\UPC\\SDM\\Project\\DataIntegration")



positive_db = pd.read_csv("SparkStreaming\\src\\main\\resources\\pos-words.txt", header = None) 
negative_db = pd.read_csv("SparkStreaming\\src\\main\\resources\\neg-words.txt", header = None) 
stop_db = pd.read_csv("SparkStreaming\\src\\main\\resources\\stop-words.txt", header = None) 

words = pd.DataFrame()
words = words.append(positive_db)
words = words.append(negative_db)
words = words.append(stop_db)
words = words.values.tolist()

customer_db = pd.read_csv("Spark\\src\\main\\resources\\customerNode\\part-00000",sep = ";") 
product_db = pd.read_csv("Spark\\src\\main\\resources\\productNode\\part-00000",sep = ";") 
customer_db=customer_db[":ID"]
product_db=product_db[[":ID","nameproduct:string"]]

customer_db_id = [i[1:] for i in customer_db]
product_db_id = [i[1:] for i in product_db[":ID"]]
product_db_name = [i[1:] for i in product_db["nameproduct:string"]]

while(True):
    product = product_db.sample()
    product_db_id = product[":ID"].to_string(index=False).strip()[1:]
    product_db_name = product["nameproduct:string"].to_string(index=False).strip()
    customer = random.choice(customer_db_id)
    rating = random.randint(1,5)
    about = f"{product_db_name} is"
    for i in range(3, random.randint(3,20)):
        about += " " + random.choice(words)[0]
    json_string = f"\"customerid\":\"{customer}\", \"productid\":\"{product_db_id}\", \"rating\": {rating}, \"about\":\"{about}\", \"product\":\"{product_db_name}\""
    os.system("echo "+"{"+json_string+"}")
    time.sleep(1)

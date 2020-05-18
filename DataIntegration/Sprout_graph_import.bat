cd Source

C:\Server\neo4j-community-3.5.11\bin\neo4j-admin import --ignore-missing-nodes=true --high-io=true --mode=csv --database=db_sprout.db --delimiter ";"  ^
--array-delimiter "|" --id-type INTEGER --nodes:customer "customer_node.csv" --nodes:segment "segment_node.csv"  ^
--nodes:category "category_node.csv" --nodes:farmer "farmer_node.csv"  ^
--nodes:order "order_node.csv" --nodes:product "product_node.csv"  ^
--nodes:quarter "quarter_node.csv"  ^
--relationships:classified_as "classifiedas_edge.csv"  --relationships:supplied_by "suppliedby_edge.csv" ^
--relationships:in_quarter "inquarter_edge.csv"  --relationships:contains "contains_edge.csv" ^
--relationships:buys "buys_edge.csv"  --relationships:belongs_to "belongsto_edge.csv" ^
>"output.txt"
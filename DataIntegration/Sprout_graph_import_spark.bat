cd Spark\src\main\resources

C:\Server\neo4j-community-3.5.11\bin\neo4j-admin import --ignore-missing-nodes=true --high-io=true --mode=csv --database=db_sprout_spark.db --delimiter ";"  ^
--array-delimiter "|" --id-type STRING ^
--nodes:customer "customerNode/part-00000" ^
--nodes:segment "fixedData/SegmentNode.csv" ^
--relationships:classified_as "classifiedasEdge/part-00000" ^
--nodes:order "orderNode/part-00000" ^
--relationships:buys "buysEdge/part-00000" ^
--nodes:date "fixedData/dateNode.csv" ^
--relationships:in_date "indateEdge/part-00000" ^
--nodes:product "productNode/part-00000" ^
--relationships:contains "containsEdge/part-00000" ^
--nodes:category "fixedData/category_node.csv" ^
--nodes:farmer "farmerNode/part-00000" ^
--relationships:supplied_by "suppliedbyEdge/part-00000" ^
--relationships:price_in_date "priceInDateEdge/part-00000" ^
--relationships:belongs_to "belongstoEdge/part-00000" ^
> "output.txt"
Rem --relationships:feedback "feedbackEdge/part-00000" 


// Databricks notebook source
val basePath = s"dbfs:/autumn_2022/ale314/"
val RDFPath = basePath + "yagoFacts.tsv"

// COMMAND ----------

// MAGIC %md
// MAGIC Turning YAGO's RDF graph into a property graph using GraphX
// MAGIC

// COMMAND ----------

import org.apache.spark.graphx._

// COMMAND ----------

//this function will created a RDD out of my dataset. It creates tuples with subjects and objects in line 3, zips them with indexes at the front. Then, it constructs the Edge RDD out of multiple mappings and joins to get the edge pattern <subject ID, object ID, and predicate>. 

//I've also removed duplicate subject-object relationships. 
def readRdf(sc:org.apache.spark.SparkContext, filename:String) = { 
  val r = sc.textFile(RDFPath).map(_.split("\t"))
  val v = r.map(_(1)).union(r.map(_(3))).distinct.zipWithIndex 
  Graph(v.map(_.swap),  
        r.map(x => (x(1),(x(2),x(3))))
        .join(v)
        .map(x => (x._2._1._2,(x._2._2,x._2._1._1)))
        .join(v)
        .map(x => new Edge(x._2._1._1, x._2._2, x._2._1._2)))
}

// COMMAND ----------

//creating GraphX graph 
val graphobj = readRdf(sc, RDFPath)

// COMMAND ----------

graphobj.cache

// COMMAND ----------

//getting edges from edge RDD in graphobj
val e = graphobj.edges

// COMMAND ----------

// once I cache, I get double the number of partitions for my edge RDD
e.getNumPartitions

// COMMAND ----------

// I have ~12M edges in my dataset of a little over 1GB (1.04M records)
e.count

// COMMAND ----------

//my edges include subject id, object id, predicate
e.take(2).foreach(println(_))

// COMMAND ----------

// filtering edges by the predicate "<isLeaderOf>". there are 18,471 occurrences when we count how many edges have that predicate.
// to filter, I define a case class for my Edge RDD, which takes a source vertex ID, object ID, and a predicate labeled "prop" and a function, which returns all the edges that meet the condition prop == "<isLeaderOf>" (i.e. it is true, we're expecting a Boolean. The == is used to denote reference equality, because it is very likely that some vertices are homonymns and be different objects living in different places in memory).
e.filter{ case Edge(src, dst, prop) => prop == "<isLeaderOf>" }.count

// COMMAND ----------

// storing new dataset in a variable called isLeaderOf. the filter function is a transformation that created a new Edge RDD of type of String. 
val isLeaderOf = e.filter{ case Edge(src, dst, prop) => prop == "<isLeaderOf>" }

// COMMAND ----------

//As suspected, the results return an Edge of type String, which each contain the subject id, an object id and the predicate which is a string. 
isLeaderOf.take(2).foreach(println(_))

// COMMAND ----------

val vertices = graphobj.vertices

// COMMAND ----------

vertices.getNumPartitions

// COMMAND ----------

//each vertex contains its id of type Int on the right, and a string which is the label. 
graphobj.vertices.take(10).foreach(println(_))

// COMMAND ----------

// I have ~4M vertices, and triple the number of edges.
graphobj.vertices.count

// COMMAND ----------

// MAGIC %md
// MAGIC Triangle Count: Looking at graph density or centrality

// COMMAND ----------

// testing speed of TriangleCount based on the partitioning scheme. The default is RandomVertexCut, which evenly distributes the edges and turns out to be the most optimal (see Google Slides) for performance comparisons. 
val g4 = graphobj.partitionBy(PartitionStrategy.EdgePartition2D)

// COMMAND ----------

//TriangleCount takes my graph and count the triangles that goes through each vertex. It returns a VertexRDD of type Int, namely the vertex ID and the count of triangles.
val triangles = graphobj.triangleCount.vertices

// COMMAND ----------

//I collect the triangles, which is an action. I can now inspect my graph for popular nodes. The node with vertex ID 1732608 has a count of 7 triangles. Relative to other vertices, it has more edges connecting it to other nodes. It might be linked to more people, places or facts on Wikipedia.
triangles.collect.take(100).foreach(println(_))

// COMMAND ----------

//Look up vertex by id.  The vertex 1732608 is John C.B. Ehringhaus, the 58th governor of North Carolina. The filter takes a function that is a pair and returns a Boolean.  
graphobj.vertices
        .filter( { case (vId, label) => (vId == 1732608) } )
        .collect

// COMMAND ----------

// MAGIC %md
// MAGIC Triplets: Joining Vertices and Edges Based on Vertex ID
// MAGIC

// COMMAND ----------

// I call on the the triplets method to view my connected vertices and edges in my graph.  
// The return type of triplets() is an RDD of EdgeTriplet[VD,ED], which is a subclass of Edge[ED] that also contains references to the source and destination vertices associated with the edge. VD and ED are type parameter generics. An EdgeTriplet contains an edge attribute (e.g. <wasBornIn>), srcId, srcAttr, dstId, and dstAttr. Here, the edge attribute appears last. I can read the first triplet as subject ID 1 is Leanne Benjamin, she was born in Baaya - a district in Qatar with object ID 665381.

graphobj.triplets.take(10).foreach(println(_))

// COMMAND ----------

// I'm interested in who has won which prize. I can add a Boolean annotation with the condition t.attr=="hasWonPrize" to edges, which will return true if the EdgeTriplet attribute is "hasWonPrize" and false otherwise. Similarly to the triplets methods, I'm expecting mapTriplets to return an RDD of EdgeTriplet with (vertex[Int], vertex[String]) for the subject in the triple, (vertex[Int], vertex[String]) for the object in the triple, but rather than an Edge[String] I've used an anonymous function that took as input an EdgeTriplet and returns and outputs a new Edge type of the tuple(String, Boolean).

graphobj.mapTriplets(t => (t.attr, t.attr=="hasWonPrize")).triplets.collect.take(5).foreach(println(_))


// COMMAND ----------

// What if I only wanted to see the triplets that contain the edge attribute "hasWonPrize"? In this case, I must use the triplets method and filter with a case pattern. 

// Some of the results don't make sense. One reason being that YAGO's RDF graph might be missing some essential edge properties. Another reason for the disconnectedness in some triplets is that when we filtered out by the edge attribute <hasWonPrize> my edge predicate function can also remove the edges going to and from a vertex that aren't <hasWonPrize>, leaving some vertices naked with the edges to explain the relationships between two vertices that seem connected somehow.

val hasWonPrize = graphobj.triplets.filter( { case t => (t.attr == "<hasWonPrize>") }).collect.take(30).foreach(println(_))

// COMMAND ----------

// MAGIC %md
// MAGIC Subgraphs

// COMMAND ----------

// I can use the subgraph operator to restrict my graph to the vertices and in the one edge of interest, in this case <hasWonPrize>.
// This is a better way of filtering my graph by predicate. The subgraph method takes a triplet as parameter. 
val PrizeSubgraph = graphobj.subgraph(epred=(triplet) => (triplet.attr == "<hasWonPrize>"))

// COMMAND ----------

// here, I retained my edge and vertex RDDs. 
val prize_edges = PrizeSubgraph.edges

// COMMAND ----------

val prize_vertices = PrizeSubgraph.vertices

// COMMAND ----------

// my subgraph vertices have a subject id and subject name. 
prize_vertices.take(2).foreach(println(_))

// COMMAND ----------

// The triplets in this subgraph are a lot less noisy than my previous filter function which used a case pattern. 
PrizeSubgraph.triplets.take(20).foreach(println(_))

// COMMAND ----------

// DBTITLE 1,GraphFrames
// MAGIC %md
// MAGIC Mixing Graphs and Relational Queries

// COMMAND ----------

import org.graphframes._

// COMMAND ----------

//Convert GraphX object to GraphFrames
val yagoGF = GraphFrame.fromGraphX(graphobj) 

// COMMAND ----------

yagoGF.cache

// COMMAND ----------

// Search for pairs of vertices with edges in both directions between them. "(a)-[e]->(b)" expresses an edge e from vertex a to vertex b. 
val motifs = yagoGF.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show

// COMMAND ----------

//let's work on the prize branch of the taxonomy
val prizeGF = GraphFrame.fromGraphX(PrizeSubgraph) 

// COMMAND ----------

// Contrary to the entire YAGO ontology, the prizeGF graph does not have any bidirectional edges. That makes sense given you can only have one recipient of the prize in the triple. 
val motifspbidirect = prizeGF.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifspbidirect.show

// COMMAND ----------

// I'm interested in the multiple edges going through a specific vertex. Motif "(a)-[e]->(b); (a)-[e2]->(c)" specifies two edges going out of vertex (a). 
// The first 100 results are all about author L.Sprague de Camp. The edge predicate is the same <created> which gives us a hint that he wrote many literary works. But how many?
// The second edge predicate [e2] is not particularly useful, and connects each of his works to The Clocks of Iraz, which makes sense given that he is also the author of this fantasy novel. 

val multipleedges = yagoGF.find("(a)-[e]->(b); (a)-[e2]->(c)")
multipleedges.show

// COMMAND ----------

// Here I am verifying that there aren't any duplicate vertices for the same person. I can use the vertices method which is available to GraphFrames to filter vertices by the attribute <L._Sprague_de_Camp>. Then, I do a count. 
// I must be careful to use attr in double quotation marks because it is the actual label for the struct field; hence the condition must be enclosed in "". I use single quotation marks for the value I am filtering by, in this case '<L._Sprague_de_Camp>'. I must return the angle brackets <> because it is how the attribute is written in the vertex. 
yagoGF.vertices.filter("attr = '<L._Sprague_de_Camp>'").count()

// COMMAND ----------

// out of curiosity, I was wondering how many vertices contained the edge attribute <created> for creative works. I stored the results in an immutable variable called created. By the way, I prefer immutable because these are records.
val created = yagoGF.edges.filter("attr = '<created>'")
created.count

// COMMAND ----------

// Verifying that I only got the results filtered by the predicate <created>. The results stored in the variable created gives me the author ID, the ID for the creative work, and the predicate <created>.
created.take(5).foreach(println(_))

// COMMAND ----------

// Selecting the subgraph of author L.Sprague de Camp, and filtering the edges by the attribute "created" to get a list of all of his works. The subgraph will become useful for running pageRank, which only takes a graph because it outputs a new graph. 
// The subgraph is of type GraphFrame. This type is restricted to certain methods, and does not allow me to do a select by column name so I must try a different way.
val SpraguedeCamp = yagoGF
  .filterEdges("attr = '<created>'")
  .filterVertices("attr = '<L._Sprague_de_Camp>'")
  .dropIsolatedVertices()

// COMMAND ----------

// To materialize a view of my subgraph, I create a Dataset using the find method to filter on my conditions: edges must be of attribute <created>, and vertices of attribute <L._Sprague_de_Camp>. The output is of a type spark.sql.Row, which is a special Dataset case class that DataFrames is. I can now use a select to project my results in my dataframe, which I store in e2. 
val paths = { yagoGF.find("(a)-[e]->(b)")
  .filter("e.attr = '<created>'") 
  .filter("a.attr = '<L._Sprague_de_Camp>'") }

val e2 = paths.select("e.src", "e.dst", "e.attr")


// COMMAND ----------

// e2 has a count of 152, meaning L.Sprague_de_Camp wrote 152 novels. More precisely, Wikipedia users are aware of 152 of his works.  
e2.count

// COMMAND ----------

// When I print the results, I get the subject ID 1032387 which is the id L.Sprague_de_Camp my GraphX function readRdf gave it, the object ID which in this case is the unique id given to each of his books, and finally the predicate <created>.

e2.take(152).foreach(println(_))

// COMMAND ----------

// The results of e2 aren't super legible. I want to get a list of the books L.Sprague de Camp wrote so I select all the content in my b column in my GraphFrame subgraph.

val o = paths.select("b.*")

// COMMAND ----------

// MAGIC %md
// MAGIC Graph Algorithms

// COMMAND ----------

// Breadth-First-Search which shows promise as a tool taxonomists because you can revisit how you linked different entities or concepts in a taxonomy. It's another way to audit your work. 
// To use BFS, you must really know your data. 
yagoGF.bfs.fromExpr(" attr = '<David_Foster_Wallace>' ").toExpr(" attr = '<Élisabeth_Rappeneau>' ").run()

// COMMAND ----------

// I got nothing.
res31.collect.foreach(println(_))

// COMMAND ----------

// I had no luck using pageRank of Sprague de Camp's 152 novels to see which one was more influential. It's likely because the size of my sample was too small, which prematurely stops pageRank. But I can run it on my Prize subgraph, which is big enough. 
val TopAwarded = prizeGF.pageRank.maxIter(10).run()

// COMMAND ----------

// I've since deleted my Sprague de Camp GF, but I kept this cell to show you what pageRank outputs. It's supposed to output a new graph with weights in its edges. I think the type is array here because I'm using edgeColumns.
Sprague_de_Camp_greatest_works.edgeColumns

// COMMAND ----------

// The vertices of the graph pageRank outputs contains the vertex id, attribute and pageRank score which is the probability that Wikipedia users will click on the page expressed as a value between 0 and 1. 
// Personally, I don't feel the Golden Scarab is a creative work or Wilipedia page that's most likely to be clicked on when Wikipedia users browse prize winners. Sometimes these pages are too old and get more clicks or links. 
TopAwarded.vertices.show()

// COMMAND ----------

// I've also ran stronglyConnectedComponents to see which vertices are strongly connected to one another that removing their association would lose you a key piece of information about the vertex.
val prize = prizeGF.stronglyConnectedComponents.maxIter(10).run()

// COMMAND ----------

// The results show me the vertex id, its attribute and the component it's strongly connected to (which is the id of the lowest vertex it's connected to, likely a parent).The results were not useful owing to the fact that a lot of the vertices in my dataset have few edges. So they're mostly going to be strongly connected to themselves. 
prize.collect.foreach(println(_))

// COMMAND ----------

// as you can see, a lot of the vertices don't have many edges. 1849067 and 1196116 are the outliers; I checked and 1196116 is the vertex id for Le Pont de la chaussée d'Etterbeek in Brussels, a bridge. But the page itself does not have many links. 
display(yagoGF.inDegrees)

// COMMAND ----------

display(yagoGF.outDegrees)

// COMMAND ----------

//Please ignore, these are the results from a previous query I ran to see if I could run BFS on two vertices that made sense together. 

o.take(152).foreach(println(_))

// COMMAND ----------

// MAGIC %md
// MAGIC Playing with Spark SQL

// COMMAND ----------

// To write SparkSQL, I have to create a temp view which I labeled "multipleE"
multipleedges.createOrReplaceTempView("multipleE")

// COMMAND ----------

// Sample list of Sprague de Camp's 152 novels. 
%sql
SELECT * from multipleE
LIMIT 100

// COMMAND ----------

// MAGIC %md
// MAGIC End

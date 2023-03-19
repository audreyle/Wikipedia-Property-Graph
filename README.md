# Wikipedia-Property-Graph

Databricks notebook written in Scala

Parses RDF triples stored in Azure blob and calls Spark GraphX API to transform Wikipedia's ontology into a property graph.

Then, runs breadth-first search, shortest path and other graph algorithms to study relationships.

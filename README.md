# A2: Connected Components in Apache Spark

Problem
Your task in this assignment is to implement efficient end-to-end Apache Spark program for finding connected components. We make several assumptions:

Undirected graph on which we are operating is too large to be represented in the memory of a single compute node.
The graph is represented by a list of edges in the form source target, where source is integer representing source vertex id, target is integer representing target vertex id, and source and target are separated with single space.
Graph has no self-loops (i.e. source = target) and no particular ordering of source, target is assumed.
Instructions
Create and use folder A2 to implement your program.
Your main program, which will be invoked via spark-submit, must be named a2.xxx, where xxx is the language specific extension, e.g. py for Python, scala for Scala, etc.
You can use any language you like as long as it works with Apache Spark. However, if you decide to use a compiled language, e.g. Scala, Java, you must provide a build system and Makefile to invoke it. In the essence, by running make in the main folder we should receive a2 executable, ready for submission with spark-submit.
Your application should be taking one input command line argument: the name of a file or directory containing the graph to analyze. You may assume that input is always correct and input files are passed properly.
Your application should create a folder or file, with output in the form vertex label, where vertex is vertex id, and label is the label of connected component to which vertex belongs. Vertex and label should be separated by a white space.

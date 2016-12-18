# Big data and social networks

## A personal recommender system based on an unweighted graph

We wrote the foundations thanks to "Iterative MapReduce and Counters" a map-reduce algorithm developed by users of Hadoop Wikispaces  since our algorithm works by using a BFS-map reduce approach. For the development we decided to use Java as programming language. We created a Java-Apache-Maven project using Apache Maven (version 3.3.3) and the Java Development Kit version 1.7. We write our project using Eclipse Mars 1 as development environment.

Before starting explain how our algorithm works we also want to improve what BFS means. Breadth-first search is a graph traversal algorithm. The search starts from the root node and the neighboring nodes are visited until there are no more possible nodes to visit. One way of performing the BFS is coloring the nodes and traversing according to the color of the nodes. There are three possible colors for the node - white (unvisited), gray (visited) and black (finished). Before starting all the nodes are colored in white except for the source node that will be colored gray.

To develop this algorithm we need to run the same Mapper and Reducer multiple times (iterative map-reduce).Each iteration can use the previous iteration's output as its input.

"Iterative MapReduce and Counters" contains also a solution for the shortest path problem (the shortest path between two nodes can be defined as the path that has the minimum total weight of the edges along the path. if we don't talk about weighted graph the minimum weight will be just the minimum number of edges). It implements this problem using Dijkstra's algorithm.

The algorithm is designed to make it run on a graph that has only a source node (or as we prefer call it a "starter node"), instead our solution needs to has more than a single start node ( i.e. the whole set of keynodes given in input). Then we modify the graph input's structure in order to be able to run the BFS starting from more than one node (those will be the keynodes). In addition we tried to adapt our input structure according to the majority of graphs we found online which allowed us to use our program in lots of graphs. So we transformed the graph input from the following format:

**ID\<tab>NEIGHBORS|DISTANCE_FROM_SOURCE|COLOR** to a more common format: **ID\<tab>NEIGHBORS**.

As we can see, we haven't get colors and BFS stuff anymore, our program require just the basic structure of a graph (the node ID with the respective adjacent-node IDs). Most of the graphs found don't show the node with all the other nodes connected to it, but for each edge they write the starting and the destination node, then we developed a little algorithm to "translate" inputs as we required. Our software, to work correctly, needs another input file where will be simply written the keynodes.

Now step by step, assuming that the reader has the basic knowledge on how Hadoop MapReduce works, we are going to explain how our project works and also how it is written. First of all we want to show our program's classes and explain a little bit their functionality, after that we will explain the implementation of the software in its entirety.

### BaseJob.java

Our software doesn't need a complex and structured job, so we decided to keep the Hadoop base job structure and adapt it with just slightly modifications.

The following is an abstract base class for the programs in the graph algorithm tool kit. This class provides an abstraction for setting the various classes associated with a job in a MapReduce program. The base job class contains a setupJob method that takes the job name and a JobInfo object as parameters and returns a job that is used by the driver to execute the MapReduce program. In this method are set the mapper and reducer classes, the number of reducer used for the program (easily editable), the types of the output key and output value of the mapper and reducer.

Finally, into BaseJob is declared the abstract class JobInfo that contains getter methods to get the program-specific classes associated with Job.

### Driver.java

This class contains the driver to execute the job and invoke the map/reduce functions and the main class. The task of the main class is just to create the output directory and call the run method.

The run method contains the driver code and it has the task of managing the entire algorithm's process. Run method, for each iteration, must perform the following tasks: checking if the process has reached the end by monitoring the iteration and if it arrived at a convergent point; getting the job configuration; setting the input/output file and their path; waiting until the job finish his work, then repeat the process again.

Driver contains other two classes, MapperCaller and ReduceCaller, which are responsible for the calling of the mapper and reducer super classes.

MapperCaller extends the MapperWorker class. MapperCaller contains a map method who takes as parameters a key (node ID), a value (node information) and the context. This method is in charge to take the node with the right parameter's value by initializing and instantiating the Node variables (Node inNode = new Node (value.toString())), then calling the map method of the super class passing the keynode, input value, Context object and the node (just created) as parameters.

ReducerCaller extends instead the ReducerWorker. ReduceCaller contains a reduce method who takes as parameters the same parameters than the map method written above. Reduce method is in charge to initializing and instantiating an empty node , then calling the reduce method of the super class passing the keynode, input value, Context object and the node (just created) as parameters.

### Node.java

Node class contains the information about the node ID, the list of adjacent nodes, the distance from the source, the color of the node (color could be white for unvisited node, gray for visited node and black for the node which has been visited by all the keynodes) ,the parent node and the starter list(list of the keynodes that visited the node).

The node class, at the first iteration, read inputs for setting the nodes as required in a BFS for starting the job. In all the other iterations the node's work is reading and interpreting the node's information by the input files. Node class also contains his getter and setter method as well and a method for getting all the node's information appended in a string.

### MapperWorker.java

MapperWorker is the base mapper class for the programs that use parallel breadth-first search algorithm. In this class are implemented the map method which is responsible for the mapper work.

The method takes the keynode, the input value, the Context object and Node (input node) as parameters. The map function, for each gray node, emit each of the adjacent nodes as a new node and set them. Once finished the map function writes the results (the news nodes with their setting) on the context in order to pass them at the reducer.

### ReducerWorker.java

ReducerWorker is the base reducer class for the programs that use parallel breadth-first search algorithm. It combines the information for a single node. The complete list of adjacent nodes, the minimum distance from the source, the darkest color, the parent node of the node that is being processed , and the starter list are determined in the reducer step. This information is emitted from the reducer function to the output file.

After gave you a brief introduction and a general overview about software's classes and their works, we want to follow our algorithm step by step in order to explain how it works, explaining some steps in detail.

At the beginning(first iteration) the program has to prepare input for the mapper class. So the class node read the two inputs. For each node the algorithm check if the node processed is a keynode. If it is, the node will be set as a source node.

```java
// setting a node as starter if it is present in the keywords
if (keywords.contains(this.id)) {
  // setting the source node given in the list
  this.distance = 0;
  this.color = Color.valueOf("GRAY");
  this.parent = "source";
  this.starters.add(this.id);
}
```

If it's not a keynode, the processed node will got the setting as a "standard" node.
 
```java
else {
  // setting the distance of the node
  this.distance = Integer.MAX_VALUE;
  // setting the color of the node
  this.color = Color.valueOf("WHITE");
  // setting the parent of the node
  this.parent = null;
}
```

In the next iterations, inputs will be specifically written in such a way that we don't need to translate it anymore. In the second step each node is passed by the MapperCaller class to the mapper subclass ( MapperWorker).

Mapper worker processed every gray node(called inNode), and, create (as typically in a mapper process) for each adjacent node (of the node under consideration) a new empty node(called adjacentNode) with the adjacent ID. The method also provide to setting the following adjacentNode information: the color: the color will be gray; the parent: the parent of the adjacentNode will be the inNode ID; the distance: the distance will be setting increasing the inNode distance (adjacentNode.setDistance(inNode.getDistance() + 1)); The starter list: it will be the same than inNode node (keynodes that can get closer to the parent are also able to touch the nearby node). The adjacentNode will be then written on the context, ready for be passed at the reducer.

The last work for the Mapper is to check if inNode has been visited by every keynodes, then color it black. A black node will never be visited in the whole process of the algorithm. InNode will be passed at the reducer as well. The reducer (ReducerWorked) get all the results given by the mapper (adjacentNodes and inNodes) and combines the information for a single node. In other words, when there are multiple values associated with the same node ID, only one will contain the information and will be written to the output. We think that showing the reducer code is the best way to explain how the reducer method meets those demands.

```java
for (Text value : values) {

  Node inNode = new Node(key.toString() + "\t" + value.toString());

  // One (and only one) copy of the node will be the fully expanded version,
  // which includes the list of adjacent nodes, in other cases,
  // the mapper emits the ndoes with no adjacent nodes
  // In other words, when there are multiple values associated with the key (node Id),
  // only one will 
  if (inNode.getEdges().size() > 0) {
    outNode.setEdges(inNode.getEdges());
  }
				
  // Save the minimum distance
  if (inNode.getDistance() < outNode.getDistance()) {
    outNode.setDistance(inNode.getDistance());
    // if the distance gets updated then the predecessor node
    // that was responsible for this distance will be the parent node
    outNode.setParent(inNode.getParent());
  }

  // Save the darkest color
  if (inNode.getColor().ordinal() > outNode.getColor().ordinal()) {				
    outNode.setColor(inNode.getColor());		
  }
			
  // save the starterlist (keywords)
  if (inNode.getStarters().size() > 0) {
    List<String> startertemp=new ArrayList<String>();
    //concatenate the two result List 
    startertemp.addAll(inNode.getStarters());
    startertemp.addAll(outNode.getStarters());
    // clean and sort list for duplicates
    Collections.sort(startertemp);
    Set setPmpListArticle=new HashSet(startertemp);
    outNode.setStarters(new ArrayList <String>(setPmpListArticle));
  }
			
  // this node is visited from every key, color is black
  if (Node.numbstarters==outNode.getStarters().size()){
    outNode.setColor(Node.Color.BLACK);
  }							
}
```

The comments over the code efficiently explain how the reducer works. So we would like to focus only on how the reducer gets chooses and merges the starter list for each node. The reducer combines all the starter lists with the same node ID to hold on all the keynodes that have visited the processed node. Doing this, the final list could contains duplication. For this reason the reducer method should clean the list of duplicates. Finally the list will be sorted to avoid convergences issue and write it on the output.

Reduce() at the end check if the output node needs to be visited other times and, if not, change the node's color from gray to black and save it to the context as well.

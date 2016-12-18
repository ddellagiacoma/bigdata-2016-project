package org.apache.hadoop.BigDataProject;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;


// the type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class MapperWorker extends Mapper<Object, Text, Text, Text> {

	//the parameters are the types of the input key, input value and the Context object through which the Mapper communicates with the Hadoop framework
	public void map(Object key, Text value, Context context, Node inNode)
			throws IOException, InterruptedException {

		
	
		// For each GRAY node, emit each of the adjacent nodes as a new node
		//if the adjacent node is already processed and colored BLACK, the reducer retains the color BLACK 
		if (inNode.getColor() == Node.Color.GRAY) {
			
			for (String neighbor : inNode.getEdges()) { // for all the adjacent nodes of the gray node
												
				Node adjacentNode = new Node(); // create a new node

				adjacentNode.setId(neighbor); // set the id of the node
				
				adjacentNode.setDistance(inNode.getDistance() + 1); // set the distance of the node, the distance of the adjacentNode is set to be the distance				
				//of its predecessor node+ 1, this is done since we consider a graph of unit edge weights
				
				 // set the color of the node
				adjacentNode.setColor(inNode.getColor());
				
				//set the parent of the node
				adjacentNode.setParent(inNode.getId());
				
				// set the keywords of the node which visited the parent node
				if(inNode.getStarters().size()>0){
				
					adjacentNode.setStarters(inNode.getStarters());
				
				}
								
				
				//emit the information about the adjacent node in the form of key-value pair where the key is the node Id and the value is the associated information
				//for the nodes emitted here, the list of adjacent nodes will be empty in the value part, the reducer will merge this with the original list of adjacent nodes associated with a node
				context.write(new Text(adjacentNode.getId()), adjacentNode.getNodeInfo());

			}
			

		}

		// No matter what, emit the input node
		// If the node came into this method GRAY, it will be output as BLACK
		//otherwise, the nodes that are already colored BLACK or WHITE are emitted by setting the key as the node Id and the value as the node's information
		context.write(new Text(inNode.getId()), inNode.getNodeInfo());

	}
}

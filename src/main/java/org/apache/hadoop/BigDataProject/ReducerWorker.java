package org.apache.hadoop.BigDataProject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


	// the type parameters are the input keys type, the input values type, the
	// output keys type, the output values type

public class ReducerWorker extends Reducer<Text, Text, Text, Text> {

	//the parameters are the types of the input key, the values associated with the key, the Context object through which the Reducer communicates with the Hadoop framework and the node whose information has to be output
    //the return type is a Node
	public Node reduce(Text key, Iterable<Text> values, Context context, Node outNode)
			throws IOException, InterruptedException {
		  
		//set the node id as the key
		outNode.setId(key.toString());
			
		//since the values are of the type Iterable, iterate through the values associated with the key
		//for all the values corresponding to a particular node id
	
		for (Text value : values) {

			Node inNode = new Node(key.toString() + "\t" + value.toString());


			// One (and only one) copy of the node will be the fully expanded version, which includes the list of adjacent nodes, in other cases, the mapper emits the ndoes with no adjacent nodes
			//In other words, when there are multiple values associated with the key (node Id), only one will 
			if (inNode.getEdges().size() > 0) {
				outNode.setEdges(inNode.getEdges());
			}
				
			// Save the minimum distance
			if (inNode.getDistance() < outNode.getDistance()) {
				outNode.setDistance(inNode.getDistance());
				//if the distance gets updated then the predecessor node that was responsible for this distance will be the parent node
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
			
			// this node is visited from every key, color it black
			if (Node.numbstarters==outNode.getStarters().size()){
		
				outNode.setColor(Node.Color.BLACK);}					
		
		}
		
		//emit the key, value pair where the key is the node id and the value is the node's information
		context.write(key, new Text(outNode.getNodeInfo()));		
						
		return outNode;
		
	}
}
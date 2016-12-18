package org.apache.hadoop.BigDataProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import org.apache.hadoop.io.Text;

public class Node {

	// three possible colors a node can have (to keep track of the visiting
	// status of the nodes during graph search)

	public static enum Color {
		WHITE, GRAY, BLACK
	};

	private String id; // id of the node
	private int distance; // distance of the node from the source
	public static int numbstarters; //number of starter node (keywords)
	private List<String> edges = new ArrayList<String>(); // list of edges
	private Color color = Color.WHITE;
	private String parent; // parent/predecessor of the node : The parent of the source is marked "source" to leave it unchanged
	private List<String> starters = new ArrayList<String>(); //list of keywords that reached the node

	public Node() {

		distance = Integer.MAX_VALUE;
		color = Color.WHITE;
		parent = null;
	}

	// constructor
	// the parameter nodeInfo is the line that is passed from the input, this
	// nodeInfo is then split into key, value pair where the key is the node id
	// and the value is the information associated with the node
	public Node(String nodeInfo) throws FileNotFoundException {

		String[] inputLine = nodeInfo.split("\t"); // splitting the input line
													// record by tab delimiter
													// into key and value
		String key = "", value = ""; // initializing the strings 'key' and
										// 'value'

		try {
			key = inputLine[0]; // node id
			value = inputLine[1]; // the list of adjacent nodes, distance, color, parent

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		String[] tokens = value.split("\\|"); // split the value into tokens
												// where
		// tokens[0] = list of adjacent nodes, tokens[1]= distance, tokens[2]=
		// color, tokens[3]= parent

		this.id = key; // set the id of the node

		// setting the edges of the node
		for (String s : tokens[0].split(",")) {
			if (s.length() > 0) {
				edges.add(s);
			}
		}

		//if read the input for the first time
		if (tokens.length == 1) {
			List<String> keywords = new ArrayList<String>();
	
			// read the second input file and saving the starter node (keywords) in the list
			Scanner fileScanner = new Scanner(new File("keynodes"));
			while (fileScanner.hasNext()) {
				keywords.add(fileScanner.next());
			}
			fileScanner.close();
			numbstarters=keywords.size();
			
			// setting a node as starter if it is present in the keywords
			if (keywords.contains(this.id)) {
				// setting the source node given in the list
				this.distance = 0;
				this.color = Color.valueOf("GRAY");
				this.parent = "source";
				this.starters.add(this.id);

			} else {
				// setting the distance of the node
				this.distance = Integer.MAX_VALUE;
				// setting the color of the node
				this.color = Color.valueOf("WHITE");
				// setting the parent of the node
				this.parent = null;

			}

			
			
		} else {
			// for the iteration >=2
			
			// setting the distance of the node
			if (tokens[1].equals("Integer.MAX_VALUE")) {
				this.distance = Integer.MAX_VALUE;
			} else {
				this.distance = Integer.parseInt(tokens[1]);
			}

			// setting the color of the node
			this.color = Color.valueOf(tokens[2]);

			// setting the parent of the node
			this.parent = tokens[3];

			//setting the keywords of the node
			if (tokens.length == 5) {
				for (String s : tokens[4].split(",")) {
					starters.add(s);
				}
			}

		}

	}

	// this method appends the list of adjacent nodes, the distance , the color
	// and the parent and returns all these information as a single Text
	public Text getNodeInfo() {
		StringBuffer s = new StringBuffer();

		// forms the list of adjacent nodes by separating them using ','
		try {
			for (String v : edges) {
				s.append(v).append(",");
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
			System.exit(1);
		}

		// after the list of edges, append '|'
		s.append("|");

		// append the minimum distance between the current distance and
		// Integer.Max_VALUE
		if (this.distance < Integer.MAX_VALUE) {
			s.append(this.distance).append("|");
		} else {
			s.append("Integer.MAX_VALUE").append("|");
		}

		// append the color of the node
		s.append(color.toString()).append("|");

		// append the parent of the node
		s.append(getParent()).append("|");

		//append the keywords of the node
		try {
			for (String v : starters) {
				s.append(v).append(",");
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
			System.exit(1);
		}

		return new Text(s.toString());
	}

	// getter and setter methods
	public String getId() {
		return this.id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public int getDistance() {
		return this.distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public Color getColor() {
		return this.color;
	}

	public void setColor(Color color) {
		this.color = color;
	}

	public List<String> getEdges() {
		return this.edges;
	}

	public void setEdges(List<String> edges) {
		this.edges = edges;
	}
	
	public List<String> getStarters() {
		return this.starters;
	}

	public void setStarters(List<String> starters) {
		this.starters = starters;
	}

	public String getParent() {
		return parent;
	}
	
	public void setParent(String parent) {
		this.parent = parent;
	}
	

}
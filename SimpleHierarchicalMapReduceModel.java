import java.util.ArrayList;
import java.util.HashMap;


public class SimpleHierarchicalMapReduceModel {

	//Stored transit nodes
	protected static HashMap<Double, Node> nodeMap = new HashMap<Double, Node>();
	
	//Stores leaf nodes
	protected static ArrayList<Node> leafArray = new ArrayList<Node>();
	
	//Indicates the number of total communications
	protected static int communicationNum =0;


	public SimpleHierarchicalMapReduceModel(int depth, int mapedDataNum) {
		//Generates balanced tree
		new Node(0,Integer.MAX_VALUE,depth,1);

		//For Debug
		//printAllRange();

		//Executes shuffle operations for all nodes' mapedData
		for(Node n: leafArray){
			//Creates mapedData
			ArrayList<Integer> mapedData = new ArrayList<Integer>();
			for(int i =0;i<mapedDataNum;i++)mapedData.add((int)(Math.random()*Integer.MAX_VALUE));

			//Activates
			n.shuffle(mapedData);
		}
		
		System.out.println("ToalCommunicationNum::"+communicationNum);
	}

	protected void  printAllRange(){
		for(Node n: leafArray){
			n.printRange();
		}
	}
	
	private class Node {
		private int left;
		private int right;
		protected double parent;
		private double id;
		private int depth;
		Node r;
		Node l;

		public Node(int left, int right, int depth, double parent) {
			this.left = left;
			this.right = right;
			this.id = Math.random();
			this.parent = parent;
			this.depth = depth;

			if (depth > 0) {
				r = new Node(left + ( right- left) / 2, right, depth - 1,this.id);
				l = new Node(left, left + (right - left) / 2, depth - 1,this.id);
				nodeMap.put(this.id, this);
			} else {
					leafArray.add(this);
			}
		}
		
		//Prints own range
		protected void printRange(){
			System.out.println(left+" "+right);
		}

		protected void put(int key, int value){
			//Increments communicationNum
			SimpleHierarchicalMapReduceModel.communicationNum++;

			if(left <= key && key <= right){
				if(depth ==0){
					//Finishes searching
				}else{
					//Sends a value for a child node which covers key's range.
					if(left <= key && key <= left+(right - left)/2){
						l.put(key, value);
					}else{
						r.put(key, value);
					}

				}
			}else{
				Node p = nodeMap.get(parent);
				p.put(key, value);
			}
		}

		protected void shuffle(ArrayList<Integer> mapedData){
			for(int i:mapedData)put(i,1);
		}
	}

	public static void main(String[] args) {
		new SimpleHierarchicalMapReduceModel(7,10000);
	}

}

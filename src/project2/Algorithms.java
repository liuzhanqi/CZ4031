/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2. This set of code shall NOT be redistributed.
 * You should provide implementation for the three algorithms declared in this class.  
 */

package project2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import project2.Relation.RelationLoader;
import project2.Relation.RelationWriter;

public class Algorithms {
	
	private static Block[] sortSubLists(Block[] blockList) {
		ArrayList<Tuple> tupleList = new ArrayList<>();
		int blockListActualSize = 0;
		for (Block b : blockList) {
			if (b==null) break;
			blockListActualSize++;
			for (Tuple t : b.tupleLst) {
				tupleList.add(t);
			}
		}
		Collections.sort(tupleList, new Comparator<Tuple>() {
			public int compare(Tuple t1, Tuple t2) {
				return t1.key - t2.key;
			}
		});
		
		Block[] sortedBlocks = new Block[blockListActualSize];
		
		int F = Setting.blockFactor;
		int numBlocks = (int) Math.ceil(1.0*tupleList.size()/F);
		for (int i=0; i<numBlocks; i++) {
			int low = i*F;
			int high = (i+1)*F;
			if (high>tupleList.size()) high=tupleList.size();
			Block b = new Block();
			for (int j=low; j<high; j++) b.insertTuple(tupleList.get(j));
			sortedBlocks[i]=b;
		}
		
		return sortedBlocks;
	}
	
	/**
	 * Sort the relation using Setting.memorySize buffers of memory 
	 * @param rel is the relation to be sorted. 
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 * @throws Exception 
	 */
	public static int mergeSortRelation(Relation rel){
		int numIO=0;
		
		int M = Setting.memorySize;
		int F = Setting.blockFactor;
		
		//phase 1: produce at most M-1 sorted sublists
		final class InMemoryArrayList<T> extends ArrayList<T> {
			  @Override
			  public boolean add(T e) {
			      if (this.size() < Setting.memorySize) {
			          return super.add(e);
			      }
			      return false;
			  }
			}
		
		RelationLoader relLoader = rel.getRelationLoader();
		InMemoryArrayList<Relation> sortedSublists = new InMemoryArrayList<>();
		numIO += rel.getNumBlocks();
		while (relLoader.hasNextBlock()) {
			Relation sortedSublist = new Relation("sortedSublists");
			RelationWriter sortedSublistsWriter = sortedSublist.getRelationWriter();
			Block[] blocklist = relLoader.loadNextBlocks(M);
			blocklist = sortSubLists(blocklist);
			for (Block b : blocklist) {
				sortedSublistsWriter.writeBlock(b);
				numIO += 1;
			}
			boolean added = sortedSublists.add(sortedSublist);
			if (!added) {
				System.out.println("Error: Number of Sublists exceeds M-1");
				return -1;
			}
		}
		
		System.out.println("Number of sorted sublists = " + sortedSublists.size());
		
		//phase 2: merge sorted sublists
		//pair the relation loader with block so that it is easier to get the next block
		HashMap<Block,RelationLoader> inputBuffers = new HashMap<>();
		
		//load the first block of each sorted sublists into the input buffers
		for (Relation list : sortedSublists) {
			RelationLoader listLoader = list.getRelationLoader();
			Block[] blocks = listLoader.loadNextBlocks(1);
			numIO += 1;
			inputBuffers.put(blocks[0],listLoader);
		}
		
		System.out.println("size of input buffers = " + inputBuffers.entrySet().size());
		
		//merge blocks in input buffer to outRel
		//use a priority queue to get the smallest tuple each time
		final class TupleItem {
			Tuple tuple;
			int index;
			Block block;

			public TupleItem(Tuple t, int i, Block b) {
				this.tuple=t; this.index=i; this.block=b;
			}
		}

		PriorityQueue<TupleItem> tupleQueue = new PriorityQueue<TupleItem>(
			new Comparator<TupleItem>() {
				public int compare (TupleItem t1, TupleItem t2) {
					return t1.tuple.key - t2.tuple.key;
				}
			});

		//add the first tuples of each block in input buffer to the priority queue
		for (Block block: inputBuffers.keySet()) {
			Tuple tuple = block.tupleLst.get(0);
			TupleItem tupleItem = new TupleItem(tuple, 0, block);
			tupleQueue.add(tupleItem);
		}

		Block outputBuffer = new Block();
		Relation outRel = new Relation("outRel");
		RelationWriter outWriter = outRel.getRelationWriter();

		while (!tupleQueue.isEmpty()) {
			//get the smallest tuple in priority queue and insert it to output buffer
			TupleItem tupleItem = tupleQueue.poll();
			outputBuffer.insertTuple(tupleItem.tuple);
			//insert the next tuple into priority queue
			//check if this is the last tuple in the block
			if (tupleItem.index == tupleItem.block.getNumTuples() - 1) {
				//load a new block into input buffer
				RelationLoader loader = inputBuffers.get(tupleItem.block);
				if (loader.hasNextBlock()) {
					Block newBlock = loader.loadNextBlocks(1)[0];
					numIO += 1;
					inputBuffers.put(newBlock, loader);
					tupleQueue.add(new TupleItem(newBlock.tupleLst.get(0), 0, newBlock));
				}
			} else { //this is not the last tuple in the block
				Tuple newTuple = tupleItem.block.tupleLst.get(tupleItem.index + 1);
				tupleQueue.add(new TupleItem(newTuple, tupleItem.index + 1, tupleItem.block));
			}

			//check if output buffer is full
			if (outputBuffer.getNumTuples() == F) {
				//write the output buffer in outRel
				outWriter.writeBlock(outputBuffer);
				outputBuffer = new Block();
			}
		}
		
		//write to remaining blocks in output buffer to outrel
		outWriter.writeBlock(outputBuffer);
		
		//System.out.println("---------Printing relations----------");
		outRel.printRelation(true, true);
		
		return numIO;
	}
	
	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory to produce the result relation relRS
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int hashJoinRelations(Relation relR, Relation relS, Relation relRS){
		int numIO=0;
		
		int M = Setting.memorySize;
		if(M<2){
			System.out.printf("The memory size (%d) is too small \n",M);
			return -1; //return a negative value indicating e
		}

		//initialize M-1 buckets using M-1 empty buffers
		ArrayList<Block> outBuffer = new ArrayList<Block>();
		for(int i = 0; i< (M-1);i++){
			outBuffer.add(new Block());
		}
		Block inputBuffer = new Block();



		// Hash R ----------------------------------------------------------------------

		RelationLoader R = relR.getRelationLoader();

        //initiate bucket storing R
        ArrayList<ArrayList<Block>> bucketR = new ArrayList<ArrayList<Block>>();
        for(int i=0;i<M-1;i++){
            bucketR.add(new ArrayList<Block>());
        }

        //blockInBucketR an array of integer tracking number of blocks in each bucket storing R
        int[] blockInBucketR = new int[M-1];


		//Load R
		while(R.hasNextBlock()){
			numIO++;
			inputBuffer = R.loadNextBlocks(1)[0];
			for(Tuple tR: inputBuffer.tupleLst){
				int h = tR.key%(M-1);

				//add to buckets if the output buffer is full
				if(outBuffer.get(h).getNumTuples()==Setting.blockFactor){

					//check number of tuples in buffer
					if(blockInBucketR[h]<(M-1)){
						//copy the buffer to disk
						bucketR.get(h).add(blockInBucketR[h], outBuffer.get(h));
						numIO++;
                        blockInBucketR[h]++;
						//initial a new empty buffer
                        outBuffer.set(h, new Block());
						outBuffer.get(h).insertTuple(tR);

					} else {
						System.out.printf("Relation R has exceeded memory size!");
						return -1;
					}
				}else{
					outBuffer.get(h).insertTuple(tR);
				}
			}
		}

		//load the remain part in buffer to disk
		for(int i=0;i<(M-1);i++){
			//if the buffer for this bucket is not empty
			if(outBuffer.get(i).getNumTuples()!=0){
				if(blockInBucketR[i]<(M-1)){
					bucketR.get(i).add(blockInBucketR[i], outBuffer.get(i));
					numIO++;
                    blockInBucketR[i]++;
				}else{
					System.out.printf("Relation R has exceeded memory size!");
                    return -1;
				}
			}
		}


		//Hash S -----------------------------------------------------------

		for(int i = 0; i< (M-1);i++){
			outBuffer.set(i,new Block());
		}
		RelationLoader S = relS.getRelationLoader();
        //initiate bucket storing S
        ArrayList<ArrayList<Block>> bucketS = new ArrayList<ArrayList<Block>>();
        for(int i=0;i<M-1;i++){
            bucketS.add(new ArrayList<Block>());
        }

        //blockInBucketR an array of integer tracking number of blocks in each bucket storing R
        int[] blockInBucketS = new int[M-1];

        //Load S
        while(S.hasNextBlock()){
            numIO++;
            inputBuffer = S.loadNextBlocks(1)[0];
            for(Tuple tS: inputBuffer.tupleLst) {

                int h = tS.key%(M-1);

                //add to buckets if the output buffer is full
                if(outBuffer.get(h).getNumTuples()==Setting.blockFactor){

                    //check number of tuples in buffer
                    if(blockInBucketS[h]<(M-1)){
                        //copy the buffer to disk
                        bucketS.get(h).add(blockInBucketS[h],outBuffer.get(h));
                        numIO++;
                        blockInBucketS[h]++;
                        //initial a new empty buffer
                        outBuffer.set(h, new Block());
                        outBuffer.get(h).insertTuple(tS);

                    } else {
                        System.out.printf("Relation S has exceeded memory size!");
                        return -1;
                    }
                } else {
                    outBuffer.get(h).insertTuple(tS);
                }
            }
        }

        //load the remain part in buffer to disk
        for(int i=0;i<(M-1);i++){
            //if the buffer for this bucket is not empty
            if(outBuffer.get(i).getNumTuples()!=0){
                if(blockInBucketS[i]<(M-1)){
                    bucketS.get(i).add(blockInBucketS[i],outBuffer.get(i));
                    numIO++;
                    blockInBucketS[i]++;
                }else{
                    System.out.printf("Relation S has exceeded memory size!");
                    return -1;
                }
            }
        }


        //Join Phase --------------------------------------------------------------------
        int BucketIdx=0;
        RelationWriter RSWriter=relRS.getRelationWriter();
        Block RSBlock = new Block();

        //read in pairs of bucket Ri, Si
        while (BucketIdx<(M-1)){
            //read in blocks in each bucket
            numIO+=blockInBucketS[BucketIdx];

            for (int idxOfR=0;idxOfR<blockInBucketR[BucketIdx];idxOfR++){

                //for each block in R search for matches and join them
                Block BufferR = bucketR.get(BucketIdx).get(idxOfR);
                numIO++;
                for (Tuple tupleR:BufferR.tupleLst){
                    for (int idxOfS = 0 ; idxOfS< blockInBucketS[BucketIdx];idxOfS++){
                        for (Tuple tupleS:bucketS.get(BucketIdx).get(idxOfS).tupleLst){
                            if (tupleS.key==tupleR.key){
                                JointTuple jTuple = new JointTuple(tupleR,tupleS);
                                if (RSBlock.getNumTuples()==Setting.blockFactor){
                                    RSWriter.writeBlock(RSBlock);
                                    RSBlock = new Block();
                                    RSBlock.insertTuple(jTuple);
                                }else{
									RSBlock.insertTuple(jTuple);
								}
                            }
                        }
                    }
                }
            }
            BucketIdx++;
        }
        if (RSBlock.getNumTuples()!=0) {
            RSWriter.writeBlock(RSBlock);

        }
		
		return numIO;
	}
	
	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory to produce the result relation relRS
	 * @param relS is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	
	public static int refinedSortMergeJoinRelations(Relation relR, Relation relS, Relation relRS){
		int numIO=0;
		
		int M = Setting.memorySize;
		int F = Setting.blockFactor;
		
		if (M < 1) {
			System.out.println("Memory size is too small");
			return -1;
		}
		
		final class InMemoryArrayList<T> extends ArrayList<T> {
			  @Override
			  public boolean add(T e) {
			      if (this.size() < Setting.memorySize) {
			          return super.add(e);
			      }
			      return false;
			  }
			}
		
		//Phase 1: Produce sorted sublist of R and S
		//R
		RelationLoader relLoaderR = relR.getRelationLoader();
		InMemoryArrayList<Relation> sortedSublistsR = new InMemoryArrayList<>();
		numIO += relR.getNumBlocks();
		while (relLoaderR.hasNextBlock()) {
			Relation sortedSublistR = new Relation("sortedSublistR");
			RelationWriter sortedSublistsWriterR = sortedSublistR.getRelationWriter();
			Block[] blocklist = relLoaderR.loadNextBlocks(M);
			blocklist = sortSubLists(blocklist);
			for (Block b : blocklist) {
				sortedSublistsWriterR.writeBlock(b);
				numIO += 1;
			}
			boolean added = sortedSublistsR.add(sortedSublistR);
			if (!added) {
				System.out.println("Error: Number of Sublists exceeds M-1");
				return -1;
			}
		}
		
		//S
		RelationLoader relLoaderS = relS.getRelationLoader();
		InMemoryArrayList<Relation> sortedSublistsS = new InMemoryArrayList<>();
		numIO += relS.getNumBlocks();
		while (relLoaderS.hasNextBlock()) {
			Relation sortedSublistS = new Relation("sortedSublistS");
			RelationWriter sortedSublistsWriterS = sortedSublistS.getRelationWriter();
			Block[] blocklist = relLoaderS.loadNextBlocks(M);
			blocklist = sortSubLists(blocklist);
			for (Block b : blocklist) {
				sortedSublistsWriterS.writeBlock(b);
				numIO += 1;
			}
			boolean added = sortedSublistsS.add(sortedSublistS);
			if (!added) {
				System.out.println("Error: Number of Sublists exceeds M-1");
				return -1;
			}
		}
		
		int numOfSublistsR = sortedSublistsR.size();
		int numOfSublistsS = sortedSublistsS.size();
		
		if (numOfSublistsR + numOfSublistsS > M-1) {
            System.out.println("Error: Number of Sublists exceeds M-1");
            return -1;
		}
		
		//Phase 2: Merge and join sorted sublists
		//Pair the relation loader with block so that it is easier to get the next block
		LinkedHashMap<Block,RelationLoader> inputBuffersR = new LinkedHashMap<>();
		LinkedHashMap<Block,RelationLoader> inputBuffersS = new LinkedHashMap<>();
		
		//load the first block of each sorted sublists into the input buffers
		for (Relation list : sortedSublistsR) {
			RelationLoader listLoader = list.getRelationLoader();
			Block[] blocks = listLoader.loadNextBlocks(1);
			numIO += 1;
			inputBuffersR.put(blocks[0],listLoader);
		}
		
		for (Relation list : sortedSublistsS) {
			RelationLoader listLoader = list.getRelationLoader();
			Block[] blocks = listLoader.loadNextBlocks(1);
			numIO += 1;
			inputBuffersS.put(blocks[0],listLoader);
		}
		
		//merge blocks in input buffer to outRel
		//use a priority queue to get the smallest tuple each time
		final class TupleItem {
			Tuple tuple;
			int index;
			Block block;

			public TupleItem(Tuple t, int i, Block b) {
				this.tuple=t; this.index=i; this.block=b;
			}
		}

		PriorityQueue<TupleItem> tupleQueueR = new PriorityQueue<TupleItem>(
			new Comparator<TupleItem>() {
				public int compare (TupleItem t1, TupleItem t2) {
					return t1.tuple.key - t2.tuple.key;
				}
			});
		
		PriorityQueue<TupleItem> tupleQueueS = new PriorityQueue<TupleItem>(
				new Comparator<TupleItem>() {
					public int compare (TupleItem t1, TupleItem t2) {
						return t1.tuple.key - t2.tuple.key;
					}
				});

		
		//add the first tuples of each block in input buffer to the priority queue
		for (Block block: inputBuffersR.keySet()) {
			Tuple tuple = block.tupleLst.get(0);
			TupleItem tupleItem = new TupleItem(tuple, 0, block);
			tupleQueueR.add(tupleItem);
		}
		
		for (Block block: inputBuffersS.keySet()) {
			Tuple tuple = block.tupleLst.get(0);
			TupleItem tupleItem = new TupleItem(tuple, 0, block);
			tupleQueueS.add(tupleItem);
		}

		Block outputBuffer = new Block();
		RelationWriter outWriter = relRS.getRelationWriter();

		TupleItem checkItemR;
		TupleItem checkItemS;
		TupleItem tupleItemR;
		TupleItem tupleItemS;
		
		while (true) {
			//get the smallest tuple in priority queue and insert it to output buffer
			checkItemR = tupleQueueR.peek();
			checkItemS = tupleQueueS.peek();
			
			if (checkItemR == null || checkItemS == null)
				break;
			
			if (checkItemR.tuple.key == checkItemS.tuple.key) {
				List<Tuple> joinListR = new ArrayList<Tuple>();
				List<Tuple> joinListS = new ArrayList<Tuple>();
				int smallestKey = checkItemR.tuple.key;
				
				while (tupleQueueR.peek().tuple.key == smallestKey) {			
					tupleItemR = tupleQueueR.poll();
					joinListR.add(tupleItemR.tuple);
			
					//R: Insert the next tuple into priority queue and check if this is the last tuple in the block
					if (tupleItemR.index == tupleItemR.block.getNumTuples() - 1) {
						RelationLoader loader = inputBuffersR.get(tupleItemR.block);
						if (loader.hasNextBlock()) {
							Block newBlock = loader.loadNextBlocks(1)[0];
							numIO += 1;
							inputBuffersR.put(newBlock, loader);
							tupleQueueR.add(new TupleItem(newBlock.tupleLst.get(0), 0, newBlock));
						}
					} else {
						Tuple newTuple = tupleItemR.block.tupleLst.get(tupleItemR.index + 1);
						tupleQueueR.add(new TupleItem(newTuple, tupleItemR.index + 1, tupleItemR.block));
					}
					//
					
					if (tupleQueueR.isEmpty()) break;
				}
				
				while (tupleQueueS.peek().tuple.key == checkItemR.tuple.key) {	
					tupleItemS = tupleQueueS.poll();
					joinListS.add(tupleItemS.tuple);
					
					//S: Insert the next tuple into priority queue and check if this is the last tuple in the block
					if (tupleItemS.index == tupleItemS.block.getNumTuples() - 1) {
						RelationLoader loader = inputBuffersS.get(tupleItemS.block);
						if (loader.hasNextBlock()) {
							Block newBlock = loader.loadNextBlocks(1)[0];
							numIO += 1;
							inputBuffersS.put(newBlock, loader);
							tupleQueueS.add(new TupleItem(newBlock.tupleLst.get(0), 0, newBlock));
						}
					} else {
						Tuple newTuple = tupleItemS.block.tupleLst.get(tupleItemS.index + 1);
						tupleQueueS.add(new TupleItem(newTuple, tupleItemS.index + 1, tupleItemS.block));
					}
					//
					
					if (tupleQueueS.isEmpty()) break;
				}
				
	            for (int i = 0; i < joinListR.size(); i++) {
	                for (int j = 0; j < joinListS.size(); j++) {
	                	JointTuple tuple = new JointTuple(joinListR.get(i), joinListS.get(j));	
	    				outputBuffer.insertTuple(tuple);
	        			if (outputBuffer.getNumTuples() == F) {
	        				outWriter.writeBlock(outputBuffer);
	        				outputBuffer = new Block();
	        			}
	                }
	            }
			}
			else {
				if (checkItemR.tuple.key < checkItemS.tuple.key) {
					tupleItemR = tupleQueueR.poll();
					//R: Insert the next tuple into priority queue and check if this is the last tuple in the block
					if (tupleItemR.index == tupleItemR.block.getNumTuples() - 1) {
						RelationLoader loader = inputBuffersR.get(tupleItemR.block);
						if (loader.hasNextBlock()) {
							Block newBlock = loader.loadNextBlocks(1)[0];
							numIO += 1;
							inputBuffersR.put(newBlock, loader);
							tupleQueueR.add(new TupleItem(newBlock.tupleLst.get(0), 0, newBlock));
						}
					} else {
						Tuple newTuple = tupleItemR.block.tupleLst.get(tupleItemR.index + 1);
						tupleQueueR.add(new TupleItem(newTuple, tupleItemR.index + 1, tupleItemR.block));
					}
					//
					
					while (true) {
						checkItemR = tupleQueueR.peek();
						if (checkItemR.tuple.key >= checkItemS.tuple.key || checkItemR == null)
							break;
						
						tupleItemR = tupleQueueR.poll();
						//R: Insert the next tuple into priority queue and check if this is the last tuple in the block
						if (tupleItemR.index == tupleItemR.block.getNumTuples() - 1) {
							RelationLoader loader = inputBuffersR.get(tupleItemR.block);
							if (loader.hasNextBlock()) {
								Block newBlock = loader.loadNextBlocks(1)[0];
								numIO += 1;
								inputBuffersR.put(newBlock, loader);
								tupleQueueR.add(new TupleItem(newBlock.tupleLst.get(0), 0, newBlock));
							}
						} else {
							Tuple newTuple = tupleItemR.block.tupleLst.get(tupleItemR.index + 1);
							tupleQueueR.add(new TupleItem(newTuple, tupleItemR.index + 1, tupleItemR.block));
						}
						//	
					}
				}
				else {
					tupleItemS = tupleQueueS.poll();
					//S: Insert the next tuple into priority queue and check if this is the last tuple in the block
					if (tupleItemS.index == tupleItemS.block.getNumTuples() - 1) {
						RelationLoader loader = inputBuffersS.get(tupleItemS.block);
						if (loader.hasNextBlock()) {
							Block newBlock = loader.loadNextBlocks(1)[0];
							numIO += 1;
							inputBuffersS.put(newBlock, loader);
							tupleQueueS.add(new TupleItem(newBlock.tupleLst.get(0), 0, newBlock));
						}
					} else {
						Tuple newTuple = tupleItemS.block.tupleLst.get(tupleItemS.index + 1);
						tupleQueueS.add(new TupleItem(newTuple, tupleItemS.index + 1, tupleItemS.block));
					}
					//
					
					while (true) {
						checkItemS = tupleQueueS.peek();
						if (checkItemS.tuple.key >= checkItemR.tuple.key || checkItemS == null)
							break;
						
						tupleItemS = tupleQueueS.poll();			
						//S: Insert the next tuple into priority queue and check if this is the last tuple in the block
						if (tupleItemS.index == tupleItemS.block.getNumTuples() - 1) {
							RelationLoader loader = inputBuffersS.get(tupleItemS.block);
							if (loader.hasNextBlock()) {
								Block newBlock = loader.loadNextBlocks(1)[0];
								numIO += 1;
								inputBuffersS.put(newBlock, loader);
								tupleQueueS.add(new TupleItem(newBlock.tupleLst.get(0), 0, newBlock));
							}
						} else {
							Tuple newTuple = tupleItemS.block.tupleLst.get(tupleItemS.index + 1);
							tupleQueueS.add(new TupleItem(newTuple, tupleItemS.index + 1, tupleItemS.block));
						}
						//
					}
				}
			}

			//check if output buffer is full
			if (outputBuffer.getNumTuples() == F) {
				//write the output buffer in outRel
				outWriter.writeBlock(outputBuffer);
				outputBuffer = new Block();
			}
		}
		
		//write to remaining blocks in output buffer to outrel
		outWriter.writeBlock(outputBuffer);
		
		//System.out.println("---------Printing relations----------");
		relRS.printRelation(true, true);
		
		return numIO;
	}
	
	
	/**
	 * Example usage of classes. 
	 */
	public static void examples(){

		/*Populate relations*/
		System.out.println("---------Populating two relations----------");
		Relation relR=new Relation("RelR");
		int numTuples=relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains "+numTuples+" tuples.");
		Relation relS=new Relation("RelS");
		numTuples=relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains "+numTuples+" tuples.");
		System.out.println("---------Finish populating relations----------\n\n");
			
		/*Print the relation */
		System.out.println("---------Printing relations----------");
		relR.printRelation(true, true);
		relS.printRelation(true, false);
		System.out.println("---------Finish printing relations----------\n\n");
		
		
		/*Example use of RelationLoader*/
		System.out.println("---------Loading relation RelR using RelationLoader----------");
		RelationLoader rLoader=relR.getRelationLoader();		
		while(rLoader.hasNextBlock()){
			System.out.println("--->Load at most 7 blocks each time into memory...");
			Block[] blocks=rLoader.loadNextBlocks(7);
			//print out loaded blocks 
			for(Block b:blocks){
				if(b!=null) b.print(false);
			}
		}
		System.out.println("---------Finish loading relation RelR----------\n\n");
				
		
		/*Example use of RelationWriter*/
		System.out.println("---------Writing to relation RelS----------");
		RelationWriter sWriter=relS.getRelationWriter();
		rLoader.reset();
		if(rLoader.hasNextBlock()){
			System.out.println("Writing the first 7 blocks from RelR to RelS");
			System.out.println("--------Before writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);
			
			Block[] blocks=rLoader.loadNextBlocks(7);
			for(Block b:blocks){
				if(b!=null) sWriter.writeBlock(b);
			}
			System.out.println("--------After writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);
		}
	}
	
	/**
	 * Testing cases. 
	 */
	public static void testCases(){
	
		//Setting.blockFactor =10;
        //Setting.memorySize =20;
        Relation relR=new Relation("RelR");
        relR.populateRelationFromFile("RelR.txt");

        Relation relS=new Relation("RelS");
        relS.populateRelationFromFile("RelS.txt");

        Relation relRS=new Relation("RelRS");

//        int numIOHash = Algorithms.hashJoinRelations(relR,relS,relRS);

        System.out.printf("----------Hash Join---------");
        relRS.printRelation(false, false);
        relR.printRelation(false, false);
        relS.printRelation(false, false);
//        System.out.printf("Number of IO: %d",numIOHash);
	
	}
	
	/**
	 * This main method provided for testing purpose
	 * @param arg
	 */
	public static void main(String[] arg){
		//Algorithms.examples();
		//Populate relations
		Relation relR=new Relation("RelR");
		int numTuplesR=relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains "+numTuplesR+" tuples.");
		Relation relS=new Relation("RelS");
		int numTuplesS=relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelR contains "+numTuplesS+" tuples.");
		Relation relRS=new Relation("RelRS");
		
		int numIO = refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("---------numIO----------");
		System.out.println(numIO);

		//Yichang
//		Algorithms.testCases();
	}
}

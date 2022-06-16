package dataManager;

import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class UpdateTask implements Runnable {
	private LinkedBlockingQueue<UpdateItemRequest> queue;
	private DynamoDbClient client;
	private final AtomicBoolean running = new AtomicBoolean(true);
	private Vector<Integer> counterVector;

	public UpdateTask(
			LinkedBlockingQueue<UpdateItemRequest> updateQueue,
			DynamoDbClient dynamo, Vector<Integer> counterVector) {
		queue = updateQueue;
		client = dynamo;
		this.counterVector = counterVector;
	}

	public void stop() {
		running.set(false);
	}

	@Override
	public void run() {
		int count = 0;
		while (running.get()) {
			while (!queue.isEmpty()) {
				UpdateItemRequest request = queue.poll();
				if (request != null) {
					client.updateItem(request);
					count++;
				}
			}
		}
		System.out.println("Thread\t" + Thread.currentThread().getId()
				+ "\tstopping after\t" + count + " updates");
		counterVector.add(Integer.valueOf(count));
	}
}

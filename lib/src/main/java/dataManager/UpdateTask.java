package dataManager;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class UpdateTask implements Runnable {
	private LinkedBlockingQueue<UpdateItemRequest> queue;
	private DynamoDbClient client;
	private final AtomicBoolean running = new AtomicBoolean(false);
	public UpdateTask(
			LinkedBlockingQueue<UpdateItemRequest> updateQueue,
			DynamoDbClient dynamo) {
		queue = updateQueue;
		client = dynamo;
	}

	public void stop() {
		running.set(false);
	}

	@Override
	public void run() {
		int count = 0;
		running.set(true);
		while (running.get()) {
			while (!queue.isEmpty()) {
				UpdateItemRequest request = queue.poll();
				if (request != null) {
					client.updateItem(request);
					count++;
				}
			}
		}
		System.out.println("Thread " + Thread.currentThread().getId()
				+ " stopping after " + count + " updates");
	}

}

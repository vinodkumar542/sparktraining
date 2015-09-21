package com.dmac.analytics.spark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class MyOwnCustomReceiver extends Receiver<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public MyOwnCustomReceiver(StorageLevel storageLevel) {
		super(StorageLevel.MEMORY_ONLY());
	}


	

	@Override
	public void onStart() {
		new Thread()  {
		      @Override public void run() {
		        receive();
		      }
		    }.start();
	}

	@Override
	public void onStop() {
		// TODO Auto-generated method stub
		
	}

	 private void receive() {
		    Socket socket = null;
		    String userInput = null;

		    try {
		      // connect to the server
		      socket = new Socket("", 0);

		      BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

		      // Until stopped or connection broken continue reading
		      while (!isStopped() && (userInput = reader.readLine()) != null) {
		        System.out.println("Received data '" + userInput + "'");
		        store(userInput);
		      }
		      reader.close();
		      socket.close();

		      // Restart in an attempt to connect again when server is active again
		      restart("Trying to connect again");
		    } catch(ConnectException ce) {
		      // restart if could not connect to server
		      restart("Could not connect", ce);
		    } catch(Throwable t) {
		      // restart if there is any other error
		      restart("Error receiving data", t);
		    }
		  }
	 
	 
		public static void main(String[] args) {
			// TODO Auto-generated method stub

		}
}

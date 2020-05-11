package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	// Install lock for concurrency
	static Lock lock = new ReentrantLock();
    //static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	//static ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
	//static ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

	public class DynamoDatabase extends SQLiteOpenHelper{
		public static final String DATABASE_NAME = "DynamoDB.db";
		public static final String TABLE_NAME = "KeyValueTable";
		public static final String TABLE_NAME_BUDDY1 = "Buddy1KeyValueTable";
		public static final String TABLE_NAME_BUDDY2 = "Buddy2KeyValueTable";
		public static final String KEY_COL = "key";
		public static final String VALUE_COL = "value";

		public DynamoDatabase(Context context){
			super(context,DATABASE_NAME,null,1);
		}

		@Override
		public synchronized void onCreate(SQLiteDatabase db) {
			db.execSQL("CREATE TABLE " + TABLE_NAME + " (key TEXT UNIQUE ON CONFLICT REPLACE, value TEXT);");

			// Create the Buddy Tables
			db.execSQL("CREATE TABLE " + TABLE_NAME_BUDDY1 + " (key TEXT UNIQUE ON CONFLICT REPLACE, value TEXT);");
			db.execSQL("CREATE TABLE " + TABLE_NAME_BUDDY2 + " (key TEXT UNIQUE ON CONFLICT REPLACE, value TEXT);");
		}

		@Override
		public synchronized void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);

			// Drop the Buddy Tables also
			db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME_BUDDY1);
			db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME_BUDDY2);
		}

		public synchronized boolean insertData(ContentValues values,String buddy){
			SQLiteDatabase db = this.getWritableDatabase();
			long row_id;
            try {
                if (buddy.equals("buddy1")) {
                    row_id = db.insert(TABLE_NAME_BUDDY1, null, values);
                } else if (buddy.equals("buddy2")) {
                    row_id = db.insert(TABLE_NAME_BUDDY2, null, values);
                } else {
                    row_id = db.insert(TABLE_NAME, null, values);
                }
            }finally {

            }


			if(row_id == -1){
				return false;
			}else{
				return true;
			}
		}

		public synchronized Cursor queryDatabase(String[] projection,String selection,String[] selectionArgs,String sortOrder,String buddy){
			SQLiteDatabase db = this.getReadableDatabase();
			SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
			String TABLE_TO_CALL;
			if(buddy.equals("buddy1")){
				TABLE_TO_CALL = TABLE_NAME_BUDDY1;
			}else if (buddy.equals("buddy2")){
				TABLE_TO_CALL = TABLE_NAME_BUDDY2;
			}else{
				TABLE_TO_CALL = TABLE_NAME;
			}

			queryBuilder.setTables(TABLE_TO_CALL);

			Cursor cursor= null;
			try {
                cursor = queryBuilder.query(db, projection, selection, selectionArgs, null, null, sortOrder, null);
            }finally {

            }

			return cursor;
		}
		public synchronized int deleteData(Uri uri,String selection,String[] selectionArgs){
			// Write deleteData
			SQLiteDatabase db = this.getWritableDatabase();
			int affectedRows, affectedRows1, affectedRows2, affectedRows3;
			affectedRows1 = db.delete(TABLE_NAME,selection,selectionArgs);
			affectedRows2 = db.delete(TABLE_NAME_BUDDY1,selection,selectionArgs);
			affectedRows3 = db.delete(TABLE_NAME_BUDDY2,selection,selectionArgs);
			affectedRows = affectedRows1 + affectedRows2 + affectedRows3;
			return affectedRows;
		}
	}

	DynamoDatabase myDatabase;

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static int key_id = 0;
	static final String ACK = "ACK";
	public String thisServerPort;
	public String myPredecessor = null;
	public String mySuccessor = null;
	public ContentValues valuesToSend = null;
	public boolean forceInsert = false;
	public boolean forceQuery = false;
	public boolean useBuddy1 = false;
	public boolean useBuddy2 = false;
	public boolean dontCall = false;
	public Cursor threadCursor = null;
	static HashMap<String,Boolean> writeStats = new HashMap<String, Boolean>();
	private static boolean WRITELOCK = false;
	public static boolean INIT_DELETE = false;
	//public String[] listOfPorts = {"5554","5556","5558","5560","5562"};
	public ArrayList<String> listOfPorts = new ArrayList<String>(){{
	    add("5554");
	    add("5556");
	    add("5558");
	    add("5560");
	    add("5562");
    }
    };
	public ArrayList<String> deletedList = new ArrayList<String>();
	public int NumberOfPorts = 5;
	public HashMap<String,String> buddy1Map = new HashMap<String, String>();
	public HashMap<String,String> buddy2Map = new HashMap<String, String>();

	public HashMap<String,String> revBuddy1Map = new HashMap<String, String>();
	public HashMap<String,String> revBuddy2Map = new HashMap<String, String>();


	private Uri buildUri(String scheme, String authority){
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	Uri uri = buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String actualKey;
		INIT_DELETE = true;
		if(selection.equals("@") || selection.equals("*")){
			actualKey = null;
            int rowsAffected = myDatabase.deleteData(uri,actualKey,selectionArgs);
		}else{

		    String contactNode = getNode(selection);
		    deletedList.add(selection);
            //actualKey = "key='" + selection + "'";
		    //myDatabase.deleteData(uri,actualKey,selectionArgs);
		    //deletedList.add(selection);
		    if(!contactNode.equals(thisServerPort)){
                try {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"delete",selection).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }else {
		        //deletedList.add(selection);
                actualKey = "key='" + selection + "'";
                int rowsAffected = myDatabase.deleteData(uri,actualKey,selectionArgs);
            }
		    Log.e(TAG,"deletion success");
		}


		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub


        String keyId = (String) values.get("key");
        String valueId = (String) values.get("value");
        String contactNode = getNode(keyId);
        String buddyToCall;


        // what if none of the buddys are activated ?
        // in that case I need to forward this value to the appropriate node


        if (contactNode.equals(thisServerPort)) {
            // Also it needs to inform it's buddy's
            buddyToCall = "NoOne";
            synchronized (this) {
                myDatabase.insertData(values, buddyToCall);
                //lock.unlock();
            }
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", keyId,valueId);
        //lock.unlock();

		return uri;
	}
    public class HashComparator implements Comparator<String> {

        @Override
        public int compare(String id1,String id2) {
            try{
                String id1Hash = genHash(id1);
                String id2Hash = genHash(id2);
                return id1Hash.compareTo(id2Hash);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG,"Failed to compare data");
                //e.printStackTrace();
            }
            return -1;
        }
    }
    public void createBuddy(){
        Collections.sort(listOfPorts,new HashComparator());
        //Log.e(TAG,"Buddy System");
        for(int i = 0;i < listOfPorts.size();i++){
            String basePort = listOfPorts.get(i);
            String nextPort = listOfPorts.get((i + 1)%5);
            String nextNextPort = listOfPorts.get((i+2)%5);

            buddy1Map.put(basePort,nextPort);
            revBuddy1Map.put(nextPort,basePort);
            buddy2Map.put(basePort,nextNextPort);
            revBuddy2Map.put(nextNextPort,basePort);
        }
    }
    public String getNode(String id){
	    // find the next node
        /*
        Loop Through the list assume it is sorted / or sort it every time
         */
        Collections.sort(listOfPorts,new HashComparator());

        // find the next node
        try {
            String idHash = genHash(id);
            String contactNode = "";
            boolean foundNodeInLoop = false;
            for (int i = 0; i < listOfPorts.size(); i++) {
                String basePort = listOfPorts.get(i);
                String baseHash = genHash(basePort);
                if(baseHash.compareTo(idHash) >= 0){
                    foundNodeInLoop = true;
                    contactNode = basePort;
                    break;
                }
            }if (foundNodeInLoop){
                return contactNode;
            }else{
                // if not everything then send to the first port
                contactNode = listOfPorts.get(0);
                return contactNode;
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG,"Failed To generate hash in getNode");
            //e.printStackTrace();
        }
        return "Noone";
    }

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		myDatabase = new DynamoDatabase(getContext());

		/*
		Add all the functionality when creating a socket
		 */
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr)*2));
		thisServerPort = portStr;

		// Log.e(TAG,"This SERVER_PORT");
		// Log.e(TAG,thisServerPort);

		try{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);
			Log.e(TAG,"Server created chill now");
		}catch (IOException e){
			Log.e(TAG,"Cannot create socket");
		}


        createBuddy();
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"join",thisServerPort);

		return false;
	}


	// Create a server task class
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			/*
			 * TODO: Fill in your server code that receives messages and passes them
			 * to onProgressUpdate().
			 */
			ServerSocket serverSocket = sockets[0];

			while(true){
				// define the insert, join and query protocol
                try {
                    Socket socket = serverSocket.accept();
                    //Log.e(TAG,"Accepted a connection");

                    ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                    serverOut.flush();
                    ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

                    // now I need to check for protocols
                    Message clientMessage = (Message) serverIn.readObject();

                    //Log.e(TAG,"Protocol");
                    //Log.e(TAG,clientMessage.protocol);

                    if (clientMessage.protocol.equals("delete")){

                        synchronized (this) {
                            String msgKey = clientMessage.keyString;
                            String msgVal = clientMessage.valString;

                            deletedList.add(msgKey);
                            String actualKey = "key='" + msgKey + "'";

                            //getContext().getContentResolver().insert(uri, cv);
                            myDatabase.deleteData(uri,actualKey,null);

                            Message serverResponse = new Message(ACK, null, null, null, null);

                            serverOut.writeObject(serverResponse);
                            serverOut.flush();
                        }

                        //writeLock.unlock();

                        serverIn.close();
                        serverOut.close();
                        socket.close();
                    }else if (clientMessage.protocol.equals("insert")){

                        synchronized (this) {
                            String msgKey = clientMessage.keyString;
                            String msgVal = clientMessage.valString;

                            //lock.writeLock().lock();
                            ContentValues cv = new ContentValues();
                            cv.put("key", msgKey);
                            cv.put("value", msgVal);
                            //getContext().getContentResolver().insert(uri, cv);
                            //lock.lock();
                            if(!deletedList.contains(msgKey)) {
                                myDatabase.insertData(cv, "Noone");
                            }
                            //lock.unlock();


                            Message serverResponse = new Message(ACK, null, null, null, null);

                            serverOut.writeObject(serverResponse);
                            serverOut.flush();
                        }

                        //writeLock.unlock();

                        serverIn.close();
                        serverOut.close();
                        socket.close();
                    }else if (clientMessage.protocol.equals("insertBuddy1")){

                        //lock.lock();
                        synchronized (this) {
                            String msgKey = clientMessage.keyString;
                            String msgVal = clientMessage.valString;
                            //lock.lock();

                            ContentValues cv = new ContentValues();
                            cv.put("key", msgKey);
                            cv.put("value", msgVal);
                            if(!deletedList.contains(msgKey)) {
                                myDatabase.insertData(cv, "buddy1");
                            }
                            //lock.unlock();
                            //Log.e(TAG,"Insert into buddy 1 successful");

                            Message serverResponse = new Message(ACK, null, null, null, null);

                            serverOut.writeObject(serverResponse);
                            serverOut.flush();
                        }

                        serverIn.close();
                        serverOut.close();
                        socket.close();


                    }else if (clientMessage.protocol.equals("insertBuddy2")){

                        synchronized (this) {
                            String msgKey = clientMessage.keyString;
                            String msgVal = clientMessage.valString;

                            //lock.writeLock().lock();
                            //lock.lock();
                            ContentValues cv = new ContentValues();
                            cv.put("key", msgKey);
                            cv.put("value", msgVal);
                            //getContext().getContentResolver().insert(uri,cv)
                            if(!deletedList.contains(msgKey)) {
                                myDatabase.insertData(cv, "buddy2");
                            }
                            //lock.unlock();

                            //Log.e(TAG,"Insert into buddy 2 successful");

                            Message serverResponse = new Message(ACK, null, null, null, null);

                            serverOut.writeObject(serverResponse);
                            serverOut.flush();
                        }
                        serverIn.close();
                        serverOut.close();
                        socket.close();
                    }else if(clientMessage.protocol.equals("query")){

                        synchronized (this) {
                            lock.lock();
                            String valueKey = clientMessage.keyString;
                            //lock.readLock().lock();
                            String actualKey = "key='" + valueKey + "'";

                            Cursor resultCursor = myDatabase.queryDatabase(null, actualKey, null, null, "Noone");
                            lock.unlock();
                            //Cursor resultCursor = getContext().getContentResolver().query(uri, null, valueKey, null, null);
                            //lock.readLock().unlock();
                            String valToFind;
                            Message serverResponse;
                            boolean foundMessage = false;

                            resultCursor.moveToFirst();
                            while (!resultCursor.isAfterLast()) {
                                String keyEntity = resultCursor.getString(0);
                                String valEntity = resultCursor.getString(1);

                                if (valueKey.equals(keyEntity)) {
                                    serverResponse = new Message(ACK, valueKey, valEntity, null, null);
                                    serverOut.writeObject(serverResponse);
                                    serverOut.flush();
                                    foundMessage = true;
                                    break;
                                }
                                resultCursor.moveToNext();
                            }

                            resultCursor.close();

                            //dontCall = false;
                            //readLock.unlock();
                            if (!foundMessage) {
                                serverResponse = new Message("notFound", null, null, null, null);
                                serverOut.writeObject(serverResponse);
                                serverOut.flush();
                            }
                        }

                        serverIn.close();
                        serverOut.close();
                        socket.close();

                    }else if(clientMessage.protocol.equals("queryBuddy1")){

                        //useBuddy1 = true;
                        synchronized (this) {
                            String valueKey = clientMessage.keyString;
                            String actualKey = "key='" + valueKey + "'";
                            //lock.readLock().lock();
                            //lock.lock();
                            Cursor resultCursor = myDatabase.queryDatabase(null, actualKey, null, null, "buddy1");
                            //lock.unlock();
                            String valToFind;
                            Message serverResponse;
                            boolean keyFound = false;

                            resultCursor.moveToFirst();
                            while (!resultCursor.isAfterLast()) {
                                String keyEntity = resultCursor.getString(0);
                                String valEntity = resultCursor.getString(1);

                                if (valueKey.equals(keyEntity)) {
                                    serverResponse = new Message(ACK, valueKey, valEntity, null, null);
                                    keyFound = true;
                                    serverOut.writeObject(serverResponse);
                                    serverOut.flush();

                                    break;
                                }

                                resultCursor.moveToNext();
                            }

                            resultCursor.close();

                            if (!keyFound) {
                                serverResponse = new Message("contactOriginal", null, null, null, null);
                                serverOut.writeObject(serverResponse);
                                serverOut.flush();
                            }
                        }

                        serverIn.close();
                        serverOut.close();
                        socket.close();

                    }else if(clientMessage.protocol.equals("queryBuddy2")){


						synchronized (this) {
                            String valueKey = clientMessage.keyString;
                            //lock.readLock().lock();
                            //lock.lock();
                            Cursor resultCursor = myDatabase.queryDatabase(null, null, null, null, "buddy2");
                            //lock.unlock();
                            String valToFind;
                            Message serverResponse;
                            resultCursor.moveToFirst();
                            boolean keyFound = false;
                            while (!resultCursor.isAfterLast()) {
                                String keyEntity = resultCursor.getString(0);
                                String valEntity = resultCursor.getString(1);

                                if (valueKey.equals(keyEntity)) {
                                    serverResponse = new Message(ACK, valueKey, valEntity, null, null);
                                    //Log.e(TAG,"I did send the key");
                                    keyFound = true;
                                    serverOut.writeObject(serverResponse);
                                    serverOut.flush();
                                    break;
                                }

                                resultCursor.moveToNext();
                            }

                            //Log.e(TAG,"Out of Query Buddy 2");
                            resultCursor.close();

                            if (!keyFound) {
                                serverResponse = new Message("contactBuddy1", null, null, null, null);
                                serverOut.writeObject(serverResponse);
                                serverOut.flush();
                            }
                        }

						serverIn.close();
						serverOut.close();
						socket.close();

					}else if(clientMessage.protocol.equals("queryAll")){

                    	synchronized (this) {
                            Cursor cursor = myDatabase.queryDatabase(null, null, null, null, "Noone");
                            //Cursor cursor = getContext().getContentResolver().query(uri,null,"@",null,null);

                            cursor.moveToFirst();
                            while (!cursor.isAfterLast()) {
                                String returnKey = cursor.getString(0);
                                String returnVal = cursor.getString(1);

                                if (!deletedList.contains(returnKey)) {

                                    Message serverResponse = new Message("acceptValue", returnKey, returnVal, null, null);

                                    serverOut.writeObject(serverResponse);
                                    serverOut.flush();
                                }

                                cursor.moveToNext();
                            }

                            cursor.close();
                            Log.e(TAG, "All values returned");

                            Message finalServerResponse = new Message(ACK, null, null, null, null);

                            serverOut.writeObject(finalServerResponse);
                            serverOut.flush();
                        }

						serverIn.close();
						serverOut.close();
						socket.close();
					}else if(clientMessage.protocol.equals("queryBuddy2All")){

                        synchronized (this) {
                            Cursor cursor = myDatabase.queryDatabase(null, null, null, null, "buddy2");
                            //Cursor cursor = getContext().getContentResolver().query(uri,null,"@",null,null);

                            cursor.moveToFirst();
                            while (!cursor.isAfterLast()) {
                                String returnKey = cursor.getString(0);
                                String returnVal = cursor.getString(1);

                                if (!deletedList.contains(returnKey)) {

                                    Message serverResponse = new Message("acceptValue", returnKey, returnVal, null, null);

                                    serverOut.writeObject(serverResponse);
                                    serverOut.flush();
                                }

                                cursor.moveToNext();
                            }

                            cursor.close();
                            Log.e(TAG, "All values returned by buddy 2");

                            Message finalServerResponse = new Message(ACK, null, null, null, null);

                            serverOut.writeObject(finalServerResponse);
                            serverOut.flush();
                        }

                        serverIn.close();
                        serverOut.close();
                        socket.close();
                    }else if(clientMessage.protocol.equals("queryBuddy1All")){

                        synchronized (this) {
                            Cursor cursor = myDatabase.queryDatabase(null, null, null, null, "buddy1");
                            //Cursor cursor = getContext().getContentResolver().query(uri,null,"@",null,null);

                            cursor.moveToFirst();
                            while (!cursor.isAfterLast()) {
                                String returnKey = cursor.getString(0);
                                String returnVal = cursor.getString(1);

                                if (!deletedList.contains(returnKey)) {

                                    Message serverResponse = new Message("acceptValue", returnKey, returnVal, null, null);

                                    serverOut.writeObject(serverResponse);
                                    serverOut.flush();
                                }
                                cursor.moveToNext();
                            }

                            cursor.close();
                            Log.e(TAG, "All values returned by buddy 1");

                            Message finalServerResponse = new Message(ACK, null, null, null, null);

                            serverOut.writeObject(finalServerResponse);
                            serverOut.flush();
                        }

                        serverIn.close();
                        serverOut.close();
                        socket.close();
                    }else{
                        // Do nothing and break
						Log.e(TAG,"Breaking");
                        break;
                    }
                } catch (IOException e) {
                    Log.e(TAG,"Failed to create a socket as a server");
                    //e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    Log.e(TAG,"Failed to read the class in server");
                    //e.printStackTrace();
                }

            }


			return null;
		}

		protected void onProgressUpdate(String...strings) {
			/*
			 * The following code displays what is received in doInBackground().
			 */
			String strReceived = strings[0].trim();

			return;
		}
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

        //  query based on what the selection and the buddy is
        //while (WRITELOCK)
        String buddy;
        Cursor resultCursor;

        //while (!WRITELOCK);
        String actualKey;
        // it can be @ or * in which case I need to make it null
        if (selection.equals("*")) {
            // Invoke query all
            actualKey = null;
            buddy = "Noone";
            Cursor myCursor = null;
            Cursor buddy1Cursor = null;
            Cursor buddy2Cursor = null;
            ArrayList<Cursor> cursorList = new ArrayList<Cursor>();
            //lock.readLock().lock();
            //lock.lock();
            myCursor = myDatabase.queryDatabase(projection, actualKey, selectionArgs, sortOrder, "Noone");
            //buddy1Cursor = myDatabase.queryDatabase(projection, actualKey, selectionArgs, sortOrder, "buddy1");
            //buddy2Cursor = myDatabase.queryDatabase(projection, actualKey, selectionArgs, sortOrder, "buddy2");
            //lock.readLock().unlock();
            //lock.unlock();
            //Cursor completeCursor;
            //Log.e(TAG, "Initiated query all protocol");
            threadCursor = null;
            try {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "getAll", selection).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            if (threadCursor == null) {
                Log.e(TAG, "Thread cursor is still null something is wrong");
            }
            //completeCursor = threadCursor;
            //completeCursor = getAllValues();

            cursorList.add(myCursor);
            //cursorList.add(buddy1Cursor);
            //cursorList.add(buddy2Cursor);
            cursorList.add(threadCursor);

            resultCursor = new MergeCursor(cursorList.toArray(new Cursor[0]));

        } else if (selection.equals("@")) {
            // Invoke query here
            actualKey = null;
            buddy = "Noone";
            Cursor myCursor = null;
            Cursor buddy1Cursor = null;
            Cursor buddy2Cursor = null;
            ArrayList<Cursor> cursorList = new ArrayList<Cursor>();
            //lock.writeLock().lock();
            lock.lock();
            myCursor = myDatabase.queryDatabase(projection, actualKey, selectionArgs, sortOrder, "Noone");
            buddy1Cursor = myDatabase.queryDatabase(projection, actualKey, selectionArgs, sortOrder, "buddy1");
            buddy2Cursor = myDatabase.queryDatabase(projection, actualKey, selectionArgs, sortOrder, "buddy2");
            lock.unlock();
            //lock.writeLock().unlock();
            cursorList.add(myCursor);
            cursorList.add(buddy1Cursor);
            cursorList.add(buddy2Cursor);

            deletedList = new ArrayList<String>();

            resultCursor = new MergeCursor(cursorList.toArray(new Cursor[0]));

            //resultCursor = myDatabase.queryDatabase(projection,actualKey,selectionArgs,sortOrder,buddy);
        } else {
            // Check if the key belongs to you
            String contactNode = getNode(selection);
            String readNode = buddy2Map.get(contactNode);
            String readAnother = buddy1Map.get(contactNode);

            if (!contactNode.equals(thisServerPort)) {

                //lock.lock();
                Log.e(TAG, "Initiated query protocol");
                Log.e(TAG,selection);
                Log.e(TAG,contactNode);
                threadCursor = null;
                try {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", selection).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                if (threadCursor == null) {
                    Log.e(TAG, "Thread cursor is still null something is wrong");
                }
                resultCursor = threadCursor;
                //lock.unlock();
                //resultCursor = getQuery(selection, contactNode);

            } else {
                //Log.e(TAG, "Query Belongs to me");
                //Log.e(TAG, selection);

                actualKey = "key='" + selection + "'";
                buddy = "Noone";
                //lock.writeLock().lock();
                lock.lock();
                resultCursor = myDatabase.queryDatabase(projection, actualKey, selectionArgs, sortOrder, buddy);
                lock.unlock();
                //lock.writeLock().unlock();
            }
        }
        //WRITELOCK = false;
		return resultCursor;
		//return null;
	}
	public MatrixCursor getQuery(String selection,String contactNode){
		// ask 11108 who is the guy to contact
		Message clientMessage = new Message("query",selection,null,null,thisServerPort);
		String selectionVal = "";
		// At this point everyone knows which port contains the information

		// set a timeout so that we may contact the buddy's
        //String readNode = buddy2Map.get(contactNode);
        //boolean contactFailed = false;

		try{
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(contactNode)*2);

			//socket.setSoTimeout(100);
			ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
			serverOut.flush();
			ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

			serverOut.writeObject(clientMessage);
			serverOut.flush();

			Message serverResponse;

			while (true){
				serverResponse = (Message) serverIn.readObject();

				if(serverResponse.protocol.equals(ACK)){
					selectionVal = serverResponse.valString;
					//Log.e(TAG,"server did find the message");
					break;
				}
				else if (serverResponse.protocol.equals("notFound")){
				    Log.e(TAG,"Server did not find it");
				    //contactFailed = true;
				    break;
                }else{
				    // do nothing
                }
				Log.e(TAG,"Waiting for server to respond");
			}

			serverIn.close();
			serverOut.close();
			socket.close();
			} catch (UnknownHostException e) {
				//e.printStackTrace();
			}catch (IOException e) {
				//e.printStackTrace();
				Log.e(TAG,"Client Socket Broke");
			} catch (ClassNotFoundException e) {
				//e.printStackTrace();
			}
		// form a cursor object and return selection, it's selection value
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
		cursor.addRow(new String[]{selection, selectionVal});

		return cursor;

	}

	public MatrixCursor getAllValues(){
		// this function is tasked with getting all the values from all the databases
		// first we need to contact all successors then we contact all predecessors
		Message clientMessage = new Message("queryAll",null,null,null,null);
		MatrixCursor cursor = new MatrixCursor(new String[] {"key","value"});
		for (int i = 0;i < listOfPorts.size();i++){
			String contactPort = listOfPorts.get(i);
			if (contactPort.equals(thisServerPort)){
				continue;
			}
			String buddyPort = buddy1Map.get(contactPort);
			boolean contactEstablished = true;
			try{

				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(contactPort)*2);

				ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
				serverOut.flush();
				ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

				serverOut.writeObject(clientMessage);
				serverOut.flush();

				Message serverResponse;
				while (true){
				    socket.setSoTimeout(100);
					serverResponse = (Message) serverIn.readObject();
					if (serverResponse.protocol.equals(ACK)){
						break;
					}else{
						cursor.addRow(new String[] {serverResponse.keyString,serverResponse.valString});
					}
				}

				serverIn.close();
				serverOut.close();
				socket.close();

			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (StreamCorruptedException e) {
				e.printStackTrace();
			} catch (SocketTimeoutException ste){
			    contactEstablished = false;
            } catch (IOException e) {
			    contactEstablished = false;
				Log.e(TAG,"IO exception in return all values, I think some node has failed");
			    //e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}

			if(!contactEstablished){
			    clientMessage.protocol = "queryBuddy1All";
                MatrixCursor tempcursor = new MatrixCursor(new String[] {"key","value"});

                try{

                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(buddyPort)*2);

                    ObjectOutputStream serverOut1 = new ObjectOutputStream(socket1.getOutputStream());
                    serverOut1.flush();
                    ObjectInputStream serverIn1 = new ObjectInputStream(socket1.getInputStream());

                    serverOut1.writeObject(clientMessage);
                    serverOut1.flush();

                    //Log.e(TAG,"Connection Established with buddy for query all");

                    Message serverResponse;
                    while (true){
                        socket1.setSoTimeout(100);
                        serverResponse = (Message) serverIn1.readObject();
                        if (serverResponse.protocol.equals(ACK)){
                            break;
                        }else{

                            tempcursor.addRow(new String[] {serverResponse.keyString,serverResponse.valString});
                        }
                    }

                    serverIn1.close();
                    serverOut1.close();
                    socket1.close();

                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (StreamCorruptedException e) {
                    e.printStackTrace();
                } catch (SocketTimeoutException ste){
                    contactEstablished = false;
                } catch (IOException e) {
                    contactEstablished = false;
                    Log.e(TAG,"IO exception in return all values for buddy , I think some node has failed");
                    //e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

                // add remaining values to the cursor
                tempcursor.moveToFirst();
                while (!tempcursor.isAfterLast()){
                    String returnKey = tempcursor.getString(0);
                    String returnVal = tempcursor.getString(1);

                    cursor.addRow(new String[] {returnKey,returnVal});
                    //Message serverResponse = new Message("acceptValue",returnKey,returnVal,null,null);

                    //serverOut1.writeObject(serverResponse);
                    //serverOut1.flush();

                    tempcursor.moveToNext();
                }

                tempcursor.close();

            }
        }

		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	private class ClientTask extends AsyncTask<String, Void, Void> {

		public Cursor clientCursor = null;
	    @Override
		protected Void doInBackground(String... msgs) {

			String action = msgs[0];
			//Log.e(TAG,"Action is");
			//Log.e(TAG,action);
			if(action.equals("insert")) {
				//lock.lock();
                synchronized (this) {
                    String msgKey = msgs[1];
                    String msgVal = msgs[2];
                    //String msgKey = (String) valuesToSend.get("key");
                    String contactNode = getNode(msgKey);
                    //String msgVal = (String) valuesToSend.get("value");
                    //Log.e(TAG,"Insert Protocol Initiated");
                    //Log.e(TAG,"Contact Node is");
                    //Log.e(TAG,contactNode);
                    //Log.e(TAG,"KEY");
                    //Log.e(TAG,msgKey);
                    //Log.e(TAG,"VALUE");
                    //Log.e(TAG,msgVal);
                    if (!contactNode.equals(thisServerPort)) {


                        Message clientMessage = new Message("insert", msgKey, msgVal, null, thisServerPort);
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(contactNode) * 2);

                            ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                            clientOut.flush();
                            ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                            clientOut.writeObject(clientMessage);

                            Message serverResponse;
                            while (true) {
                                serverResponse = (Message) clientIn.readObject();
                                if (serverResponse.protocol.equals(ACK)) {
                                    break;
                                }
                            }
                            clientIn.close();
                            clientOut.close();
                            socket.close();

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "Failed to create socket at insert");
                            //e.printStackTrace();
                        } catch (IOException e) {
                            Log.e(TAG, "Failed to create socket at Insert");
                            //e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            Log.e(TAG, "Failed to read object");
                            //e.printStackTrace();
                        }
                    }
                    // time to send it to the buddys
                    String buddy1 = buddy1Map.get(contactNode);
                    String buddy2 = buddy2Map.get(contactNode);

                    Message clientMessage1 = new Message("insertBuddy1", msgKey, msgVal, null, thisServerPort);

                    if (buddy1.equals(thisServerPort)) {

                        synchronized (this) {
                            ContentValues cv = new ContentValues();
                            cv.put("key",msgKey);
                            cv.put("value",msgVal);
                            if(!deletedList.contains(msgKey)) {
                                myDatabase.insertData(cv, "buddy1");
                            }
                        }
                    } else {
                        // first send the message to buddy1
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(buddy1) * 2);

                            //Log.e(TAG,"Scoket created to send messages to buddy1");
                            ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                            clientOut.flush();
                            ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                            //Log.e(TAG,"Streams have been set");
                            clientOut.writeObject(clientMessage1);
                            clientOut.flush();

                            Message serverResponse;
                            //Log.e(TAG,"Waiting for Ack");
                            while (true) {
                                serverResponse = (Message) clientIn.readObject();
                                if (serverResponse.protocol.equals(ACK)) {
                                    //Log.e(TAG,"Got the Ack");
                                    break;
                                }
                            }
                            clientIn.close();
                            clientOut.close();
                            socket.close();
                        } catch (UnknownHostException e) {
                            Log.e(TAG, "Failed in inserting to buddy1");
                            //e.printStackTrace();
                        } catch (IOException e) {
                            Log.e(TAG, "Failed in inserting to buddy1");
                            //e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            Log.e(TAG, "Failed to read class");
                            //e.printStackTrace();
                        }
                    }
                    if (buddy2.equals(thisServerPort)) {

                       synchronized (this) {
                           ContentValues cv = new ContentValues();
                           cv.put("key",msgKey);
                           cv.put("value",msgVal);
                           if(!deletedList.contains(msgKey)) {
                               myDatabase.insertData(cv, "buddy2");
                           }
                       }
                    } else {
                        Message clientMessage2 = new Message("insertBuddy2", msgKey, msgVal, null, thisServerPort);
                        // now send the message to buddy2
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(buddy2) * 2);

                            ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                            clientOut.flush();
                            ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                            //Log.e(TAG,"Stream created for insertBuddy2");
                            clientOut.writeObject(clientMessage2);
                            clientOut.flush();


                            Message serverResponse;
                            while (true) {
                                serverResponse = (Message) clientIn.readObject();
                                if (serverResponse.protocol.equals(ACK)) {
                                    //Log.e(TAG,"Got ACK from buddy2");
                                    break;
                                }
                            }
                            clientIn.close();
                            clientOut.close();
                            socket.close();
                        } catch (UnknownHostException e) {
                            Log.e(TAG, "Failed in inserting to buddy2");
                            //e.printStackTrace();
                        } catch (IOException e) {
                            Log.e(TAG, "Failed in inserting to buddy2");
                            //e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            Log.e(TAG, "Failed to read class");
                            //e.printStackTrace();
                        }
                    }
                }
                //lock.unlock();
			}else if(action.equals("query")){
                // ask 11108 who is the guy to contact
                synchronized (this) {
                    //lock.lock();
                    String selection = msgs[1];
                    String contactNode = getNode(selection);
                    String buddy1Port = buddy1Map.get(contactNode);
                    String buddy2Port = buddy2Map.get(contactNode);
                    Message clientMessage = new Message("query", selection, null, null, thisServerPort);
                    String selectionVal = "";
                    // At this point everyone knows which port contains the information

                    // set a timeout so that we may contact the buddy's
                    //String readNode = buddy2Map.get(contactNode);
                    //boolean contactFailed = false;

                    boolean contactNodeFound = true;
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(contactNode) * 2);


                        ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                        serverOut.flush();
                        ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

                        serverOut.writeObject(clientMessage);
                        serverOut.flush();

                        Message serverResponse;

                        while (true) {
                            socket.setSoTimeout(100);
                            serverResponse = (Message) serverIn.readObject();

                            if (serverResponse.protocol.equals(ACK)) {
                                selectionVal = serverResponse.valString;
                                //Log.e(TAG,"selection was found");
                                //Log.e(TAG,selection);
                                //Log.e(TAG,"server did find the message");
                                break;
                            } else if (serverResponse.protocol.equals("notFound")) {
                                Log.e(TAG,"SERVER DID NOT FIND IT");
                                //contactFailed = true;
                                break;
                            } else {
                                // do nothing
                            }
                            //Log.e(TAG,"Waiting for server to respond");
                        }

                        serverIn.close();
                        serverOut.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        //e.printStackTrace();
                        contactNodeFound = false;
                    } catch (SocketTimeoutException ste) {
                        contactNodeFound = false;
                    } catch (IOException e) {
                        //e.printStackTrace();
                        contactNodeFound = false;
                        Log.e(TAG, "Client Socket Broke");
                    } catch (ClassNotFoundException e) {
                        //e.printStackTrace();
                        contactNodeFound = false;
                    }

                    if (!contactNodeFound) {
                        boolean buddy1Found = true;
                        clientMessage.protocol = "queryBuddy1";
                        String selectionVal2 = "";
                        try {
                            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(buddy1Port) * 2);


                            ObjectOutputStream serverOut1 = new ObjectOutputStream(socket1.getOutputStream());
                            serverOut1.flush();
                            ObjectInputStream serverIn1 = new ObjectInputStream(socket1.getInputStream());

                            serverOut1.writeObject(clientMessage);
                            serverOut1.flush();

                            Message serverResponse;

                            Log.e(TAG, "Established Connection in buddy1");

                            while (true) {
                                socket1.setSoTimeout(100);
                                serverResponse = (Message) serverIn1.readObject();

                                if (serverResponse.protocol.equals(ACK)) {
                                    selectionVal2 = serverResponse.valString;
                                    //Log.e(TAG,"selection was found in buddy 1");
                                    //Log.e(TAG,selection);
                                    //Log.e(TAG,"And it's value is");
                                    //Log.e(TAG,selectionVal2);
                                    //Log.e(TAG,"server did find the message");
                                    break;
                                } else if (serverResponse.protocol.equals("notFound")) {
                                    Log.e(TAG,"SERVER DID NOT FIND IT HERE ALSO");
                                    //contactFailed = true;
                                    break;
                                } else {
                                    // do nothing
                                }
                                //Log.e(TAG,"Waiting for server to respond");
                            }

                            serverIn1.close();
                            serverOut1.close();
                            socket1.close();

                            // form a cursor object and return selection, it's selection value
                            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                            cursor.addRow(new String[]{selection, selectionVal2});

                            threadCursor = cursor;
                        } catch (UnknownHostException e) {
                            //e.printStackTrace();
                            //contactNodeFound = false;
                        } catch (SocketTimeoutException ste) {
                            //contactNodeFound = false;
                        } catch (IOException e) {
                            //e.printStackTrace();
                            //contactNodeFound = false;
                            Log.e(TAG, "Client Socket Broke for buddy 1");
                        } catch (ClassNotFoundException e) {
                            //e.printStackTrace();
                            //contactNodeFound = false;
                        }
                    } else {
                        // form a cursor object and return selection, it's selection value
                        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                        cursor.addRow(new String[]{selection, selectionVal});

                        threadCursor = cursor;
                    }
                }
                //lock.unlock();
            }else if(action.equals("join")){
			    // contact buddy and get any missing value
                synchronized (this){
                    String buddy1Port = buddy1Map.get(thisServerPort);
                    String buddy2Port = buddy2Map.get(thisServerPort);
                    String revBuddy1Port = revBuddy1Map.get(thisServerPort);
                    String revBuddy2Port = revBuddy2Map.get(thisServerPort);
                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(buddy2Port)*2);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        clientOut.flush();
                        ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                        Message clientMessage = new Message("queryBuddy2All",null,null,null,null);

                        clientOut.writeObject(clientMessage);
                        clientOut.flush();

                        Message serverResponse;
                        while (true){
                            serverResponse = (Message) clientIn.readObject();
                            if (serverResponse.protocol.equals(ACK)){
                                Log.e(TAG,"Received All messages from buddy2");
                                break;
                            }else{
                                String msgKey = serverResponse.keyString;
                                String msgVal = serverResponse.valString;
                                ContentValues cv = new ContentValues();
                                cv.put("key",msgKey);
                                cv.put("value",msgVal);
                                if(!deletedList.contains(msgKey)) {
                                    myDatabase.insertData(cv, "Noone");
                                }
                            }
                        }
                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(buddy1Port)*2);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        clientOut.flush();
                        ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                        Message clientMessage = new Message("queryBuddy1All",null,null,null,null);

                        clientOut.writeObject(clientMessage);
                        clientOut.flush();

                        Message serverResponse;
                        while (true){
                            serverResponse = (Message) clientIn.readObject();
                            if (serverResponse.protocol.equals(ACK)){
                                Log.e(TAG,"Received All messages from buddy 1");
                                break;
                            }else{
                                String msgKey = serverResponse.keyString;
                                String msgVal = serverResponse.valString;
                                ContentValues cv = new ContentValues();
                                cv.put("key",msgKey);
                                cv.put("value",msgVal);
                                if(!deletedList.contains(msgKey)) {
                                    myDatabase.insertData(cv, "Noone");
                                }

                            }
                        }
                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(revBuddy1Port)*2);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        clientOut.flush();
                        ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                        Message clientMessage = new Message("queryAll",null,null,null,null);

                        clientOut.writeObject(clientMessage);
                        clientOut.flush();

                        Message serverResponse;
                        while (true){
                            serverResponse = (Message) clientIn.readObject();
                            if (serverResponse.protocol.equals(ACK)){
                                break;
                            }else{
                                String msgKey = serverResponse.keyString;
                                String msgVal = serverResponse.valString;
                                ContentValues cv = new ContentValues();
                                cv.put("key",msgKey);
                                cv.put("value",msgVal);
                                //myDatabase.insertData(cv,"buddy1");
                                if(!deletedList.contains(msgKey)) {
                                    myDatabase.insertData(cv, "buddy1");
                                }

                            }
                        }
                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(revBuddy2Port)*2);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        clientOut.flush();
                        ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                        Message clientMessage = new Message("queryAll",null,null,null,null);

                        clientOut.writeObject(clientMessage);
                        clientOut.flush();

                        Message serverResponse;
                        while (true){
                            serverResponse = (Message) clientIn.readObject();
                            if (serverResponse.protocol.equals(ACK)){
                                break;
                            }else{
                                String msgKey = serverResponse.keyString;
                                String msgVal = serverResponse.valString;
                                ContentValues cv = new ContentValues();
                                cv.put("key",msgKey);
                                cv.put("value",msgVal);
                                //myDatabase.insertData(cv,"buddy2");
                                if(!deletedList.contains(msgKey)) {
                                    myDatabase.insertData(cv, "buddy2");
                                }

                            }
                        }
                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }else if(action.equals("getAll")){

                MatrixCursor properCursor = new MatrixCursor(new String[] {"key","value"});
                ArrayList<Cursor> cursorList = new ArrayList<Cursor>();
                String failedNode = null;
                for (int i = 0;i < listOfPorts.size();i++){
                    String contactPort = listOfPorts.get(i);
                    if (contactPort.equals(thisServerPort)){
                        continue;
                    }

                    Message clientMessage = new Message("queryAll",null,null,null,null);
                    Log.e(TAG,"Getting values from");
                    Log.e(TAG,contactPort);
                    String buddyPort = buddy1Map.get(contactPort);
                    boolean contactEstablished = true;
                    try{

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(contactPort)*2);

                        MatrixCursor cursor = new MatrixCursor(new String[] {"key","value"});

                        ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                        serverOut.flush();
                        ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

                        serverOut.writeObject(clientMessage);
                        serverOut.flush();

                        Message serverResponse;
                        while (true){
                            socket.setSoTimeout(100);
                            serverResponse = (Message) serverIn.readObject();
                            if (serverResponse.protocol.equals(ACK)){
                                Log.e(TAG,"Got all values from this port");
                                break;
                            }else{
                                Log.e(TAG,"Adding key");
                                Log.e(TAG,serverResponse.keyString);
                                Log.e(TAG,"Adding value");
                                Log.e(TAG,serverResponse.valString);
                                cursor.addRow(new String[] {serverResponse.keyString,serverResponse.valString});
                            }
                        }

                        serverIn.close();
                        serverOut.close();
                        socket.close();

                        cursorList.add(cursor);
                        cursor.close();

                    } catch (UnknownHostException e) {
                        contactEstablished = false;
                        //e.printStackTrace();
                    } catch (StreamCorruptedException e) {
                        contactEstablished = false;
                        failedNode = contactPort;
                        //e.printStackTrace();
                    } catch (SocketTimeoutException ste){
                        contactEstablished = false;
                        failedNode = contactPort;
                    } catch (IOException e) {
                        contactEstablished = false;
                        failedNode = contactPort;
                        Log.e(TAG,"IO exception in return all values, I think some node has failed");
                        //e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        contactEstablished = false;
                        e.printStackTrace();
                    }


                    if(!contactEstablished){
                        clientMessage.protocol = "queryBuddy1All";


                        try{

                            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(buddyPort)*2);

                            ObjectOutputStream serverOut1 = new ObjectOutputStream(socket1.getOutputStream());
                            serverOut1.flush();
                            ObjectInputStream serverIn1 = new ObjectInputStream(socket1.getInputStream());

                            MatrixCursor cursor = new MatrixCursor(new String[] {"key","value"});

                            serverOut1.writeObject(clientMessage);
                            serverOut1.flush();

                            //Log.e(TAG,"Connection Established with buddy for query all");
                            Message serverResponse;
                            while (true){
                                socket1.setSoTimeout(100);
                                serverResponse = (Message) serverIn1.readObject();
                                if (serverResponse.protocol.equals(ACK)){
                                    Log.e(TAG,"This port was broken hence got value from buddy1");
                                    break;
                                }else{
                                    //Log.e(TAG,"Entering stuff into the temp cursor");
                                    cursor.addRow(new String[] {serverResponse.keyString,serverResponse.valString});
                                }
                            }

                            serverIn1.close();
                            serverOut1.close();
                            socket1.close();

                            cursorList.add(cursor);
                            cursor.close();


                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch (StreamCorruptedException e) {
                            e.printStackTrace();
                        } catch (SocketTimeoutException ste){
                            contactEstablished = false;
                        } catch (IOException e) {
                            contactEstablished = false;
                            Log.e(TAG,"IO exception in return all values for buddy , I think some node has failed");
                            //e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }

                    }else{

                    }
                }
                threadCursor = new MergeCursor(cursorList.toArray(new Cursor[0]));
            }else if(action.equals("delete")){
			    String selection = msgs[1];
			    String contactNode = getNode(selection);
			    String buddy1Port = buddy1Map.get(contactNode);
			    String buddy2Port = buddy2Map.get(contactNode);
			    if(!contactNode.equals(thisServerPort)){
			        Log.e(TAG,"Deleting from Port");
			        Log.e(TAG,contactNode);
                    Message clientMessage = new Message("delete", selection, null, null, thisServerPort);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(contactNode) * 2);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        clientOut.flush();
                        ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                        clientOut.writeObject(clientMessage);

                        Message serverResponse;
                        while (true) {
                            serverResponse = (Message) clientIn.readObject();
                            if (serverResponse.protocol.equals(ACK)) {
                                break;
                            }
                        }
                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Failed to create socket at insert");
                        //e.printStackTrace();
                    } catch (IOException e) {
                        Log.e(TAG, "Failed to delete from Main");
                        //e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        Log.e(TAG, "Failed to read object");
                        //e.printStackTrace();
                    }
                }

                // time to send it to the buddys
                String buddy1 = buddy1Map.get(contactNode);
                String buddy2 = buddy2Map.get(contactNode);

                Message clientMessage1 = new Message("delete", selection, null, null, thisServerPort);

                if (buddy1.equals(thisServerPort)) {

                    synchronized (this) {
                        deletedList.add(selection);
                        String actualKey = "key='" + selection + "'";
                        myDatabase.deleteData(uri,actualKey,null);
                    }
                } else {
                    // first send the message to buddy1
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(buddy1) * 2);

                        //Log.e(TAG,"Scoket created to send messages to buddy1");
                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        clientOut.flush();
                        ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                        //Log.e(TAG,"Streams have been set");
                        clientOut.writeObject(clientMessage1);
                        clientOut.flush();

                        Message serverResponse;
                        //Log.e(TAG,"Waiting for Ack");
                        while (true) {
                            serverResponse = (Message) clientIn.readObject();
                            if (serverResponse.protocol.equals(ACK)) {
                                //Log.e(TAG,"Got the Ack");
                                break;
                            }
                        }
                        clientIn.close();
                        clientOut.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Failed in deleting from buddy1");
                        //e.printStackTrace();
                    } catch (IOException e) {
                        Log.e(TAG, "Failed in deleting from buddy1");
                        //e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        Log.e(TAG, "Failed to read class");
                        //e.printStackTrace();
                    }
                }
                if (buddy2.equals(thisServerPort)) {

                    synchronized (this) {
                        //ContentValues cv = new ContentValues();
                        //cv.put("key",msgKey);
                        //cv.put("value",msgVal);
                        deletedList.add(selection);
                        String actualKey = "key='" + selection + "'";
                        myDatabase.deleteData(uri,actualKey,null);
                    }
                } else {
                    Message clientMessage2 = new Message("delete", selection, null, null, thisServerPort);
                    // now send the message to buddy2
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(buddy2) * 2);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        clientOut.flush();
                        ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                        //Log.e(TAG,"Stream created for insertBuddy2");
                        clientOut.writeObject(clientMessage2);
                        clientOut.flush();


                        Message serverResponse;
                        while (true) {
                            serverResponse = (Message) clientIn.readObject();
                            if (serverResponse.protocol.equals(ACK)) {
                                //Log.e(TAG,"Got ACK from buddy2");
                                break;
                            }
                        }
                        clientIn.close();
                        clientOut.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Failed in deleting from buddy2");
                        //e.printStackTrace();
                    } catch (IOException e) {
                        Log.e(TAG, "Failed in deleting from buddy2");
                        //e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        Log.e(TAG, "Failed to read class");
                        //e.printStackTrace();
                    }
                }
            }
			else{
				// Do nothing
			}

			return null;
		}
		public Cursor getCursor(){
		    return clientCursor;
        }
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}

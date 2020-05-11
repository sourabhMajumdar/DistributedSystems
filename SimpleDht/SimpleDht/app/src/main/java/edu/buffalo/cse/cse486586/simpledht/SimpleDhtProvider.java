package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    public class CustomDatabase extends SQLiteOpenHelper{
        public static final String DATABASE_NAME = "KeyValueDatabase2.db";
        public static final String TABLE_NAME = "KeyValueTable";
        public static final String KEY_COL = "key";
        public static final String VALUE_COL = "value";


        public CustomDatabase(Context context){
            super(context,DATABASE_NAME,null,1);

        }

        @Override
        public void onCreate(SQLiteDatabase db) {

            db.execSQL("CREATE TABLE " + TABLE_NAME + " (key TEXT UNIQUE ON CONFLICT REPLACE, value TEXT);");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
            onCreate(db);

        }

        boolean insertData(ContentValues values){
            SQLiteDatabase db = this.getWritableDatabase();
            long row_id = db.insert(TABLE_NAME,null,values);
            if(row_id == -1){
                return false;
            }
            else{
                //Log.e(TAG,"INSERTED INTO THE DATABASE");
                return true;
            }
        }
        Cursor queryDatabase(String[] projection,String selection,String[] selectionArgs, String sortOrder){
            SQLiteDatabase db = this.getReadableDatabase();
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(TABLE_NAME);

            Cursor cursor = queryBuilder.query(db,projection,selection,selectionArgs,null,null,sortOrder,null);
            return cursor;
        }
        int deleteData(Uri uri, String selection, String[] selectionArgs){
            SQLiteDatabase db = this.getWritableDatabase();
            int affectedRows = db.delete(TABLE_NAME,selection,selectionArgs);
            return affectedRows;
        }
    }

    CustomDatabase myDatabase;

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static int key_id = 0;
    static final String ACK = "ACK";
    public String thisServerPort;
    public String myPredecessor = null;
    public String mySuccessor = null;
    public ContentValues valuesToSend = null;
    public boolean forceInsert = false;
    public boolean forceQuery = false;

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    Uri uri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String actualKey;
        if(selection.equals("@") || selection.equals("*")){
            actualKey = null;
        }else{
            actualKey = "key='" + selection + "'";
        }
        int rowsAffected = myDatabase.deleteData(uri,actualKey,selectionArgs);
        
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public BigInteger getValue(String id){
        BigInteger value = new BigInteger(id,16);
        //long maxVal = Math.round(Math.pow(2,32));
        //BigInteger maxValue = BigInteger.valueOf(maxVal);

        //BigInteger completeVal = value.mod(maxValue);

        return value;
        //return completeVal;
    }
    public String getRelation(String id){
        try {
            String nodeHash = genHash(Integer.toString(Integer.parseInt(thisServerPort)/2));
            String idHash = genHash(id);

            //Log.e(TAG,"ID hash is");
            //Log.e(TAG,idHash);

            if(nodeHash.compareToIgnoreCase(idHash) >= 0){
                // Check for predecessor, return contact predecessor if id very low
                if(myPredecessor == null){
                    return "predecessor";
                }else{
                    String predHash = genHash(Integer.toString(Integer.parseInt(myPredecessor)/2));
                    if(idHash.compareToIgnoreCase(predHash) > 0){
                        return "predecessor";
                    }else{
                        return "contactPredecessor";
                    }
                }

            }else{
                // Check for successor, return contact successor if id very high
                if(mySuccessor == null){
                    return "successor";
                }else{
                    String succHash = genHash(Integer.toString(Integer.parseInt(mySuccessor)/2));
                    if(succHash.compareToIgnoreCase(idHash) >= 0){
                        return "successor";
                    }else{
                        return "contactSuccessor";
                    }
                }
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return "None";


    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        /*this is where I recieve the keys I need to check if I have to enter them here or forward them to a new node*/
        /*Define logic to get the correct node*/
        //Log.e(TAG,"I got a value to insert");
        String valuesKey = (String) values.get("key");
        String valuesVal = (String) values.get("value");
        String relation = getRelation(valuesKey);
        //Log.e(TAG,"Relation is");
        //Log.e(TAG,relation);

        try {
            String valueHash = genHash(valuesKey);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(forceInsert){
            //myDatabase.insertData(values);
            /*ContentValues cv = new ContentValues();
            cv.put("key",valuesKey);
            cv.put("value",valuesVal);
            myDatabase.insertData(cv);*/
            myDatabase.insertData(values);
        }else if(mySuccessor == null && myPredecessor == null){

            /*ContentValues cv = new ContentValues();
            cv.put("key",valuesKey);
            cv.put("value",valuesVal);
            myDatabase.insertData(cv);*/
            myDatabase.insertData(values);
        }else {
            if (relation.equals("predecessor")) {
                //Log.e(TAG,"Inserting a value in my database");
                //Log.e(TAG,"This value is a predecessor");
                /*ContentValues cv = new ContentValues();
                cv.put("key",valuesKey);
                cv.put("value",valuesVal);
                myDatabase.insertData(cv);*/
                myDatabase.insertData(values);
            } else {
                //Log.e(TAG,"Sending it to main 11108 to ask for value");
                this.valuesToSend = values;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", thisServerPort);
            }
        }

        //myDatabase.insertData(values);
        return uri;
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        myDatabase = new CustomDatabase(getContext());
        /*Add all the functionality when creating the a socket*/
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        thisServerPort = myPort;

        //Log.e(TAG,"This SERVER PORT");
        //Log.e(TAG,thisServerPort);

        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);
            Log.e(TAG,"Server Created Chill Now");
        } catch (IOException e) {
            Log.e(TAG,"Cannot Create Socket");
            //e.printStackTrace();
        }

        if(!thisServerPort.equals("11108")){
            // Create a client Task that does
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"join",thisServerPort);
        }
        return false;
    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            ServerSocket serverSocket = sockets[0];

            while(true){


                //Log.e(TAG,"We are inside the Server Loop");
                try{

                    Socket socket = serverSocket.accept();

                    // Create Object OutputStream and flush it
                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.flush();
                    ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                    //Log.e(TAG,"Ready to Accept a message");

                    Message clientMessage = (Message) clientIn.readObject();
                    if(clientMessage.protocol.equals("getAll")){

                        // return all values inside this database

                        Log.e(TAG,"Inside getAll protocol");
                        Cursor cursor = getContext().getContentResolver().query(uri,null,"@",null,null);

                        cursor.moveToFirst();
                        while (!cursor.isAfterLast()){
                            String returnKey = cursor.getString(0);
                            String returnVal = cursor.getString(1);

                            Message serverResponse = new Message("acceptValue",returnKey,returnVal,null,null);

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();

                            cursor.moveToNext();
                        }

                        cursor.close();
                        Log.e(TAG,"All values returned");

                        Message finalServerResponse = new Message(ACK,null,null,null,null);

                        clientOut.writeObject(finalServerResponse);
                        clientOut.flush();

                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    }else if(clientMessage.protocol.equals("getSuccessor")){

                        Log.e(TAG,"Client asked for successor");

                        Message serverResponse = new Message(ACK,null,null,mySuccessor,null);

                        clientOut.writeObject(serverResponse);
                        clientOut.flush();

                        clientIn.close();
                        clientOut.close();
                        socket.close();
                    }else if(clientMessage.protocol.equals("getPredecessor")){

                        Log.e(TAG,"Client asked for predecessor");
                        Message serverResponse = new Message(ACK,null,null,myPredecessor,null);

                        clientOut.writeObject(serverResponse);
                        clientOut.flush();

                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    }else if(clientMessage.protocol.equals("getLastQuery")){

                        Log.e(TAG,"Inside Last Query Protocol");
                        if(myPredecessor == null){

                            Log.e(TAG,"No More Predecessor, it has to be with me");

                            forceQuery = true;

                            String valueKey = clientMessage.keyString;

                            //String actualKey = "key='" + valueKey + "'";


                            Cursor resultCursor = getContext().getContentResolver().query(uri,null,valueKey,null,null);
                            if (resultCursor == null) {
                                Log.e(TAG, "Result null");
                                throw new Exception();
                            }

                            int keyIndex = resultCursor.getColumnIndex("key");
                            int valueIndex = resultCursor.getColumnIndex("value");
                            if (keyIndex == -1 || valueIndex == -1) {
                                Log.e(TAG, "Wrong columns");
                                resultCursor.close();
                                throw new Exception();
                            }

                            resultCursor.moveToFirst();

                            if (!(resultCursor.isFirst() && resultCursor.isLast())) {
                                Log.e(TAG, "Wrong number of rows");
                                resultCursor.close();
                                throw new Exception();
                            }

                            String returnKey = resultCursor.getString(keyIndex);
                            String returnValue = resultCursor.getString(valueIndex);

                            Message serverResponse = new Message(ACK,valueKey,returnValue,null,null);
                            //serverResponse.cursor = resultCursor;

                            resultCursor.close();



                            clientOut.writeObject(serverResponse);

                            clientIn.close();
                            clientOut.close();
                            socket.close();

                            forceQuery = false;

                        }else{

                            Log.e(TAG,"I have a predecessor and he is");
                            Log.e(TAG,myPredecessor);
                            Message serverResponse = new Message("contact",null,null,myPredecessor,null);
                            clientOut.writeObject(serverResponse);
                            clientOut.flush();
                        }

                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    }else if(clientMessage.protocol.equals("query")){

                        Log.e(TAG,"Inside Query Protocol");

                        String valueKey = clientMessage.keyString;
                        String relation = getRelation(valueKey);

                        Log.e(TAG,"Relation of this query is");
                        Log.e(TAG,relation);
                        if(relation.equals("predecessor")){
                            // find the cursor col
                            Cursor resultCursor = getContext().getContentResolver().query(uri,null,valueKey,null,null);
                            String valToFind;
                            Message serverResponse;

                            resultCursor.moveToFirst();
                            while(!resultCursor.isAfterLast()){
                                String keyEntity = resultCursor.getString(0);
                                String valEntity = resultCursor.getString(1);

                                if(valueKey.equals(keyEntity)){
                                    serverResponse = new Message(ACK,valueKey,valEntity,null,null);

                                    clientOut.writeObject(serverResponse);
                                    clientOut.flush();

                                    break;
                                }

                                resultCursor.moveToNext();
                            }

                            resultCursor.close();
                            clientIn.close();
                            clientOut.close();
                            socket.close();


                        }else if(relation.equals("successor") || relation.equals("contactSuccessor")){

                            Message serverResponse;
                            if(mySuccessor != null){
                                serverResponse = new Message("contact",null,null,mySuccessor,null);
                            }else{
                                serverResponse = new Message("loopBackQuery",null,null,null,null);
                            }


                            clientOut.writeObject(serverResponse);
                            clientOut.flush();
                        }else{
                            Message serverResponse = new Message("contact",null,null,myPredecessor,null);

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();
                        }
                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    }else if(clientMessage.protocol.equals("insertLast")){

                        //Log.e(TAG,"Got Insert Last Protocol");
                        Message serverResponse;
                        if(myPredecessor == null){

                            //Log.e(TAG,"I don't have a predecessor");



                            //Log.e(TAG,"InsertLast Protocol and I dont have a predecessor");

                            forceInsert = true;
                            ContentValues cv = new ContentValues();
                            cv.put("key",clientMessage.keyString);
                            cv.put("value",clientMessage.valString);

                            getContext().getContentResolver().insert(uri,cv);

                            forceInsert = false;
                            serverResponse = new Message(ACK,null,null,null,null);
                            clientOut.writeObject(serverResponse);
                            clientOut.flush();

                        }else{

                            //Log.e(TAG,"Insert Last Protocol and I sent my Predecessor");
                            //Log.e(TAG,myPredecessor);
                            serverResponse = new Message(ACK,null,null,myPredecessor,null);
                            clientOut.writeObject(serverResponse);
                            clientOut.flush();
                        }


                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    }
                    else if(clientMessage.protocol.equals("insert")){

                        //Log.e(TAG,"This is the INSERT protocol");
                        // Log.e(TAG,"Got Insert protocol from");
                       //Log.e(TAG,clientMessage.portId);

                        String valuesKey = clientMessage.keyString;
                        String relation = getRelation(valuesKey);

                        //Log.e(TAG,"It's relation is");
                        //Log.e(TAG,relation);

                        if(relation.equals("predecessor")){
                            //myPredecessor = clientMessage.portId;

                            ContentValues cv = new ContentValues();
                            cv.put("key",clientMessage.keyString);
                            cv.put("value",clientMessage.valString);

                            //Log.e(TAG,"Inserting value from");
                            //Log.e(TAG,clientMessage.portId);

                            getContext().getContentResolver().insert(uri,cv);

                            Message serverResponse = new Message("predecessor",null,null,null,null);

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();

                        }else if(relation.equals("successor")){

                            //mySuccessor = clientMessage.portId;
                            Message serverResponse;
                            if(mySuccessor == null){
                                //Log.e(TAG,"There is a message that has reached the end of it's journey");

                                /*forceInsert = true;
                                ContentValues cv = new ContentValues();
                                cv.put("key",clientMessage.keyString);
                                cv.put("value",clientMessage.valString);

                                //Log.e(TAG,"Inserting value from");
                                //Log.e(TAG,clientMessage.portId);

                                getContext().getContentResolver().insert(uri,cv);
                                forceInsert = false;*/

                                serverResponse = new Message("loopBack",null,null,null,null);
                            }
                            else {
                                serverResponse = new Message("contact", null, null, mySuccessor, null);
                            }

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();

                        }else if (relation.equals("contactPredecessor")){
                            Message serverResponse = new Message("contact",null,null,myPredecessor,null);

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();
                        }else{
                            Message serverResponse = new Message("contact",null,null,mySuccessor,null);

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();
                        }
                        clientIn.close();
                        clientOut.close();
                        socket.close();
                    }else if (clientMessage.protocol.equals("join")){
                        // Here I need the portId

                        //Log.e(TAG,"Join Protocol from");
                        //Log.e(TAG,clientMessage.portId);

                        //Log.e(TAG,"This is the INSERT protocol");
                        String valuesKey = Integer.toString(Integer.parseInt(clientMessage.portId)/2);

                        String relation = getRelation(valuesKey);

                        //Log.e(TAG,"It's relation is");
                        //Log.e(TAG,relation);
                        if(relation.equals("predecessor")){

                            Message serverResponse = new Message("predecessor",null,null,null,myPredecessor);

                            myPredecessor = clientMessage.portId;

                            //Log.e(TAG,"My Predecessor is");
                            //Log.e(TAG,Integer.toString(Integer.parseInt(myPredecessor)/2));

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();

                        }else if(relation.equals("successor")){

                            Message serverResponse = new Message("successor",null,null,null,mySuccessor);
                            mySuccessor = clientMessage.portId;

                            Log.e(TAG,"My Successor is");
                            Log.e(TAG,Integer.toString(Integer.parseInt(mySuccessor)/2));

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();
                        }else if (relation.equals("contactPredecessor")){
                            Message serverResponse = new Message("contact",null,null,myPredecessor,null);

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();
                        }else{
                            Message serverResponse = new Message("contact",null,null,mySuccessor,null);

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();
                        }

                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    }else if(clientMessage.protocol.equals("changeSuccessor")){
                        mySuccessor = clientMessage.portId;

                        //Log.e(TAG,"Changed Successor to");
                        //Log.e(TAG,Integer.toString(Integer.parseInt(mySuccessor)/2));

                        Message serverResponse = new Message(ACK,null,null,null,null);

                        clientOut.writeObject(serverResponse);
                        clientOut.flush();

                        clientIn.close();
                        clientOut.close();
                        socket.close();

                    }else if(clientMessage.protocol.equals("changePredecessor")){


                        myPredecessor = clientMessage.portId;

                        //Log.e(TAG,"Changed Predecessor to");
                        //Log.e(TAG,Integer.toString(Integer.parseInt(myPredecessor)/2));

                        Message serverResponse = new Message(ACK,null,null,null,null);

                        clientOut.writeObject(serverResponse);
                        clientOut.flush();

                        clientIn.close();
                        clientOut.close();
                        socket.close();
                    }

                    else{
                        socket.close();
                        break;
                    }

                } catch (IOException e) {
                    Log.e(TAG,"Caught a Null Ptr");
                    //e.printStackTrace();
                }catch (ClassNotFoundException cle){
                    //cle.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
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
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub


        //Log.e(TAG,selection);
        String actualKey;
        Cursor cursor = null;
        if(forceQuery){
            actualKey = "key='" + selection + "'";
            cursor = myDatabase.queryDatabase(projection,actualKey,selectionArgs,sortOrder);
            cursor.setNotificationUri(getContext().getContentResolver(),uri);
        }else if(selection.equals("@")) {
            actualKey = null;
            cursor = myDatabase.queryDatabase(projection,actualKey,selectionArgs,sortOrder);
            cursor.setNotificationUri(getContext().getContentResolver(),uri);

        }else if(selection.equals("*")){
            actualKey = null;
            Cursor presentCursor = null;
            ArrayList<Cursor> cursorList = new ArrayList<Cursor>();
            presentCursor = myDatabase.queryDatabase(projection,actualKey,selectionArgs,sortOrder);
            presentCursor.setNotificationUri(getContext().getContentResolver(),uri);

            if(presentCursor != null){

                Log.e(TAG,"I have values and I am adding them");
                cursorList.add(presentCursor);
            }
            // now I also need to get other cursors as well
            Cursor successorCursor = null;
            Cursor predecessorCursor = null;


            if(mySuccessor != null){
                //Log.e(TAG,"My Successor has values and I am adding them");
                successorCursor = getAllValues("successor");
                cursorList.add(successorCursor);
            }

            if(myPredecessor != null){
                //Log.e(TAG,"My Predecessor has values and I am adding them");
                predecessorCursor = getAllValues("predecessor");
                cursorList.add(predecessorCursor);
            }

            cursor = new MergeCursor(cursorList.toArray(new Cursor[0]));


        }else if(myPredecessor == null && mySuccessor == null){
            actualKey = "key='" + selection + "'";
            cursor = myDatabase.queryDatabase(projection,actualKey,selectionArgs,sortOrder);
            cursor.setNotificationUri(getContext().getContentResolver(),uri);
        }else{
            String relation = getRelation(selection);
            if(relation.equals("predecessor")){
                actualKey = "key='" + selection + "'";
                cursor = myDatabase.queryDatabase(projection,actualKey,selectionArgs,sortOrder);
                cursor.setNotificationUri(getContext().getContentResolver(),uri);
                //MatrixCursor cursor1 = new MatrixCursor();
            }
            else{
                // send the query request to the node
                // question is how can I send a cursor object across via message;
                // cursor = getQuery(selection)
                //Log.e(TAG,"Asking server for the value");
                cursor = getQuery(selection);

            }
        }

        Log.v("query",selection);

        //return null;
        return cursor;
    }

    public MatrixCursor getQuery(String selection){
        // ask 11108 who is the guy to contact
        Message clientMessage = new Message("query",selection,null,null,thisServerPort);
        int contactNode = 11108;
        String selectionVal;
        while(true){
            try{
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), contactNode);

                ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                serverOut.flush();
                ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

                serverOut.writeObject(clientMessage);
                serverOut.flush();

                Message serverResponse = (Message) serverIn.readObject();
                if(serverResponse.protocol.equals("loopBackQuery")){
                    contactNode = 11108;
                    clientMessage.protocol = "getLastQuery";
                }else {
                    if (serverResponse.nodeToContact == null) {
                        // This is the node to contact
                        // I expect the server responsible to return the value
                        selectionVal = serverResponse.valString;
                        serverIn.close();
                        serverOut.close();
                        socket.close();
                        break;
                    } else {
                        Log.e(TAG,"Server Asks to contact");
                        Log.e(TAG,serverResponse.nodeToContact);
                        contactNode = Integer.parseInt(serverResponse.nodeToContact);
                    }
                }
            } catch (UnknownHostException e) {
                //e.printStackTrace();
            } catch (IOException e) {
                //e.printStackTrace();
                Log.e(TAG,"Client Socket Broke");
            } catch (ClassNotFoundException e) {
                //e.printStackTrace();
            }
        }

        // form a cursor object and return selection, it's selection value
        MatrixCursor cursor = new MatrixCursor(new String[] {"key","value"});
        cursor.addRow(new String[] {selection,selectionVal});

        return cursor;

    }

    public MatrixCursor getAllValues(String direction){
        // this function is tasked with getting all the values from all the databases
        // first we need to contact all successors then we contact all predecessors
        int contactNode;
        MatrixCursor cursor = new MatrixCursor(new String[] {"key","value"});


        String finalRequest;
        if(direction.equals("successor")){
            // contact all successors
            contactNode = Integer.parseInt(mySuccessor);
            finalRequest = "getSuccessor";

        }else{
            contactNode = Integer.parseInt(myPredecessor);
            finalRequest = "getPredecessor";
        }
        while(true){
            // try to open a socket and retrieve all the values from it
            try{
                //Log.e(TAG,"My Contact Node is");
                //Log.e(TAG,Integer.toString(contactNode));

                Message clientMessage = new Message("getAll",null,null,null,null);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), contactNode);

                ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                serverOut.flush();
                ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

                serverOut.writeObject(clientMessage);
                serverOut.flush();

                while(true){

                    //Log.e(TAG,"Stuck inside the loop");
                    Message serverResponse = (Message) serverIn.readObject();
                    if(serverResponse.protocol.equals(ACK)){
                        //Log.e(TAG,"Time to move on");
                        serverIn.close();
                        serverOut.close();
                        socket.close();
                        break;
                    }else{
                        // server has send a key value pair add it to your matrix cursor
                        cursor.addRow(new String[] {serverResponse.keyString,serverResponse.valString});
                    }
                }

                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), contactNode);

                ObjectOutputStream serverOut2 = new ObjectOutputStream(socket2.getOutputStream());
                serverOut2.flush();
                ObjectInputStream serverIn2 = new ObjectInputStream(socket2.getInputStream());

                //Log.e(TAG,"Out of the loop");
                Message clientMessage2 = new Message(finalRequest,null,null,null,null);

                // after this you need to ask the server to get it's successor
                //clientMessage.protocol = finalRequest;

                serverOut2.writeObject(clientMessage2);
                serverOut2.flush();

                //Log.e(TAG,"Waiting for serverResponse");

                Message finalServerResponse = (Message) serverIn2.readObject();

                //Log.e(TAG,"Received a response from server");

                if (finalServerResponse.nodeToContact == null){
                    Log.e(TAG,"Server Says there is no successor");
                    serverIn2.close();
                    serverOut2.close();
                    socket2.close();
                    break;
                }else {

                    contactNode = Integer.parseInt(finalServerResponse.nodeToContact);
                    //Log.e(TAG,"Server Says contact node is");
                    //Log.e(TAG,Integer.toString(contactNode));
                    //clientMessage.protocol = "getAll";
                    serverIn2.close();
                    serverOut2.close();
                    socket2.close();
                }
            } catch (UnknownHostException e) {
                //e.printStackTrace();
                //Log.e(TAG,"Could not create a socket");
            } catch (StreamCorruptedException e) {
                //e.printStackTrace();
                //Log.e(TAG,"Stream Broke");
            } catch (IOException e) {
                //e.printStackTrace();
                //Log.e(TAG,"Socket Couldnt Create");
            } catch (ClassNotFoundException e) {
                //e.printStackTrace();
                //Log.e(TAG,"cLASS not Found");
            }
        }

        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {

            String action = msgs[0];
            //Log.e(TAG,"Action is");
            //Log.e(TAG,action);
            if(action.equals("insert")){
                // Create a message called insert
                //Log.e(TAG,"Atleast I caught the insert action");

                String keyString = (String) valuesToSend.get("key");
                String valString = (String) valuesToSend.get("value");

                //Log.e(TAG,"Got a new value to insert");
                Message clientMessage = new Message("insert",keyString,valString,null,thisServerPort);

                // stay inside the loop untill the message is delivered
                //int contactNode = Integer.parseInt(thisServerPort);
                int contactNode = 11108;
                //int contactNode = Integer.parseInt(msgs[1]);
                while(true){
                    try {
                        //Log.e(TAG,"Insert call to ");
                        //Log.e(TAG,Integer.toString(contactNode));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), contactNode);

                        //Log.e(TAG,"Socket to server created successfully");

                        ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                        serverOut.flush();
                        ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

                        //Log.e(TAG,"Created Streams successfully");

                        serverOut.writeObject(clientMessage);

                        //Log.e(TAG,"Awaiting Server Response");

                        Message serverResponse = (Message) serverIn.readObject();

                        //Log.e(TAG,"Got a server Response");
                        if(serverResponse.protocol.equals("loopBack")){

                            //Log.e(TAG,"LoopBack Protocol Initiated");
                            clientMessage.protocol = "insertLast";
                            contactNode = 11108;
                        }else {
                            if (serverResponse.nodeToContact == null) {

                                //Log.e(TAG,"Server Accepts this value");
                                serverIn.close();
                                serverOut.close();
                                socket.close();
                                break;
                            } else {
                                //Log.e(TAG,"Server Asks to contact");
                                //Log.e(TAG,serverResponse.nodeToContact);

                                contactNode = Integer.parseInt(serverResponse.nodeToContact);
                            }
                        }

                    } catch (IOException e) {
                        Log.e(TAG,"Server Socket broke");
                        break;
                        //e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        //e.printStackTrace();
                    }
                }
            }else if(action.equals("join")){
                // find where to add
                Message clientMessage = new Message("join",null,null,null,thisServerPort);
                int contactNode = 11108;
                //int prevNode = 11108;
                String changeProtocol;
                String changeNode = null;
                while(true){
                    try {

                        Log.e(TAG,"Join call to");
                        Log.e(TAG,Integer.toString(contactNode));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), contactNode);

                        ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                        serverOut.flush();
                        ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

                        serverOut.writeObject(clientMessage);

                        Message serverResponse = (Message) serverIn.readObject();

                        if(serverResponse.nodeToContact == null){
                            if(serverResponse.protocol.equals("successor")){
                                //Log.e(TAG,"I am the successor");
                                myPredecessor = Integer.toString(contactNode);

                                // I also need to tell the contactNodes predecessor that you have a new successor
                               //Log.e(TAG,"My Predecessor is");
                               //Log.e(TAG,myPredecessor);

                                changeProtocol = "changePredecessor";
                                if(serverResponse.portId != null) {
                                    mySuccessor = serverResponse.portId;
                                    changeNode = serverResponse.portId;
                                }else{
                                    //Log.e(TAG,"Server Says I am the predecessor and I have no one to contact");
                                }

                            }else {
                                //Log.e(TAG,"I am Predecessor");
                                // I need to tell the successor Node that you have a new predecessor
                                changeProtocol = "changeSuccessor";
                                if(serverResponse.portId != null) {
                                    myPredecessor = serverResponse.portId;
                                    changeNode = serverResponse.portId;
                                }
                                //changeNode = serverResponse.portId;
                                mySuccessor = Integer.toString(contactNode);

                                //Log.e(TAG,"My Successor is");
                                //Log.e(TAG,mySuccessor);
                            }
                            serverIn.close();
                            serverOut.close();
                            socket.close();
                            break;
                        }
                        else{
                            Log.e(TAG,"Server Asks to contact this node to join");
                            Log.e(TAG,serverResponse.nodeToContact);
                            contactNode = Integer.parseInt(serverResponse.nodeToContact);
                        }
                    } catch (IOException e) {
                        //e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        //e.printStackTrace();
                    }

                }

                Message clientMessage2 = new Message(changeProtocol,null,null,null,thisServerPort);

                if(changeNode != null) {
                    //Log.e(TAG,"Informing the this node regarding the change");
                    //Log.e(TAG,changeNode);
                    //Log.e(TAG,"Change Protocol is");
                    //Log.e(TAG,changeProtocol);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(changeNode));

                        ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                        serverOut.flush();
                        ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

                        serverOut.writeObject(clientMessage2);
                        serverOut.flush();

                        while (true) {
                            Message serverResponse = (Message) serverIn.readObject();
                            if (serverResponse.protocol.equals(ACK)) {
                                serverIn.close();
                                serverOut.close();
                                socket.close();
                                break;
                            }
                        }
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (StreamCorruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
            else{
                // Do nothing
            }

            return null;
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

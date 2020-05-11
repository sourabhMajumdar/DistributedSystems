package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final int SERVER_PORT = 10000;
    static int key_id = 0;
    static final String ACK = "ACK";
    static int currentSeqNum = 0;
    static int currentMaxProposedNum = 0;
    static int failedNode = -1;
    String thisServerPort;
    static int timeoutlimit = 8000;
    public Set<String> failedMessages = new HashSet<String>();
    public int failedNodeCount = 0;
    public int msgsRcv = 0;
    public int msgsPub = 0;
    static int msgNumber = 0;
    //public Hashtable<String,Boolean> publishedMessages = new Hashtable<String, Boolean>();
    //public Hashtable<String,Integer> publishedId = new Hashtable<String, Integer>();
    static HashMap<String, Integer> messagePrevProposedSeqNum = new HashMap<String, Integer>();
    public ArrayList<String> publishedMessages = new ArrayList<String>();
    public ArrayList<String> setMessages = new ArrayList<String>();
    public ArrayList<Message> deliveredMessages = new ArrayList<Message>();

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    Uri uri = buildUri("content","edu.buffalo.cse.cse486586.groupmessenger2.provider");


    public class MessageComparator implements Comparator<Message>{

        @Override
        public int compare(Message firstMessage,Message otherMessage) {
            if(Integer.compare(firstMessage.proposedSequenceNum,otherMessage.proposedSequenceNum) == 0){
                if(Integer.compare(firstMessage.senderPort,otherMessage.senderPort) == 0){
                    return Integer.compare(firstMessage.messageNumber,otherMessage.messageNumber);
                }
                else{
                    return Integer.compare(firstMessage.senderPort,otherMessage.senderPort);
                }
            }
            else {
                return Integer.compare(firstMessage.proposedSequenceNum, otherMessage.proposedSequenceNum);
            }
        }
    }
   // public PriorityQueue<Message> messagePriorityQueue = new PriorityQueue<Message>(10,new MessageComparator());
    public PriorityBlockingQueue<Message> messagePriorityQueue = new PriorityBlockingQueue<Message>(10,new MessageComparator());



    public int customMax(int i, int j){
        return i > j ? i : j;
    }
    public void removeFailedMessages(int failedPort){
        // remvoe all messages from the failed node
        //Log.e(TAG,"REMOVING FAILED MESSAGES");
        ArrayList<Message> failedMessages = new ArrayList<Message>();
        Iterator<Message> ity = messagePriorityQueue.iterator();
        while(ity.hasNext()){
            Message ityMessage = ity.next();
            if(ityMessage.senderPort == failedPort && ityMessage.isDeliverable == false) {
                failedMessages.add(ityMessage);
            }
        }

        // now remove all messages in the failedMessages from the priority queue
        Iterator<Message> failIt = failedMessages.iterator();
        while (failIt.hasNext()){
            Message failM = failIt.next();
            messagePriorityQueue.remove(failM);
        }


    }
    public ArrayList<Message> giveFailedNodeMessages(int failedNode){
        ArrayList<Message> failedNodeMessages = new ArrayList<Message>();
        Iterator<Message> ftm = deliveredMessages.iterator();
        while(ftm.hasNext()){
            Message tmpMessage = ftm.next();
            if(tmpMessage.senderPort == failedNode){
                failedNodeMessages.add(tmpMessage);
            }
        }
        return failedNodeMessages;
    }


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * Allow this Activity to use a layout file that defines what UI elements to use.
         * Please take a look at res/layout/main.xml to see how the UI elements are defined.
         *
         * R is an automatically generated class that contains pointers to statically declared
         * "resources" such as UI elements and strings. For example, R.layout.main refers to the
         * entire UI screen declared in res/layout/main.xml file. You can find other examples of R
         * class variables below.
         */

        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        thisServerPort = myPort;

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         * Retrieve a pointer to the input box (EditText) defined in the layout
         * XML file (res/layout/main.xml).
         *
         * This is another example of R class variables. R.id.edit_text refers to the EditText UI
         * element declared in res/layout/main.xml. The id of "edit_text" is given in that file by
         * the use of "android:id="@+id/edit_text""
         */
        final EditText editText = (EditText) findViewById(R.id.editText1);
        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final Button sendButton = (Button) findViewById(R.id.button4);
        sendButton.setOnClickListener(new View.OnClickListener(){
            public  void onClick(View v){
                // send the messages
                String msg = editText.getText().toString() + "\n";
                editText.setText("");
                TextView localTextView = (TextView) findViewById(R.id.textView1);
                localTextView.append("\t" + msg); // This is one way to display a string.
                TextView remoteTextView = (TextView) findViewById(R.id.textView1);

                remoteTextView.append("\n");

                msgNumber += 1;

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg,myPort);
                return;
            }
        });
    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        public void publishQueuedMessages(){

            //removeFailedMessages(failedNode);

            if(!messagePriorityQueue.isEmpty()) {

                // determine if all messages are ready to be delivered
                boolean deliverAll = true;
                Iterator<Message> dtm = messagePriorityQueue.iterator();
                while (dtm.hasNext()){
                    Message tmpMessage = dtm.next();
                    if(tmpMessage.isDeliverable == false){
                        deliverAll = false;
                        break;
                    }
                }

                if(deliverAll) {
                    Message topMessage = messagePriorityQueue.peek();


                    if (topMessage.isDeliverable == false) {
                        Log.e(TAG, "Undeliverable message is from ");
                        Log.e(TAG, Integer.toString(topMessage.senderPort));
                    }


                    while (topMessage.isDeliverable) {
                        deliveredMessages.add(topMessage);
                        publishedMessages.add(topMessage.message);
                        publishProgress(topMessage.message);
                        Log.e(TAG, Integer.toString(topMessage.proposedSequenceNum));


                        messagePriorityQueue.poll();
                        if (messagePriorityQueue.isEmpty()) {
                            break;
                        } else {
                            topMessage = messagePriorityQueue.peek();
                            if (topMessage.isDeliverable == false) {
                                Log.e(TAG, "Undeliverable message is from ");
                                Log.e(TAG, Integer.toString(topMessage.senderPort));
                            }
                        }
                    }
                }
            }
            else{
                Log.e(TAG,"No messages queued");
            }
        }


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

            while(true){
                try{
                    Socket socket = serverSocket.accept();

                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.flush();
                    ObjectInputStream clientIn = new ObjectInputStream(socket.getInputStream());

                    Message clientMessage = (Message) clientIn.readObject();
                    if(clientMessage.protocol.matches("propose")){
                        currentMaxProposedNum = customMax(currentMaxProposedNum,clientMessage.proposedSequenceNum) + 1;
                        Message serverResponse = new Message(ACK,
                                "acknoledgment",
                                Integer.parseInt(thisServerPort),
                                clientMessage.senderPort,
                                currentMaxProposedNum,
                                -1,
                                false,
                                failedNode);

                        clientOut.writeObject(serverResponse);
                        clientOut.flush();

                        messagePriorityQueue.add(clientMessage);
                    }
                    else if(clientMessage.protocol.matches("add")){
                        // find the client message and replace it
                        if(!publishedMessages.contains(clientMessage.message)) {
                            Message messageToFind = null;
                            Iterator<Message> messageFinder = messagePriorityQueue.iterator();
                            while (messageFinder.hasNext()) {
                                Message tMessage = messageFinder.next();
                                if (tMessage.message.equals(clientMessage.message)) {
                                    messageToFind = tMessage;
                                    break;
                                }
                            }

                            clientMessage.protocol = "set";


                            if (messageToFind == null) {
                                Log.e(TAG, "Server did not find the message, it can happen");
                                messagePriorityQueue.add(clientMessage);
                            } else {
                                messagePriorityQueue.remove(messageToFind);
                                messagePriorityQueue.add(clientMessage);
                            }

                            // send an ack saying it has recieved the message
                            Message serverResponse = new Message(ACK,
                                    "message set",
                                    Integer.parseInt(thisServerPort),
                                    clientMessage.senderPort,
                                    -1,
                                    -1,
                                    false,
                                    failedNode);

                            clientOut.writeObject(serverResponse);
                            clientOut.flush();
                        }

                        publishQueuedMessages();
                    }
                    else if(clientMessage.protocol.matches("set")){
                        // find the client message and replace it
                        Message messageToFind = null;
                        Iterator<Message> messageFinder = messagePriorityQueue.iterator();
                        while(messageFinder.hasNext()){
                            Message tMessage = messageFinder.next();
                            if(tMessage.message.equals(clientMessage.message)){
                                messageToFind = tMessage;
                                break;
                            }
                        }

                        if(messageToFind == null){
                            Log.e(TAG,"Server did not find the message, it shouldnt happen");
                            messagePriorityQueue.add(clientMessage);
                        }
                        else{
                            messagePriorityQueue.remove(messageToFind);
                            messagePriorityQueue.add(clientMessage);
                        }

                        // send an ack saying it has recieved the message
                        Message serverResponse = new Message(ACK,
                                "message set",
                                Integer.parseInt(thisServerPort),
                                clientMessage.senderPort,
                                -1,
                                -1,
                                false,
                                failedNode);

                        clientOut.writeObject(serverResponse);
                        clientOut.flush();

                        // publish any remaining messages
                        publishQueuedMessages();
                    }
                    else{
                        // I think I received a null, therefore closing the socket
                        socket.close();
                        break;
                    }
                    clientIn.close();
                    clientOut.close();
                    socket.close();
                } catch (IOException e) {
                    Log.e(TAG,"server failed to create socket");
                    //e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    Log.e(TAG,"server failed to read class object");
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
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append("\n");


            //publishedMessages.add(strReceived);
            msgsPub += 1;

            ContentValues keyValueToInsert = new ContentValues();
            keyValueToInsert.put("key",Integer.toString(key_id));
            keyValueToInsert.put("value",strReceived);

            //Log.e(TAG,"Key Inserted");
            Log.e(TAG,"Entries are");
            Log.e(TAG,strReceived);
            Log.e(TAG,Integer.toString(key_id));


            //publishedMessages.put(strReceived,true);
            //publishedId.put(strReceived,key_id);

            uri = getContentResolver().insert(uri,keyValueToInsert);
            key_id += 1;

            //Log.e(TAG,"Messages Published");
            //Log.e(TAG,Integer.toString(msgsPub));

            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */



            String filename = "GroupMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;

            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }

            return;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {

            currentSeqNum = currentMaxProposedNum;
            int maxAgreedSeqNum = currentSeqNum;
            String msgToSend = msgs[0];
            int currentMessageNum = msgNumber;

            for(int i = 11108;i <= 11124;i = i + 4){
                try{
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i);

                    ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                    serverOut.flush();
                    ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

                    // it is the propose phase i.e. 1
                    Message clientMessage = new Message("propose",
                            msgToSend,
                            Integer.parseInt(thisServerPort),
                            i,
                            maxAgreedSeqNum,
                            currentMessageNum,
                            false,
                            failedNode);

                    serverOut.writeObject(clientMessage);
                    serverOut.flush();


                    while(true){
                        socket.setSoTimeout(timeoutlimit);
                        Message serverResponse = (Message) serverIn.readObject();
                        if(serverResponse.protocol.matches(ACK)){
                            maxAgreedSeqNum = customMax(maxAgreedSeqNum,serverResponse.proposedSequenceNum);
                            currentSeqNum = maxAgreedSeqNum;
                            serverIn.close();
                            serverOut.close();
                            socket.close();
                            break;
                        }
                    }
                } catch (UnknownHostException e) {
                    Log.e(TAG,"Client Says unknown host exception");
                    failedNode = i;
                    maxAgreedSeqNum = currentSeqNum;
                    removeFailedMessages(failedNode);
                    //e.printStackTrace();
                } catch (IOException e) {
                    Log.e(TAG,"Client Says IO Exception");
                    failedNode = i;
                    maxAgreedSeqNum = currentSeqNum;
                    removeFailedMessages(failedNode);
                    //e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    Log.e(TAG,"Client Failed to read class from the server");
                    failedNode = i;
                    maxAgreedSeqNum = currentSeqNum;
                    removeFailedMessages(failedNode);
                    //e.printStackTrace();
                }
            }

            // now we need to send the final priority to all the servers
            for(int i = 11108;i <= 11124;i = i + 4){
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i);

                    ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                    serverOut.flush();
                    ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());


                    Message finalClientMessage = new Message("set",
                            msgToSend,
                            Integer.parseInt(thisServerPort),
                            i,
                            maxAgreedSeqNum,
                            currentMessageNum,
                            true,
                            failedNode);

                    serverOut.writeObject(finalClientMessage);
                    serverOut.flush();

                    while(true){
                        socket.setSoTimeout(timeoutlimit);

                        Message serverResponse = (Message) serverIn.readObject();
                        if(serverResponse.protocol.matches(ACK)){

                            serverIn.close();
                            serverOut.close();
                            socket.close();
                            break;
                        }
                    }


                } catch (UnknownHostException e) {
                    Log.e(TAG,"Client Says unknown host exception while sending final priority");
                    failedNode = i;
                    removeFailedMessages(failedNode);
                    //e.printStackTrace();
                }catch (IOException e) {
                    Log.e(TAG,"Client catches IO Exception while sending final priority");
                    failedNode = i;
                    removeFailedMessages(failedNode);
                    //e.printStackTrace();
                }catch (ClassNotFoundException e) {
                    Log.e(TAG,"Client Failed to read class from the server while sending final priority");
                    failedNode = i;
                    removeFailedMessages(failedNode);
                    //e.printStackTrace();
                }
            }

            if(failedNode > -1){
                ArrayList<Message> failedNodeMessages = giveFailedNodeMessages(failedNode);
                Iterator<Message> ftm = failedNodeMessages.iterator();
                while(ftm.hasNext()){
                    Message finalMessage = ftm.next();

                    // now we need to send the final priority to all the servers
                    for(int i = 11108;i <= 11124;i = i + 4){
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i);

                            ObjectOutputStream serverOut = new ObjectOutputStream(socket.getOutputStream());
                            serverOut.flush();
                            ObjectInputStream serverIn = new ObjectInputStream(socket.getInputStream());

                            finalMessage.protocol = "add";
                            finalMessage.receiverPort = Integer.parseInt(thisServerPort);

                            serverOut.writeObject(finalMessage);
                            serverOut.flush();

                            while(true){
                                socket.setSoTimeout(timeoutlimit);

                                Message serverResponse = (Message) serverIn.readObject();
                                if(serverResponse.protocol.matches(ACK)){

                                    serverIn.close();
                                    serverOut.close();
                                    socket.close();
                                    break;
                                }
                            }


                        } catch (UnknownHostException e) {
                            Log.e(TAG,"Client Says unknown host exception while sending deliverable failed messages");
                            //failedNode = i;
                            //removeFailedMessages(failedNode);
                            //e.printStackTrace();
                        }catch (IOException e) {
                            Log.e(TAG,"Client catches IO Exception while sending deliverable failed messages");
                            //failedNode = i;
                            //removeFailedMessages(failedNode);
                            //e.printStackTrace();
                        }catch (ClassNotFoundException e) {
                            Log.e(TAG,"Client Failed to read class from the server while sending deliverable failed messagess");
                            //failedNode = i;
                            //removeFailedMessages(failedNode);
                            //e.printStackTrace();
                        }
                    }
                }
            }
            Log.e(TAG,"Message Number sent");
            Log.e(TAG,Integer.toString(currentMessageNum));
            return null;
        }
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
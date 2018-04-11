package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtDbHelper;
import edu.buffalo.cse.cse486586.simpledht.model.Message;
import edu.buffalo.cse.cse486586.simpledht.model.MessageType;

import static android.content.Context.TELEPHONY_SERVICE;
import static edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtContract.BASE_CONTENT_URI;
import static edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtContract.SimpleDhtEntry.KEY_FIELD;
import static edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtContract.SimpleDhtEntry.TABLE_NAME;
import static edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtContract.SimpleDhtEntry.VALUE_FIELD;
import static edu.buffalo.cse.cse486586.simpledht.model.Message.DELIMITER;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.DELETE;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.DELETE_ALL;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.INSERT;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.JOIN;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.JOIN_REQUEST;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.QUERY;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.QUERY_ALL;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.getEnumBy;

/*
 * Reference: Simple Chord Algorithm - https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf
 * Reference: Indranil Gupta - https://www.youtube.com/watch?v=q29szpcnorA&list=PLFd87qVsaLhOkTLvfp6MC94iFa_1c9wrU&index=31
 */

public class SimpleDhtProvider extends ContentProvider {

    public static final String LOG_TAG = SimpleDhtProvider.class.getSimpleName();

    private SimpleDhtDbHelper mDbHelper;

    private ContentResolver mContentResolver;

    private static final String LEADER_NODE = "5554"; //Node where all joins will take place
    private static final int SERVER_PORT = 10000;
    private static final String LDUMP = "@"; //Specific AVD
    private static final String GDUMP = "*"; //All AVDs
    private String nodeID = null; //Used to store hash of self
    private String selfPort = null; //Used to store the port number of the AVD
    private int joinedAVDsCount = 1; //Used to store the number of AVD joined in
    private String[] portTable; //Storing ports in order
    private String[] hashTable; //Storing hash values of port
    private List<String> joinedAVDs = new ArrayList<String>(); //List of joined AVDs
    private Map<String, String> lookupTable = new TreeMap<String, String>(); //Map sorted on hash value of the port

    //Make two columns for key and value
    private static final String COLUMN_NAMES[] = new String[] {KEY_FIELD, VALUE_FIELD};

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int rowsDeleted = 0;

        // Get writeable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();

        /*
         * Reference: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html#delete(java.lang.String,%20java.lang.String,%20java.lang.String[])
         */
        if (joinedAVDsCount == 1) { //If there is only one node then delete data on that node
            if (LDUMP.equals(selection) || GDUMP.equals(selection)) {
                /*
                 * For one node, deletion of msg is same for both -
                 * Deletion of msgs from all nodes and
                 * Deletion of msgs from one node
                 */
                rowsDeleted = database.delete(TABLE_NAME, null, null);
            } else {
                //Delete msg with selection key
                selectionArgs = new String[]{selection};
                selection = KEY_FIELD + "=?";
                rowsDeleted = database.delete(TABLE_NAME, selection, selectionArgs);
            }
        } else {
            if (LDUMP.equals(selection)) {
                //Delete all the msgs from a particular node
                rowsDeleted = database.delete(TABLE_NAME, null, null);
            } else if (GDUMP.equals(selection)) {
                for (String remotePort : portTable) {
                    //Delete all the msgs from each node, one at a time by connecting remotely
                    Message msg = new Message(DELETE_ALL);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), remotePort);
                }
            } else {
                try {
                    String hashedKey = genHash(selection); //Get hash of the key to find the node where the key is situated
                    String succPort = getSuccessorFrom(hashedKey); //Get the successor port from the hash value
                    if (selfPort.equals(succPort)) { //Check if the successor port is equal to self port
                        selectionArgs = new String[]{selection};
                        selection = KEY_FIELD + "=?";
                        rowsDeleted = database.delete(TABLE_NAME, selection, selectionArgs);
                    } else { //If the port is different then remotely connect to the client and delete the key and value on that node
                        Message msg = new Message(DELETE);
                        msg.setKey(selection);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), succPort);
                    }
                } catch (NoSuchAlgorithmException e) {
                    Log.e(LOG_TAG, "Error in generating hash for the key " + selection);
                }
            }
        }
        return rowsDeleted;
    }

    @Override
    public String getType(Uri uri) {
        // COMPLETED Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // Check that the key is not null
        String key = values.getAsString(KEY_FIELD);
        if (key == null) {
            throw new IllegalArgumentException("Message requires a key");
        }

        // Check that the value is not null
        String value = values.getAsString(VALUE_FIELD);
        if (value == null) {
            throw new IllegalArgumentException("Message requires a value");
        }

        /*
         * Insert the new message with the given values
         *
         * Since the table can already have the same key used insertWithOnConflict
         * The existing value for that key will be replaced with new value
         */
        if (joinedAVDsCount == 1) { //If there is only one node then insert data on that node
            return insertInDB(uri, values);
        } else {
            try {
                String hashedKey = genHash(key); //Get hash of the key to find the node where the key is situated
                String succPort = getSuccessorFrom(hashedKey); //Get the port from the hash value
                if (selfPort.equals(succPort)) { //Check if the successor port is equal to self port
                    return insertInDB(uri, values);
                } else { //If the port is different then remotely connect to the client and insert the key and value on that node
                    Message msg = new Message(INSERT, key, value);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), succPort);
                }
            } catch (NoSuchAlgorithmException e) {
                Log.e(LOG_TAG, "Error in generating hash for the key " + key);
            }
        }
        return null;
    }

    private Uri insertInDB(Uri uri, ContentValues values) {
        /*
         * Reference: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html#insertWithOnConflict(java.lang.String,%20java.lang.String,%20android.content.ContentValues,%20int)
         */
        // Get writable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();

        long id = database.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);

        // If the ID is -1, then the insertion failed. Log an error and return null.
        if (id == -1) {
            Log.e(LOG_TAG, "Failed to insert row for " + uri);
            return null;
        }
        // Notify all listeners that the data has changed for the message content URI
        mContentResolver.notifyChange(uri, null);
        // Return the new URI with the ID (of the newly inserted row) appended at the end
        return ContentUris.withAppendedId(uri, id);
    }

    @Override
    public boolean onCreate() {
        Context context = this.getContext();
        mContentResolver = context.getContentResolver();
        mDbHelper = new SimpleDhtDbHelper(context);

        //Calculate the port number that this AVD listens on
        TelephonyManager tel = (TelephonyManager) context.getSystemService(TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //Store the current port in self port for future references
        selfPort = portStr;
        try {
            //Generate the hash for the port --> e.g. 5554 - Line Number of the AVD
            nodeID = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            Log.e(LOG_TAG, "Error in generating hash for the port " + portStr);
        }
        if (LEADER_NODE.equals(portStr)) {
            joinedAVDs.add(portStr); //Add the port as joined AVD
            lookupTable.put(nodeID, portStr); //Add the hash and the port to the lookup table
        }
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             * ServerSocket is a socket which servers can use to listen and accept requests from clients
             * AsyncTask is a simplified thread construct that Android provides.
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(LOG_TAG, "Can't create a ServerSocket");
        }
        //Create JOIN_REQUEST msg
        Message msg = new Message(JOIN_REQUEST);
        //Set the port number which is joining in
        msg.setPort(portStr);
        //Add the AVD to the Leader node
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), portStr);
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        /*
         * Reference: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html#query(java.lang.String,%20java.lang.String[],%20java.lang.String,%20java.lang.String[],%20java.lang.String,%20java.lang.String,%20java.lang.String)
         */
        // Get readable database
        SQLiteDatabase database = mDbHelper.getReadableDatabase();
        Cursor resultCursor;
        if (joinedAVDsCount == 1) { //If there is only one node then query data on that node
            if (LDUMP.equals(selection) || GDUMP.equals(selection)) {
                /*
                 * For one node, query of msg is same for both -
                 * Query msgs from all nodes and
                 * Query msgs from one node
                 */
                resultCursor = database.query(TABLE_NAME, null, null, null, null, null, null);
                return resultCursor;
            } else {
                //Query msg with selection key
                selectionArgs = new String[]{selection};
                selection = KEY_FIELD + "=?";
                resultCursor = database.query(TABLE_NAME, projection, selection, selectionArgs, null, null, sortOrder);
                resultCursor.setNotificationUri(mContentResolver, uri);
                return resultCursor;
            }
        } else {
            if (LDUMP.equals(selection)) {
                //Query all the msgs from a particular node
                resultCursor = database.query(TABLE_NAME, null, null, null, null, null, null);
                return resultCursor;
            } else if (GDUMP.equals(selection)) {
                //Append messages from all the nodes
                StringBuilder output = new StringBuilder();
                //Query all the msgs from each node, one at a time by connecting remotely
                for (String remotePort : portTable) {
                    try {
                        //Get the socket with the given port number
                        Socket socket = getSocket(getPortFromLineNumber(remotePort));
                        try {
                            //Create an output data stream to send QUERY message to all the nodes
                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                            //Create a QUERY_ALL message to be sent to the nodes
                            Message msgToSend = new Message(QUERY_ALL);
                            //Write the message on the output stream
                            out.writeUTF(msgToSend.toString());
                            //Flush the output stream
                            out.flush();
                        } catch (IOException e) {
                            Log.e(LOG_TAG, "Error writing data to output stream in QUERY_ALL for port " + remotePort);
                        }
                        try {
                            //Create an input data stream to read messages from all the nodes
                            DataInputStream in = new DataInputStream(socket.getInputStream());
                            //Read the message received
                            String msgReceived = in.readUTF();
                            //Append it to the output
                            output.append(msgReceived);
                        } catch (IOException e) {
                            Log.e(LOG_TAG, "Error reading data from input stream in QUERY_ALL for port " + remotePort);
                        }
                    } catch (IOException e) {
                        Log.e(LOG_TAG, "Error in socket creation " + remotePort);
                    }
                }
                //Get messages as map of key and value from the output returned from all the nodes
                Map<String, String> allMessages = getAllMessages(output.toString());
                /*
                 * Reference: https://developer.android.com/reference/android/database/MatrixCursor.html
                 *
                 * Create Matrix Cursor for placing all the messages
                 */
                MatrixCursor matrixCursor = new MatrixCursor(COLUMN_NAMES);
                //Iterate over all the messages and add row for each entry
                for (Map.Entry<String, String> entry : allMessages.entrySet()) {
                    //Create key value pair
                    String keyValue[] = new String[] {entry.getKey(), entry.getValue()};
                    //Add that pair to the matrix cursor
                    matrixCursor.addRow(keyValue);
                }
                return matrixCursor;
            } else {
                try {
                    String hashedKey = genHash(selection); //Get hash of the key to find the node where the key is situated
                    String succPort = getSuccessorFrom(hashedKey); //Get the port from the hash value
                    if (selfPort.equals(succPort)) { //Check if the successor port is equal to self port
                        //Query selection key from the database
                        selectionArgs = new String[]{selection};
                        selection = KEY_FIELD + "=?";
                        resultCursor = database.query(TABLE_NAME, projection, selection, selectionArgs, null, null, sortOrder);
                        resultCursor.setNotificationUri(mContentResolver, uri);
                        return resultCursor;
                    } else {
                        try {
                            //Get the socket with the given port number
                            Socket socket = getSocket(getPortFromLineNumber(succPort));
                            try {
                                //Create an output data stream to send QUERY message to a particular node
                                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                                //Create a QUERY message to be sent to that node
                                Message msgToSend = new Message(QUERY);
                                //Set the selection as key in the msg
                                msgToSend.setKey(selection);
                                //Write the message on the output stream
                                out.writeUTF(msgToSend.toString());
                                //Flush the output stream
                                out.flush();
                            } catch (IOException e) {
                                Log.e(LOG_TAG, "Error writing data to output stream in QUERY for port " + succPort);
                            }
                            try {
                                //Create an input data stream to read messages from a particular node
                                DataInputStream in = new DataInputStream(socket.getInputStream());
                                //Read the message received
                                String msgReceived = in.readUTF();
                                //Create Matrix Cursor for placing all the messages from that node
                                MatrixCursor matrixCursor = new MatrixCursor(COLUMN_NAMES);
                                //Create key value pair
                                String keyValue[] = {selection, msgReceived};
                                //Add that pair to the matrix cursor
                                matrixCursor.addRow(keyValue);
                                return matrixCursor;
                            } catch (IOException e) {
                                Log.e(LOG_TAG, "Error reading data from input stream in QUERY for port " + succPort);
                            }
                        } catch (IOException e) {
                            Log.e(LOG_TAG, "Error in socket creation" + succPort);
                        }
                    }
                } catch (NoSuchAlgorithmException e) {
                    Log.e(LOG_TAG, "Error in generating hash for the key " + selection);
                }
            }
            return null;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // COMPLETED Auto-generated method stub
        return 0;
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            /*
             * an iterative server that can service multiple clients, though, one at a time.
             */
            while (true) {
                try {
                    if (serverSocket != null) {
                        Socket server = serverSocket.accept();
                        DataInputStream in = new DataInputStream(new BufferedInputStream(server.getInputStream()));
                        //Get message from the input stream
                        String msgReceived = in.readUTF();
                        String[] msgPacket = msgReceived.split(DELIMITER);
                        //Get the message type from the message string
                        MessageType msgType = getEnumBy(msgPacket[0]);
                        if (msgType != null) {
                            switch (msgType) {
                                case JOIN_REQUEST:
                                    //Get the joined AVD from the message
                                    String joinedAVD = msgPacket[1];
                                    //Add the AVD to the joined AVDs
                                    joinedAVDs.add(joinedAVD);
                                    try {
                                        //Calculate the hash of joined AVD port
                                        String hashOfJoinedAVD = genHash(joinedAVD);
                                        //Add the hash and the joined AVD port to the table
                                        lookupTable.put(hashOfJoinedAVD, joinedAVD);
                                    } catch (NoSuchAlgorithmException e) {
                                        Log.e(LOG_TAG, "Error in generating hash for the key " + joinedAVD);
                                    }
                                    //Notify and update the port and hash table of each node
                                    for (String remotePort : joinedAVDs) {
                                        //Get the successor from the joined AVDs table
                                        String table = getSuccessor();
                                        try {
                                            //Get the socket with the given port number
                                            Socket socket = getSocket(getPortFromLineNumber(remotePort));
                                            //Create an output data stream to send JOIN_REQUEST to all the nodes in the ring
                                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                                            out.writeUTF(table);
                                            out.flush();
                                        } catch (IOException e) {
                                            Log.e(LOG_TAG, "Error writing JOIN_REQUEST on port " + remotePort);
                                        }
                                    }
                                    break;

                                case JOIN:
                                    //Convert the msg received to port and hash table for finding the successor of the node
                                    convertToTables(msgReceived);
                                    //Update the count of joined AVDs
                                    joinedAVDsCount++;
                                    break;

                                case INSERT:
                                    //Insert the key and the value in the database
                                    ContentValues values = new ContentValues();
                                    values.put(KEY_FIELD, msgPacket[1]);
                                    values.put(VALUE_FIELD, msgPacket[2]);
                                    mContentResolver.insert(BASE_CONTENT_URI, values);
                                    break;

                                case QUERY:
                                    SQLiteDatabase database = mDbHelper.getReadableDatabase();
                                    String selection = msgPacket[1];
                                    //Create an output data stream to send retrieved QUERY message back to the querying node
                                    DataOutputStream out = new DataOutputStream(server.getOutputStream());
                                    //Query the database with the selection key
                                    String[] selectionArgs = new String[]{selection};
                                    selection = KEY_FIELD + "=?";
                                    Cursor cursor = database.query(TABLE_NAME, null, selection, selectionArgs, null, null, null);
                                    String value = null;
                                    /*
                                     * Move the cursor to the first record
                                     *
                                     * Reference: https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                                     */
                                    if (cursor.moveToFirst()) {
                                        //Get the value from the column
                                        value = cursor.getString(cursor.getColumnIndex(VALUE_FIELD));
                                    }
                                    out.writeUTF(value);
                                    out.flush();
                                    cursor.close();
                                    database.close();
                                    break;

                                case QUERY_ALL:
                                    SQLiteDatabase sqlDatabase = mDbHelper.getReadableDatabase();
                                    //Create an output data stream to send retrieved QUERY_ALL message back to the querying node
                                    DataOutputStream out1 = new DataOutputStream(server.getOutputStream());
                                    //Query the database to get all the results on that node
                                    Cursor resultCursor = sqlDatabase.query(TABLE_NAME, null, null, null, null, null, null);
                                    /*
                                     * Reference: https://developer.android.com/reference/android/database/MatrixCursor.html#getCount()
                                     *
                                     * Get column count
                                     */
                                    int numColumns = resultCursor.getColumnCount();
                                    StringBuilder output = new StringBuilder();
                                    /*
                                     * Move the cursor to the -1 position
                                     * In order to iterate over the result cursor returned from the sql database query
                                     * I move the cursor with -1 position so that I can directly use the function moveToNext
                                     * in the while loop
                                     *
                                     * Reference: https://developer.android.com/reference/android/database/Cursor.html
                                     * Reference: https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                                     */
                                    resultCursor.moveToPosition(-1);
                                    //Iterate over all the values in the cursor
                                    while (resultCursor.moveToNext()) {
                                        for (int i = 0; i < numColumns; i++) {
                                            output.append(resultCursor.getString(i)).append(DELIMITER);
                                        }
                                    }
                                    out1.writeUTF(output.toString());
                                    out1.flush();
                                    resultCursor.close();
                                    sqlDatabase.close();
                                    break;

                                case DELETE:
                                    mContentResolver.delete(BASE_CONTENT_URI, msgPacket[1], null);
                                    break;

                                case DELETE_ALL:
                                    mContentResolver.delete(BASE_CONTENT_URI, null, null);
                                    break;

                                default:
                                    throw new IllegalArgumentException("Unknown Message type" + msgType);
                            }
                        } else {
                            throw new IllegalArgumentException("Message type is null");
                        }
                    } else {
                        Log.e(LOG_TAG, "The server socket is null");
                    }
                } catch (IOException e) {
                    Log.e(LOG_TAG, "Error accepting socket" + e);
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            String[] msgPacket = msgs[0].split(DELIMITER);
            MessageType msgType = getEnumBy(msgPacket[0]);
            String remotePort = msgs[1];
            if (msgType != null) {
                switch (msgType) {
                    case JOIN_REQUEST:
                        // Join message
                        if (!LEADER_NODE.equals(remotePort)) {
                            try {
                                //Get the socket with the given port number
                                //Join message to be taken care by the leader node
                                Socket socket = getSocket(getPortFromLineNumber(LEADER_NODE));
                                //Create an output data stream to send JOIN message with AVD that is joining in
                                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                                //Create JOIN msg with the port that is joining in
                                Message msgToSend = new Message(JOIN_REQUEST);
                                msgToSend.setPort(remotePort);
                                //Write msg on the output stream
                                out.writeUTF(msgToSend.toString());
                                //Flush the output stream
                                out.flush();
                            } catch (IOException e) {
                                Log.e(LOG_TAG, "Error in writing JOIN on port " + LEADER_NODE);
                            }
                        }
                        break;

                    case INSERT:
                        try {
                            //Get the socket with the given port number
                            Socket socket = getSocket(getPortFromLineNumber(remotePort));
                            //Create an output data stream to send INSERT message to a particular node
                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                            //Create INSERT msg with key and value
                            Message msgToSend = new Message(INSERT, msgPacket[1], msgPacket[2]);
                            //Write msg on the output stream
                            out.writeUTF(msgToSend.toString());
                            //Flush the output stream
                            out.flush();
                        } catch (IOException e) {
                            Log.e(LOG_TAG, "Error in writing INSERT on port " + remotePort);
                        }
                        break;

                    case DELETE:
                        try {
                            //Get the socket with the given port number
                            Socket socket = getSocket(getPortFromLineNumber(remotePort));
                            //Create an output data stream to send DELETE message to a particular node
                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                            //Create DELETE msg with key to delete all the message from that node
                            Message msgToSend = new Message(DELETE);
                            msgToSend.setKey(msgPacket[1]);
                            //Write the msg on the output stream
                            out.writeUTF(msgToSend.toString());
                            //Flush the output stream
                            out.flush();
                        } catch (IOException e) {
                            Log.e(LOG_TAG, "Error in writing DELETE on port " + remotePort);
                        }
                        break;

                    case DELETE_ALL:
                        try {
                            //Get the socket with the given port number
                            Socket socket = getSocket(getPortFromLineNumber(remotePort));
                            //Create an output data stream to send DELETE_ALL message to a particular node
                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                            //Create DELETE msg with key to delete all the message from all the nodes node
                            Message msgToSend = new Message(DELETE_ALL);
                            //Write the msg on the output stream
                            out.writeUTF(msgToSend.toString());
                            //Flush the output stream
                            out.flush();
                        } catch (IOException e) {
                            Log.e(LOG_TAG, "Error in writing DELETE_ALL on port " + remotePort);
                        }
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown Message type" + msgType);

                }
            } else {
                throw new IllegalArgumentException("Message type is null");
            }
            return null;
        }
    }

    //Method to create socket for the given port
    private Socket getSocket(String remotePort) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(1000);
        return socket;
    }

    //Method to get the TCP port from the Line Number
    private String getPortFromLineNumber(String portStr) {
        return String.valueOf((Integer.parseInt(portStr) * 2));
    }

    //Method to get the list of ports sorted by hash value
    private String getSuccessor() {
        List<String> sortedPorts = new ArrayList<String>(lookupTable.values());
        StringBuilder table = new StringBuilder(JOIN.name());
        for (String port : sortedPorts) {
            table.append(DELIMITER).append(port);
        }
        return table.toString();
    }

    //Method to convert the message received to a table with port and their hash values
    private void convertToTables(String s) {
        String[] msgPacket = s.split(DELIMITER);
        int n = msgPacket.length;
        portTable = new String[n - 1];
        hashTable = new String[n - 1];
        for(int i = 1; i < n; i++) {
            portTable[i - 1] = msgPacket[i];
            try {
                hashTable[i - 1] = genHash(msgPacket[i]);
            } catch (NoSuchAlgorithmException e) {
                Log.e(LOG_TAG, "Error in generating hash for the port " + msgPacket[i]);
            }
        }
    }

    /*
     * Method to get the successor from the hash value of the port
     *
     * Pros:
     * 1. Easy to implement and maintain
     * 2. Easy to break the loop around the chord ring than maintaining two pointer successor and predecessor,
     * since the iteration will go once the hash table and if not found then will return the first port
     * otherwise the next successor in the ring
     *
     * Cons:
     * Every time I will have to go through the loop to get the successor of the port
     * So the time complexity of the algorithm becomes O(n) where n is the number of ports in the chord ring
     * Since I'm iterating from 0 to the number of AVDs in chord
     * The worst case will be go till the end and if the greater hash value is not found then return the first
     * Maintaining two pointers, for each node will give the successor in O(1) time for best case and O(n) in worst case
     */
    private String getSuccessorFrom(String hashKey) {
        for (int i = 0; i < portTable.length; i++) {
            if (hashTable[i].compareTo(hashKey) > 0) { //Compare the hash values to get the successor port
                return portTable[i];
            }
        }
        /*
         * If the value greater than the hash is not found
         * then either the key hash greater than hash of last node in the ring
         * or the key hash less than hash of first node in the ring
         */
        return portTable[0];
    }

    //Method to separate out key and value from the message
    private Map<String, String> getAllMessages(String s) {
        Map<String, String> messages = new HashMap<String, String>();
        String[] msgPackets = s.split(DELIMITER);
        if(msgPackets.length >= 2) {
            for (int i = 0; i < msgPackets.length; i += 2) {
                messages.put(msgPackets[i], msgPackets[i + 1]);
            }
        }
        return messages;
    }

}
package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtDbHelper;
import edu.buffalo.cse.cse486586.simpledht.model.Message;

import static android.content.Context.TELEPHONY_SERVICE;
import static edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtContract.SimpleDhtEntry.KEY_FIELD;
import static edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtContract.SimpleDhtEntry.TABLE_NAME;
import static edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtContract.SimpleDhtEntry.VALUE_FIELD;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.DELETE;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.DELETE_ALL;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.INSERT;
import static edu.buffalo.cse.cse486586.simpledht.model.MessageType.JOIN_REQUEST;

public class SimpleDhtProvider extends ContentProvider {

    public static final String LOG_TAG = SimpleDhtProvider.class.getSimpleName();

    private SimpleDhtDbHelper mDbHelper;

    private ContentResolver mContentResolver;

    private static final int SERVER_PORT = 10000;
    private static final String LDUMP = "@"; //Specific AVD
    private static final String GDUMP = "*"; //All AVDs
    private String nodeID = null; //Used to store hash of self
    private int joinedAVDsCount = 1; //Used to store the number of AVD joined in

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int rowsDeleted = 0;

        // Get writeable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();

        if (joinedAVDsCount == 1) { //If there is only one node then insert data on that node
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
                //Delete all the msgs from each node, one at a time by connecting remotely
                Message msg = new Message(DELETE_ALL);
                //TODO Need to send msg to client
            } else {
                try {
                    String hashedKey = genHash(selection); //Get hash of the key to find the node where the key is situated
                    if () { //TODO Check if the successor port is equal to self port
                        selectionArgs = new String[]{selection};
                        selection = KEY_FIELD + "=?";
                        rowsDeleted = database.delete(TABLE_NAME, selection, selectionArgs);
                    } else { //If the port is different then remotely connect to the client and delete the key and value on that node
                        Message msg = new Message(DELETE);
                        msg.setKey(selection);
                        //TODO Need to send msg to client
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
                if () { //TODO Check if the current port is equal to self port
                    return insertInDB(uri, values);
                } else { //If the port is different then remotely connect to the client and insert the key and value on that node
                    Message msg = new Message(INSERT, key, value);
                    //TODO need to send msg to the client
                }
            } catch (NoSuchAlgorithmException e) {
                Log.e(LOG_TAG, "Error in generating hash for the key " + key);
            }
        }
        return null;
    }

    private Uri insertInDB(Uri uri, ContentValues values) {
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
        try {
            //Generate the hash for the port --> e.g. 5554 - Line Number of the AVD
            nodeID = genHash(portStr);
            System.out.println("Node ID" + nodeID);
        } catch (NoSuchAlgorithmException e) {
            Log.e(LOG_TAG, "Error in generating hash for the port " + portStr);
        }
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             * ServerSocket is a socket which servers can use to listen and accept requests from clients
             * AsyncTask is a simplified thread construct that Android provides.
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            //TODO Create server task
        } catch (IOException e) {
            Log.e(LOG_TAG, "Can't create a ServerSocket");
        }
        //Create JOIN_REQUEST msg
        Message msg = new Message(JOIN_REQUEST);
        //Set the port number which is joining in
        msg.setPort(portStr);
        //Add the AVD to the Leader node
        //TODO need to send msg to client
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // Get readable database
        SQLiteDatabase database = mDbHelper.getReadableDatabase();
        Cursor resultCursor;
        if (joinedAVDsCount == 1) { //If there is only one node then insert data on that node
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
                //TODO to query all the nodes and get data
            } else {
                try {
                    String hashedKey = genHash(selection); //Get hash of the key to find the node where the key is situated
                    if() { //TODO Check if the current port is equal to self port
                        //Query selection key from the database
                        selectionArgs = new String[]{selection};
                        selection = KEY_FIELD + "=?";
                        resultCursor = database.query(TABLE_NAME, projection, selection, selectionArgs, null, null, sortOrder);
                        resultCursor.setNotificationUri(mContentResolver, uri);
                        return resultCursor;
                    } else {
                        //TODO to query the node and get data
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
}

package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtDbHelper;

import static edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtContract.SimpleDhtEntry.KEY_FIELD;
import static edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtContract.SimpleDhtEntry.TABLE_NAME;
import static edu.buffalo.cse.cse486586.simpledht.data.SimpleDhtContract.SimpleDhtEntry.VALUE_FIELD;

public class SimpleDhtProvider extends ContentProvider {

    public static final String LOG_TAG = SimpleDhtProvider.class.getSimpleName();

    private SimpleDhtDbHelper mDbHelper;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
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

        // Get writable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();

        /*
         * Insert the new message with the given values
         *
         * Since the table can already have the same key used insertWithOnConflict
         * The existing value for that key will be replaced with new value
         */
        long id = database.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);

        // If the ID is -1, then the insertion failed. Log an error and return null.
        if (id == -1) {
            Log.e(LOG_TAG, "Failed to insert row for " + uri);
            return null;
        }
        // Notify all listeners that the data has changed for the message content URI
        getContext().getContentResolver().notifyChange(uri, null);

        // Return the new URI with the ID (of the newly inserted row) appended at the end
        return ContentUris.withAppendedId(uri, id);
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        mDbHelper = new SimpleDhtDbHelper(getContext());
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        // Get readable database
        SQLiteDatabase database = mDbHelper.getReadableDatabase();

        /*
         * Since the selection variable has the key value to search, filled selectionArgs with value
         * from selection
         *
         * Changed the selection to query value containing equal to and question mark
         */
        selectionArgs = new String[]{selection};
        selection = KEY_FIELD + "=?";

        /*
         * This cursor will hold the result of the query
         *
         * Query the message table directly with the given projection, selection, selection
         * arguments, and sort order. The cursor contains single row of the messages table.
         */
        Cursor resultCursor = database.query(TABLE_NAME, projection, selection, selectionArgs, null, null, sortOrder);
        resultCursor.setNotificationUri(getContext().getContentResolver(), uri);
        Log.v("query", selectionArgs[0]);
        return resultCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
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

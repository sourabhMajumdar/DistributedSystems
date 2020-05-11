package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.database.Cursor;

import java.io.Serializable;

public class Message implements Serializable {
    public String protocol;
    //public ContentValues contentValues;
    public String keyString;
    public String valString;
    public String nodeToContact;
    public String portId;
    //public Cursor cursor;

    Message(String protocol,String keyString, String valString, String nodeToContact,String portId){
        this.protocol = protocol;
        //this.contentValues = contentValues;
        this.keyString = keyString;
        this.valString = valString;
        this.nodeToContact = nodeToContact;
        this.portId = portId;
    }

    public String toString(){


        return "messageAsaString";
    }
}

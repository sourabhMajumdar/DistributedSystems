package edu.buffalo.cse.cse486586.groupmessenger2;

import android.widget.MultiAutoCompleteTextView;

import java.io.Serializable;
import java.util.Comparator;

public class Message implements Serializable{
    public int senderPort;
    public int receiverPort;
    public int proposedSequenceNum;
    public boolean isDeliverable;
    public String message;
    public String protocol;
    public int failedNode = -1;
    public int messageNumber;


    Message(String protocol,String message,int senderPort, int receiverPort, int proposedSequenceNum,int messageNumber, boolean isDeliverable,int failedNode){
        this.protocol = protocol;
        this.message = message;
        this.senderPort = senderPort;
        this.receiverPort = receiverPort;
        this.proposedSequenceNum = proposedSequenceNum;
        this.messageNumber = messageNumber;
        this.isDeliverable = isDeliverable;
        this.failedNode = failedNode;
    }
    public String toString(){
        String deliver = "";
        if(this.isDeliverable){
            deliver = "deliver";
        }
        else{
            deliver = "notdeliver";
        }
        return (this.protocol + "_" + this.message + "_" + Integer.toString(this.senderPort) + "_" + Integer.toString(this.receiverPort) + "_" + Integer.toString(this.proposedSequenceNum) + "_" + Integer.toString(this.messageNumber) + "_" +  deliver + "_" + Integer.toString(this.failedNode));
    }



}

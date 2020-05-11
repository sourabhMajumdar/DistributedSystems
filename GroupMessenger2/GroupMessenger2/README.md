# Group Messenger 2

We design a group messenger that can send messages to multiple AVDs and store them in a permanent key-value storage. 
In addition to vanilla multicast we also introduce FIFO and Total Ordering and ensuring messages are sent even under failures

## What does it all mean for a layman ?
It means that we have created an Instant Messenger Service that not only is able to send your messages to everyone 
but also ensure that your friends receive your messages in the order you send them and everyone sees the same ordering of messages

ActiveMQ TextMessage Recorder & Replay
=============================

Trying out rx.scala Observables. Currently creates an observable for an ActiveMQ topic and a subscriber that writes out the messages from the observable as JSON, along with a timestamp.

The Replay functionality can read the file with JSON, do a binary search to find the timestamp it's looking for and then start playing back messages from that point onwards.

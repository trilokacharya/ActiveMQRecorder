ActiveMQ TextMessage Recorder & Replay
=============================

This started out as a test project for learning Scala RX observables. This tool creates an observable stream out of an ActiveMQ subscription. The stream is written to disk along with a timestamp. The second part of the tool can find a timestamp in the written out file and return a Iterable that is played back into an ActiveMQ topic. The replay functionality uses binary search to find the correct timestamp quickly in a large file.

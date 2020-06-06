# Task-2-Mutual-Exclusion
The goal of this code is to provide a distributed implementation of a mutual exclusion algorithm using IBM COS. There will be
one master function and a variable number of slave functions. All the slave functions will update a common shared object called
“result.json”, appending into this file the id of the slave function when it gets permission to write.

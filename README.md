# This is a distributed/parrallel union find algorithm

This is a distributed and parrallel union find algorithm to use when the union find is too big for a single machine or to have 
the structure scale with the amount of cores of the machine.

This library is a work in progress and was made with my brother Ten0.
The parallel part of the program was made with liveshare and the distributed part was made by me.

# How to use


The default parrallel in memory version can be run with :
`cargo run --release --bin big_uf`

It will just add new nodes to the union find


The distributed version has both a worker and a master binary.

The workers can be started with :
`cargo run --release --bin worker <port>`

And the master can be started with :
`cargo run --release --bin master`

The master currently expect two worker on the same machine that have port 10000 and 10001, it can be changed in the src/bin/master.rs file

It will just add new nodes to the union find


# Improvements

Currently the workers and master don't stop correctly if there are errors
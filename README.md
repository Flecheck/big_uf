# This is a distributed/parrallel union find algorithm


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
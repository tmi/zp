This folder represents a case study for postgres performance, in particular, comparing three data representations.

The source data is schematically like this:
```
Block {
  string BlockId
  repeated OuterStruct {
    string OuterId
    timestamp OuterTs
    repeated InnerStruct {
      string InnerId
      i32 Volume // a number between 0-100
      f32 Threshold // a number between 0. and 1.
    }
  }
}
```

You will do a number of task. After each task, verify it is correct (verification instructions will be part of the task), make a commit with short message of the task's description. Never git push.

1. Prepare environment
We will base the project on postgres docker image. Create a simple docker compose. Expose the docker port so that local tools can interact with it. Choose some simple password and use it for the rest of the project, we dont worry about security.

To verify, ensure you can start and shut down the container. Then commit.

2. Create a simple tool in Rust with a CLI based on Clap crate, and following commands:
 - create:
  a. Row per message above -- meaning there will be an array of arrays. Im not fully sure nested arrays can be done -- if not, you would need to expand the array.
  b. One row per each OuterStruct -- meaning the BlockId would be copied to each row, and the InnerStruct would be an array.
  c. One row per each InnerStruct -- meaning the BlockId, OuterId, and OuterTs would be copied to each row which contain just scalars beside (InnerId, Volume, Threshold)
 - clean <table> -- deletes all rows in the given table
 - fill <table> <N> <blockSize> <outerSize> -- generate N block messages, each having blockSize outer structs, and each of those having outerSize inner structs. This command returns the time information -- how long it took to insert. Be careful to generate first, and only then start measuring the insert time. Don't bulk insert, rather do one block after another. But reuse a single postgres connection. Don't worry about retries for now, just crash in that case. The values of Volume and Threshold should be uniform random.

To verify, first run cargo check. No need to write tests. Then build, and run the 'create', 'fill' with 1 row for each table, then 'clean'. Then commit.

3. Extend the tool with query capabilities.
We want a query like "given a value T in (0,1), what is the total Volume of all Inner Structs whose Threshold is over T? And what is their count?" 
In the schema 'c.', this is simple -- 'select sum(volume), count(1) from table where threshold > T', but for other schemata you will possibly need to unnest.
Extend the rust tool with 'query <table>'. Given that Threshold is uniform random, just hardcode 'T := .5' in the query.
The tool should measure the query runtime (make sure you only measure the query execution itself, not the connection setup/teardown), and the tool should print the runtime, as well as the answers (count + volume).

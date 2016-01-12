# Store 

Store general key-value store api in Clojure. The goal of store is to abstract away from policies about how data is stored and provide a uniform API for application data.

## Specifying a Store

You specify a store by specifying a collection of buckets. Here's an example

    (def bucket-specs
    	 [{:name "tweets"
      	   :type :fs
	   :path "/tmp/data/tweets"
           :merge (fn [key old-tweets new-tweets] (concat old-tweets new-tweets))}
     	 {:name "session"
	  :type :mem}
         {:name "visit-statistics"
          :type :bdb
          :merge (fn [key cur-stats new-stats] (merge-with + cur-stats new-stats))
          :flush [:self]
          :flush-freq 30}])

      (def s (store bucket-specs {:db-env berkeley-db-env})

In this example "tweets" is stored on the local filesystem at a path, where filenames are keys and contents are values. We specify a merge function to be used on this bucket with the <code>:merge</code> keyword. The visit-statistics bucket uses the `flush' mechanism which allows writes in the process to be accumulated to an in-memory hash-map and periodically flushed to the underlying store (which in this case is Berkeley BDB Java edition) every 30 seconds. The flush buckets support atomic merges with the specified merge function. In general however, store oeprations are  non-transactional operation.


## Basics

 A store consists of many buckets and each bucket has it's own key-value namespace. A store operation takes the form 

  (store operation bucket-name & args)

where operation is the name of a bucket operation and args are any other args needed for the operations. The following are valid store operations:

    :get  - return value associated with a given key in a bucket 
      (s :get "bucket" "key") 
    :put  - store value associated with a given key in a bucket 
      (s :put "bucket" "key" "val")
    :update - update value associated with a key using a function 
      (s :update "visitor-counts" "user" (fnil inc 0)
    :merge - a special case of update for a merging function specific to the bucket specified at bucket-declaration 
      (s :merge "visior-counts" "user" 1)
    :delete - delete a key-value pair 
      (s :delete "bucket" "key")
    :exists? - does a given key exist? 
      (s :exists? "bucket" "key")
    :keys - return seq of bucket keys
    :seq - return seq of [k v] pairs in bucket
    :sync - when using a bucket with flush (see below) sync in-memory state to underlying bucket
   

## Sponsors

YourKit is kindly supporting open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and
[YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).
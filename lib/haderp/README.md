## What is it? 

Haderp brings together tools to make it easy to build hadoop jobs and run them in-process, in a local hadoop installation, or provision EMR clusters and run them on AWS.

See jobs/example-job/project.clj for working example project, and example-job.job for a working example job that can be run in-process, on a local hadoop instance, or remotely on EMR.  I recommend copying that as a template, because there are several (documented) hacks in the project.clj and job file that make things work properly.

## Running jobs

### In-process

You can test jobs straight by calling clojure_hadoop.job/run.  For this to work, you need to have the appropriate :provided dependencies in your project.clj (see the example project). 

### On local hadoop

First set up local hadoop by running the instructions here through "Standalone operation".  I just unzipped the binary distribution to ~/sw/hadoop-2.6.0.

http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html

Then run `lein uberjar` then e.g. `bin/hadoop jar ~/grabbag/job/example-job/target/example-job-0.0.1-SNAPSHOT-standalone.jar clojure_hadoop.job -job example-job.job/job -input input/core-site.xml -output output`

(where you've put something in the appropriate input path).  

### On EMR

Use the `haderp.core/launch!` fn to launch a cluster, and `haderp.core/run!` to add jobs to a running cluster.  Once launched, you can see your cluster and results in the AWS dashboard.  These commands handle creating and uploading a jar (and optionally, input data) to s3 for you, so it should be nearly as easy to run jobs remotely as locally.

## Troubleshooting 

If you see Exception in thread "main" java.io.IOException: Mkdirs failed to create /var/folders/_g/_nhxhpvn249c7ylfjwftblvr0000gn/T/hadoop-unjar5014037048061726648/license, this is apparently a Mac OS issue http://stackoverflow.com/questions/10522835/hadoop-java-io-ioexception-mkdirs-failed-to-create-some-path
 
When weird shit happens on EMR and you can't find logs anywhere, ssh to the master node (you can use haderp.core/ssh-master-command) and look at /mnt/var/log/
http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-troubleshoot-failed-4.html

Currently, jobs depending on schema (and possibly other things) can fail when run out-of-process due to classloader issues.  The symptom is "Caused by: java.lang.IllegalArgumentException: No implementation of method: :walker of protocol: #'schema.core/Schema found for class: clojure.core$long".  A careful bootstrap loading sequence that can be found as the first line of the example job appears to work around this for now, but may need to be modified / extended as more code is loaded by a job.  Another nuclear option is to just turn off schema validation using

```clojure
(require '[schema.macros])
(reset! schema.macros/*compile-fn-validation* false) ;; nuclear option
```

before your ns declaration.

With this problem out of the way, our codebase seems to work fine when running in-process and on EMR.  However, there are still sometimes issues with loading Snappy when running in a separate hadoop installation locally -- this seems to be an issue with Mac OS, hadoop, snappy, Java 7, and classloaders, and I haven't found any workarounds that are successful yet.
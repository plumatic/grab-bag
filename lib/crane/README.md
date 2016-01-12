# crane
## Production services deployment in clojure

Credentials:

git clone crane

symlink crane/resources/bin/crane to /crane

Copy a clojure map to /user/home/.crane containing usernames, passwords, keyfile locations, etc.

{:key "AWS-KEY"
:secretkey "AWS-SECRET-KEY"
:key-path "/path/to/private-key"
:key-name "key-name"
:user "user-name"}

This is just a map that gets merged with the config map you select from your deploy file.

Add a deploy.clj file in the src directory of your project.

crane cares about:

:service
:user

Example:

```
# Deploy to all stage instances.
crane deploy stage all

# Deploy to one stage instance called i-892a84.
crane deploy stage i-892a84

# Deploy to stage when the service is not replicated.
crane ssh stage

# Run apt-get update on all machines that can be accessed with these ec2 credentials.
crane shell-command global 'sudo apt-get update'
```

crane 
- Copyright (c) Bradford Cross released under the MIT License (http://www.opensource.org/licenses/mit-license.php).

## Sponsors

YourKit is kindly supporting open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and
[YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).

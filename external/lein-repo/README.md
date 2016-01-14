# A story 

All good README pages start with a story (?!).  Back in 2011, our codebase was split over 30-some different git repositories and leiningen projects, in 1:1 correspondence.  Projects depended on one-another via binary jar dependencies, so when you were working on a service and needed to refactor a utility in a low-level library, you would have to go to the lowest-level repo, make your changes, test, bump the version, commit and `lein install`, increment the dependency in the next upstream library, and so on until you reached the service you were actually working on.  We attempted to use Android's [repo](https://source.android.com/source/using-repo.html) to ease this pain, but a pain it was -- not just because of the above, but also because of [JAR hell](http://en.wikipedia.org/wiki/Java_Classloader#JAR_hell) -- ensuring that no service ended up depending on multiple versions of a lower-level library is something straight out of Dante, not to mention doing sweeping refactors was basically impossible.  

To mitigate this pain, we decided to move from 30 repos and JAR dependencies to a single repository, with all code versioned together, and source-level dependencies.  However, splitting the codebase into separate leiningen projects based on function still seemed like a good idea, to keep things modular and well-structured and allow services to only pay for the external JAR dependencies they actually need.  Unfortunately, leiningen doesn't provide a functionality for dependency management between source projects (besides checkouts, which are very limited).

Thus, lein-repo was born.

# Lein-repo

`lein-repo` is our internal Leiningen plugin, which we use for a variety of purposes, but primarily for providing real source-level dependency management between multiple projects.    

`lein-repo` must be installed and in `~/.lein/profiles.clj` as a user plugin to interact with the codebase.  It does a number of things, but its primarly function is dependency-management that enables source-level dependencies between leiningen projects.  

## Dependency management syntax

Lein-repo introduces three new concepts to leiningen and project files.  

First, [`projects.clj`](https://github.com/plumatic/grab-bag/blob/master/projects.clj) is a file that lives at the repo root, and expresses:

 - **Required dependencies** that should be included in all projects in the repo
 - Canonical versions of **external dependencies** that can be included in projects in the repo in the `project.clj` files under a new key `external-dependencies`.  The version is only expressed once in `projects.clj`, and each `project.clj` only names the dependency, with the version taken from `projects`.  This helps us avoid JAR hell, by maintaining a consistent set of external project versions across the entire codebase.  *Whenever possible, prefer `:external-dependencies` to explicit `:dependencies` in your `project.clj`.*
 - Where to find **internal dependencies**, which are projects within the main repo that can be included in `project.clj` under a new key `internal-dependencies`, and the source files of the dependency will be directly added to the classpath of the target project.  This enables separate projects, while still retaining the ability to modify and refactor across projects without `lein install`, bumping versions, or even restarting your repl.  Version numbers for internal dependencies are not needed (but may be present for historical reasons), and everything is versioned together in the single top-level git repository.

The core logic of the plugin is expressed in [`plugin.clj`](https://github.com/plumatic/grab-bag/blob/master/external/lein-repo/src/lein_repo/plugin.clj).  This basically figures out the transitive dependencies of your `project.clj`, expanding out `external-dependencies` and `internal-dependencies` into the proper `dependencies` and `source-paths` to make everything work as (hopefully) expected.  Because it's a plugin, this `project.clj` munging makes everything from your REPL to `lein test` to the uberjar command used to push code to AWS find the right files and put them on the classpath / in the uberjar.  

## Tasks 

In addition to this plugin infrastructure, lein-repo is also a dumping ground for tasks that we want to add to leiningen.  To name a few:
 
  - `lein codox` generates API-docs for the whole repo
  - `lein crane` is what's run by the `crane` executable, discussed in a sec.
  - `lein test-all` is basically just `lein test` in a mega-project that includes all of the `internal-dependencies`.

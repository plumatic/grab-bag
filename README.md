## What is this?!

This repo is a heavily pruned version of our main repository, released (except where otherwise noted) under the Eclipse Public License (v1.0).

We don't expect it to be terribly useful to anyone in its current form, but it does contain many components and ideas that can hopefully be of some value to the community.  

Over time, we plan to pull out and separately release individual components (or welcome others to do so -- but if so, we'd appreciate a heads up first to make sure we don't duplicate work).


### What this isn't

This repo contains only scaffolding, from storage tools up to service infrastructure (including several example services).  The repo **does not** contain any of the key libraries, services, or clients that constituted the News product (or other products).  

Many files and portions thereof are not included in this release, so we expect some higher-level things may not work properly (although the remaining tests are all passing).  Documentation from outside the repo is also missing.

Moreover, some of the code is 4+ years old and/or ugly, and much better alternatives exist in the current Clojure ecosystem.  

***caveat emptor***.

## Overview

A quick overview of things that may be of interest:

 - `external` contains the `lein-repo` leiningen plugin, which allows for complex source-level dependencies between projects.  This allows us to break our repository into many subprojects, without having to ever `lein install`.
 - `lib` contains our various library projects, including:
   - various low-level utilities in `plumbing` (which augment the current open-source project)
   - a `store` abstraction for key-value stores
   - service infrastructure in `service`
   - selected ML infrastructure in `classify`
 - `service` contains a few example services, which show how we use `graph` to compose complex services declaratively



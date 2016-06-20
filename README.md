# RedJob

Spring based implementation of a Redis-backed job queue.

RedJob is based on the ideas of [Resque](https://github.com/resque/resque) 
and [Jesque](https://github.com/gresrun/jesque), but is not compatible with them.

## Build the project
To build (clean/compile/test/install) the project, invoke

    $ mvn clean install
    
## Build a release
To publish a release to maven central, invoke

    $ mvn -P release

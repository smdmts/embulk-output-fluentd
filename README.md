# Fluentd output plugin for Embulk

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

| name                                 | type        | required?  | default                  | description            |  
|:-------------------------------------|:------------|:-----------|:-------------------------|:-----------------------|
|  host                                | string      | optional   | "127.0.0.1"              | fluentd servers host   |
|  port                                | integer     | optional   | "24224"                  | fluentd servers port   |
|  tag                                 | string      | required   |                          | tag of logs            |
|  asyncSize                           | integer     | optional   | "1"                      | asynchronous parallelism size |
|  requestPerSeconds                   | integer     | optional   | "0"                      | Sending throttle requests in per seconds. (default 0 is non throttle) (*1) |
|  default_timezone                    | string      | optional   | UTC                      | |
|  default_timestamp_format            | string      | optional   | %Y-%m-%d %H:%M:%S.%6N    | |

*1: Throttling single request is approximately 27,000 records in a single time. If your fluentd server is overloading, adjustment this parameter.


## Example

```yaml
out:
  type: fluentd
  tag: fluentd.tag
  asyncSize: 4
  requestPerSeconds: 30
```

```
$ embulk gem install embulk-output-fluentd
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Developing or Testing

This plug-in is written by Scala. You could use sbt.

```
$ ./sbt 
$ ./sbt test
```
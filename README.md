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
|  time_key                            | string      | optional   |                          | using timekey of value is seconds. (if empty parameter then using current unixtime) |
|  async_size                          | integer     | optional   | "1"                      | asynchronous parallelism size |
|  request_grouping_size               | integer     | optional   | "100"                    | sending request grouping size (*1) |
|  request_per_seconds                 | integer     | optional   | "0"                      | Sending throttle requests in per seconds. (default 0 is non throttle) (*2) |
|  default_timezone                    | string      | optional   | UTC                      | |
|  default_timestamp_format            | string      | optional   | %Y-%m-%d %H:%M:%S.%6N    | |

*1: Grouping Request of sending fluentd in a single time. (e.f. 100 is about 2750 records in single time.)  
*2: Throttling single request of requestGroupingSize(just 100 of 2750 records) of bulk requests. 

If your fluentd server is overloading, adjustment parameters(asyncSize,requestGroupingSize,requestPerSeconds) .

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
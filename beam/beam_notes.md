# Apache Beam

## I/O adapters

Used to connect to external sources. They connect to the source and return a PCollection. You can use the `Read` transform available in all of them.   

Examples in: Python, Java, Go, and Yaml:

1.
```python
lines = pipeline | "ReadMyFile" >> beam.io.ReadFromText(
    "gs://some/input_data.txt")
```
2.
```java
public static void main(String[] args) {
    // create the pipeline
    PipelineOptions options = 
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);
    // create a PCollection lines by applying a Read transform
    PCollection<String> lines = p.apply(
        "ReadMyFile", TextIO.read().from("gs://some/input_data.txt"));
}
```
3.
```go
lines := textio.Read(scope, "gs://some/input_data.txt")
```
4.
```yaml
pipeline:
    source:
        type: ReadFromText
        config:
            path: "gs://some/input_data.txt"
```

From now on I will only use the Python examples, but for comparison it's clear which options are more verbose.


# Avro-to-arrow

## General

This package trying to convert tabular data in avro format into arrow columnar format, which then can be used in various libraries, mainly pandas and dask.

The intention of this package is to maximize the usage of arrow format and its efficient memory managements.


## To Do:
- Add Test
- More efficient reads. As of now, the current method of reading into python object from fastavro then convert them to pyarrow array/table seem to be faster than this package.
- Schemaless
- Nested data structure
- Modular compression modules? Snappy is hard coded into the lib.
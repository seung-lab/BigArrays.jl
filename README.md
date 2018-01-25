BigArrays.jl
============
[![Build Status](https://travis-ci.org/seung-lab/BigArrays.jl.svg?branch=master)](https://travis-ci.org/seung-lab/BigArrays.jl)

storing and accessing large julia array using different backends.

## Features
- serverless, clients do IO directly
- multiple processes to fully use all the CPU cores
- arbitrary subset cutout (saving should be chunk size aligned)
- extensible with multiple backends
- arbitrary shape, the dataset boundary can be curve-like
- arbitrary dataset size (in theory, tested dataset size: ~ 9 TB)
- chunk compression with gzip/blosclz/jpeg
- highly scalable due to the serverless design
- arbitrary data type 
- support negative coordinate

## supported backends
- [x] AWS S3 
- [x] Google Cloud Storage
- [x] Local binary files

## Installation
    Pkg.add("BigArrays")
    
## usage

`BigArrays` do not have limit of dataset size, if your reading index is outside of existing file range, will return an array filled with zeros.

### use backend of local binary file 
```julia
using BigArrays
using BigArrays.BinDicts
ba = BigArray( BinDict("/path/of/dataset") )
```
then use `ba` as normal array, the returned cutout result will be an OffsetArray, if you need normal Julia Array, use `parent` function to get it. 

### use backend of AWS S3 
#### setup info file 
the info file is a JSON file, which defines all the configuration of the dataset. It was defined in [neuroglancer](https://github.com/seung-lab/neuroglancer/wiki/Precomputed-API#info-json-file-specification) 

[test example](https://github.com/seung-lab/BigArrays.jl/blob/master/test/backends/s3.jl)

### use backend of Google Cloud Storage
the [info configuration file](https://github.com/seung-lab/neuroglancer/wiki/Precomputed-API#info-json-file-specification) is the same with S3 backend.

[test example](https://github.com/seung-lab/BigArrays.jl/blob/master/test/backends/gs.jl)

# Benchmark

image size: 512x512x512, data is EM image
chunk size is 256x256x32

# Development
BigArrays is a high-level architecture to transform Key-Value store (backend) to Julia Array (frontend). it provide an interface of AbstractArray, and implement the get_index and set_index functions. 

## Add new backend
The backends are different key-value stores. To add a new backend, you can simply do the following:

- wrap the key-value store as a Julia `Associate` type. [S3Dicts is an example](https://github.com/seung-lab/S3Dicts.jl/blob/master/src/S3Dicts.jl#L15) is a good example. 
- implement the `getindex` and `setindex!` functions. [S3Dicts example](https://github.com/seung-lab/S3Dicts.jl/blob/master/src/S3Dicts.jl#L29)
- make sure that the key-value store have a field of `configDict` containing the block size and data type.

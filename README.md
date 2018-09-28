BigArrays.jl
============
[![Build Status](https://travis-ci.org/seung-lab/BigArrays.jl.svg?branch=master)](https://travis-ci.org/seung-lab/BigArrays.jl)

storing and accessing large julia array.

# Features
- serverless, clients do IO directly
- multiple processes to fully use all the CPU cores
- arbitrary subset cutout (saving should be chunk size aligned)
- extensible with multiple backends
- arbitrary shape, the dataset boundary can be curve-like
- arbitrary dataset size (in theory, tested dataset size: ~ 9 TB)
- multiple chunk compression algorithms
- highly scalable due to the serverless design
- multiple data types 
- support negative coordinate

## supported backends
- [x] Local binary files

Any other storage backends could be mounted in local filesystem will work. For example, AWS S3 and Google Cloud Storage could be supported by mounting the bucket as local directory.

## compression and decompression
| Algorithm     | compression        | decompression      |
| ------------- |:------------------:| ------------------:|
| gzip          | :white_check_mark: | :white_check_mark: |
| zstd          | :white_check_mark: | :white_check_mark: |
| blosclz       | :white_check_mark: | :white_check_mark: |
| jpeg          | :x:                | :white_check_mark: |

## supported data types
Bool, UInt8, UInt16, UInt32, UInt64, Float32, Float64.
super easy to add more, please raise an issue if you need more.

# Installation
Install [Julia 1.0 or 0.7](https://julialang.org/downloads/), in the REPL, press `]` to enter package management mode, then 
```
add BigArrays
```

# usage

`BigArrays` do not have limit of dataset size, if your reading index is outside of existing file range, will return an array filled with zeros.

## setup info file 
the info file is a JSON file, which defines all the configuration of the dataset. It was defined in [neuroglancer](https://github.com/seung-lab/neuroglancer/wiki/Precomputed-API#info-json-file-specification) 

## use backend of local binary file 
```julia
using BigArrays
using BigArrays.BinDicts
ba = BigArray( BinDict("/path/of/dataset") )
```
then use `ba` as normal array, the returned cutout result will be an OffsetArray, if you need normal Julia Array, use `parent` function to get it. 
For more examples, check out the [tests](https://github.com/seung-lab/BigArrays.jl/blob/master/test/BinDicts.jl).

# Development
BigArrays is a high-level architecture to transform Key-Value store (backend) to Julia Array (frontend). it provide an interface of AbstractArray, and implement the get_index and set_index functions. 

## Add new backend
The backends are different key-value stores. To add a new backend, you can simply do the following:
- wrap the key-value store as a Julia `AbstractDict` type. [BinDicts is an example](https://github.com/seung-lab/BigArrays.jl/blob/master/src/backends/BinDicts.jl) is a good example. 
- implement the `Base.getindex` and `Base.setindex!` functions. [BinDicts example](https://github.com/seung-lab/BigArrays.jl/blob/master/src/backends/BinDicts.jl#L26)
- implement the `get_info` function to return a string of info file, which was [defined in Neuroglancer](https://github.com/google/neuroglancer/blob/c9a6b9948dd416997c91e655ec3d67bf6b7e771b/src/neuroglancer/datasource/precomputed/README.md).

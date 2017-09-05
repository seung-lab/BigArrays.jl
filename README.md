BigArrays.jl
============
[![Build Status](https://travis-ci.org/seung-lab/BigArrays.jl.svg?branch=master)](https://travis-ci.org/seung-lab/BigArrays.jl)

storing and accessing large julia array using different backends.

## Features
- serverless, clients do IO directly
- arbitrary subset cutout (saving should be chunk size aligned)
- extensible with multiple backends
- arbitrary shape, the dataset boundary can be curve-like
- arbitrary dataset size (in theory, tested dataset size: ~ 9 TB)
- chunk compression with gzip/blosclz/jpeg
- highly scalable due to the serverless design
- arbitrary data type 

## supported backends
- [x] AWS S3 
- [x] Google Cloud Storage
- [x] Local HDF5 files
- [ ] Local binary files

## Installation
    Pkg.clone("https://github.com/jingpengwu/AWS.jl.git")
    Pkg.clone("https://github.com/jingpengwu/GoogleCloud.jl.git")
    Pkg.clone("https://github.com/seung-lab/BigArrays.jl.git")
    Pkg.clone("https://github.com/seung-lab/S3Dicts.jl.git")
    Pkg.clone("https://github.com/seung-lab/GSDicts.jl.git")
    
## usage

`BigArrays` do not have limit of dataset size, if your reading index is outside of existing file range, will return an array filled with zeros.

### use the hdf5 files backend
```julia
using BigArrays.H5sBigArrays
ba = H5sBigArray("/directory/of/hdf5/files/");
# use it as normal array

ba[101:200, 201:300, 1:3] = rand(UInt8, 100,100,3)
@show ba[101:200, 201:300, 1:3]
```

### use backend of AWS S3 
#### setup info file 
the info file is a JSON file, which defines all the configuration of the dataset. It was defined in [neuroglancer](https://github.com/seung-lab/neuroglancer/wiki/Precomputed-API#info-json-file-specification) 

[test example](https://github.com/seung-lab/BigArrays.jl/blob/master/test/backends/s3.jl)

### use backend of Google Cloud Storage
the [info configuration file](https://github.com/seung-lab/neuroglancer/wiki/Precomputed-API#info-json-file-specification) is the same with S3 backend.

[test example](https://github.com/seung-lab/BigArrays.jl/blob/master/test/backends/gs.jl)

# Development
BigArrays is a high-level architecture to transform Key-Value store (backend) to Julia Array (frontend). it provide an interface of AbstractArray, and implement the get_index and set_index functions. 

## Add new backend
The backends are different key-value stores. To add a new backend, you can simply do the following:

- wrap the key-value store as a Julia `Associate` type. [S3Dicts is an example](https://github.com/seung-lab/S3Dicts.jl/blob/master/src/S3Dicts.jl#L15) is a good example. 
- implement the `getindex` and `setindex!` functions. [S3Dicts example](https://github.com/seung-lab/S3Dicts.jl/blob/master/src/S3Dicts.jl#L29)
- make sure that the key-value store have a field of `configDict` containing the block size and data type.

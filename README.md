# BigArrays.jl
storing and accessing large julia array using different backends.

# Features
- serverless, clients do IO directly
- arbitrary subset cutout (saving should be chunk size aligned)
- extensible with multiple backends
- arbitrary shape, the dataset boundary can be curve-like
- arbitrary dataset size (in theory, tested dataset size: ~ 9 TB)
- chunk compression with gzip/blosclz/jpeg
- highly scalable
- arbitrary data type (depends on implementation of backends)

## supported backends
- [x] hdf5 files. 
- [x] seunglab aligned 2D image hdf5 files.
- [x] cuboids in AWS S3 
- [x] Google Cloud Storage

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

### Aligned 2D HDF5 sections
the array was saved as 2D sections with offset, normally output of Seunglab alignment
you should use `AlignedBigArray` to read the sections.

[test example](https://github.com/seung-lab/BigArrays.jl/blob/master/test/backends/aligned.jl)

### HDF5 ND chunks
ND chunks saved in HDF5 files, normally output of convnet inference and segmentation.
you should use `H5sBigArray` to cutout and save the chunks. Note that the saving should be aligned with the chunk size.

[test example](https://github.com/seung-lab/BigArrays.jl/blob/master/test/backends/h5s.jl)
  

# Development
BigArrays is a high-level architecture to transform Key-Value store (backend) to Julia Array (frontend). it provide an interface of AbstractArray, and implement the get_index and set_index functions. 

## Add new backend
The backends are different key-value stores. To add a new backend, you can simply do the following:

- wrap the key-value store as a Julia `Associate` type. [S3Dicts is an example](https://github.com/seung-lab/S3Dicts.jl/blob/master/src/S3Dicts.jl#L15) is a good example. 
- implement the `getindex` and `setindex!` functions. [S3Dicts example](https://github.com/seung-lab/S3Dicts.jl/blob/master/src/S3Dicts.jl#L29)
- implement the `get_config_dict` function to get a Julia Dict, which defines the datatype and chunk size. [S3Dicts example](https://github.com/seung-lab/S3Dicts.jl/blob/master/src/S3Dicts.jl#L23)

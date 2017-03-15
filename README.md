# BigArrays.jl
storing and accessing large julia array using different backends.

# Features
- N dimension
- arbitrary data type
- arbitrary subset cutout (saving should be chunk size aligned)
- extensible with multiple backends
- arbitrary shape, the dataset boundary can be curve-like
- arbitrary dataset size (in theory, tested dataset size: ~ 9 TB)
- support negative coordinates
- chunk compression
- serverless, clients do IO directly
- highly scalable

## Installation
    Pkg.clone("https://github.com/seung-lab/BigArrays.jl.git")
    
## usage

```julia
using BigArrays.H5sBigArrays
ba = H5sBigArray("/directory/of/hdf5/files/");
# use it as normal array

ba[101:200, 201:300, 1:3] = rand(UInt8, 100,100,3)
@show ba[101:200, 201:300, 1:3]
```

`BigArrays` do not have limit of dataset size, if your reading index is outside of existing file range, will return an array filled with zeros.
   
## supported backends
- [x] hdf5 files. 
- [x] seunglab aligned 2D image hdf5 files.
- [x] cuboids in AWS S3 
- [x] Google Cloud Storage
- [x] [Janelia DVID](https://github.com/janelia-flyem/dvid)
- [ ] [Google Subvolume](https://developers.google.com/brainmaps/v1beta2/rest/v1beta2/volumes/subvolume)
- [ ] [KLB](http://www.nature.com/nprot/journal/v10/n11/abs/nprot.2015.111.html), [the repo](https://bitbucket.org/fernandoamat/keller-lab-block-filetype)

## Usage

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

## Add backend
The backends are different key-value stores. To add a new backend, you can simply do the following:

- wrap the key-value store as a Julia `Associate` type. [S3Dicts is an example](https://github.com/seung-lab/S3Dicts.jl/blob/master/src/S3Dicts.jl#L15) is a good example. 
- implement the `getindex` and `setindex!` functions. [S3Dicts example](https://github.com/seung-lab/S3Dicts.jl/blob/master/src/S3Dicts.jl#L29)
- implement the `get_config_dict` function to get a Julia Dict, which defines the datatype and chunk size. [S3Dicts example](https://github.com/seung-lab/S3Dicts.jl/blob/master/src/S3Dicts.jl#L23)

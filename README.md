BigArrays.jl
============

[![Build Status](https://travis-ci.org/seung-lab/BigArrays.jl.svg?branch=master)](https://travis-ci.org/seung-lab/BigArrays.jl)

Cutout and saving arbitrary chunks in Julia with backends of 
local and cloud storages.

# Introduction  
## Booming of large scale 3D image datasets 
With the augmentation of sample embedding and physical sectioning, modern electon and light microscopes have expanded field of view in the order of magnitudes with high resolution. As a result, we have seen a booming of large scale 3D image datasets around the world in recent years. In most cases, large scale image data can not fit in computer memory and traditional standalone software is not able to handle these datasets. Managing the datasets, including injecting, cutout and visualization, is challenging and getting more and more urgent. 

## Current Solutions  
Almost all the large image handling solutions use precomputed image pyramids, called [mipmaps](https://en.wikipedia.org/wiki/Mipmap). Normally, the images were chopped to small blocks with multiple resolution levels. The blocks were normally compressed with a variaty of algorithms, such as gzip and jpeg. The highest resolution blocks were normally called mip level 0. The higher mip levels were normally built using recursive downsampling. Since the data management software were normally designed and optimized for the storage backend, the solutions could be classified according to the storage architecture. 

For the traditional block storage backend, the blocks could all be saved in one big file and the blocks could be located by disk seek to avoid the filesystem search overhead. However, the internal filesystem increased the software complexity and the dataset size was limited by the largest file size of the filesystem. The blocks could also be realigned based on space filling curves, such as [Hilbert Curve](https://en.wikipedia.org/wiki/Hilbert_curve), for faster reading of neighboring blocks. However, the size of dataset was limited by the largest file size of the local storage. Although single file could also take adavantage of modern [Redundant Array of Independent Disks (RAID)](https://en.wikipedia.org/wiki/RAID) system for parallel high-bandwidth IO. The block IO could normally not taking advantage of the high bandwidth since the block size is normally small. In this case, the latency will become dominant factor of performance. The RAID system have bigger latency and could perform worse than single disk. For example, the commercialized [Amira LDA format](https://www.fei.com/software/amira-avizo-for-large-data-management/) is based on this approach.

For the traditional file system storage, the blocks were managed by the local filesystem. The files could also be shared across machines using network file system, which is normally slower than block storage since it has file search overhead and is normally not distributed across many servers.

For the mordern [object storage](https://en.wikipedia.org/wiki/Object_storage) backend, such as Google Cloud Storage and AWS S3, the meta data was separated and managed by dedicated metadata servers and the IO could be distributed across data servers. Object storage normally have web api and is easy to share files. Thus, it is both fast and easy to share with more complex software and higher maintainance cost.  

| Storage Backend | Advantages             | Disadvantages      | Example               |
| --------------- |:----------------------:| ------------------:| --------------------- |
| Block Storage   | fast                   | not easy to share  | Amira LDA format
| File System     | easy to share          | normally slower    | [TDat](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5534480/)
| Object Storage  | fast and easy to share | more expansive     | [Bossdb](https://bossdb.org/)

## The importance of large scale visualization 
Traditionally, images were visualized with standalone softwares in a single workstation. Although there exist some sophesticated softwares to visualize large scale image datasets, such as [Amira-Avizo](https://www.fei.com/software/amira-avizo-for-large-data-management/) and [TrackEM2](https://imagej.net/TrakEM2), it requires special setup for the users. 

## The rise of Julia in data science 
Data scientists have long been prototyping with dynamically typed language, such as Matlab and python. After the algorithms become stable, they'll start to reimplement the algorithm with faster statically typed language for production run. Julia was designed to solve this two-language problem. 

Data scientists can use Julia interactively with Real-Eval-Print-Loop (REPL) in terminal or Jupyter Notebooks. In the mean time, Julia code could be compiled to native machine code for fast execution thanks for the design of just-in-time compilation with type inference. Julia is getting more and more popular among data scientists since we can explore the data and develop algorithms interactively and also deploy the same code to process large scale of datasets.

## The design of BigArrays.jl
BigArrays.jl was designed with a separation of frontend and backend. The front end provide a Julia Array interface with the same indexing syntax. The backend was abstracted as a Key-Value store and all the storage backend only need to provide a key-value indexing interface.

The saved format is consistent with [neuroglancer](https://github.com/google/neuroglancer) for direct visualization and exploration of large scale image volume. BigArrays also support more compression methods for the fine control of speed and compression ratio. 

# Features
- serverless, clients communicate with storage backends directly. 
The cutout was performed in the client side. 
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
- [x] Local file system
- [x] Google Cloud Storage 
- [x] AWS S3 

Any other storage backends could be mounted in local filesystem will work. For example, shared file system could be supported by mounting the files as local directory. Most of cloud storage could also be mounted and used via local file system backend. 

## compression and decompression
| Algorithm     | compression        | decompression      |
| ------------- |:------------------:| ------------------:|
| gzip          | :white_check_mark: | :white_check_mark: |
| zstd          | :white_check_mark: | :white_check_mark: |
| blosclz       | :white_check_mark: | :white_check_mark: |
| jpeg          | :x:                | :white_check_mark: |

## supported data types
Bool, UInt8, UInt16, UInt32, UInt64, Float32, Float64.
easy to add more, please raise an issue if you need more.

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

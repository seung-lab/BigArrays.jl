module BigArrays

abstract type AbstractBigArray <: AbstractArray{Any,Any} end
using Distributed
using OffsetArrays 
using JSON
using Distributed 
using SharedArrays 

#import .BackendBase: AbstractBigArrayBackend  
# Note that DenseArray only works for memory stored Array
# http://docs.julialang.org/en/release-0.4/manual/arrays/#implementation
export AbstractBigArray, BigArray 

# basic functions
include("BackendBase.jl"); using .BackendBase
include("Codings.jl"); using .Codings;
include("Indexes.jl"); using .Indexes;
include("ChunkIterators.jl"); using .ChunkIterators;
include("backends/include.jl") 

const WORKER_POOL = WorkerPool( workers() )
const GZIP_MAGIC_NUMBER = UInt8[0x1f, 0x8b, 0x08]  
const TASK_NUM = 4
const CHUNK_CHANNEL_SIZE = 2
# map datatype of python to Julia 
const DATATYPE_MAP = Dict{String, DataType}(
    "bool"      => Bool,
    "uint8"     => UInt8, 
    "uint16"    => UInt16, 
    "uint32"    => UInt32, 
    "uint64"    => UInt64, 
    "float32"   => Float32, 
    "float64"   => Float64 
)  

const CODING_MAP = Dict{String,Any}(
    # note that the raw encoding in cloud storage will be automatically gzip encoded!
    "raw"       => GzipCoding,
    "jpeg"      => JPEGCoding,
    "blosclz"   => BlosclzCoding,
    "gzip"      => GzipCoding, 
    "zstd"      => ZstdCoding 
)


include("type.jl")
# the getindex and setindex modes with multithreads, multiprocesses, sequential, sharedarray
include("modes/include.jl")
end # module

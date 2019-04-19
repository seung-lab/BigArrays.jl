__precompile__()
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
include("BackendBase.jl"); using .BackendBase;
include("Codings.jl"); using .Codings;
include("Indexes.jl"); using .Indexes;
include("ChunkIterators.jl"); using .ChunkIterators;
include("Infos.jl"); using .Infos;
include("backends/include.jl") 

const WORKER_POOL = WorkerPool( workers() )
const GZIP_MAGIC_NUMBER = UInt8[0x1f, 0x8b, 0x08]  
const TASK_NUM = 4
const CHUNK_CHANNEL_SIZE = 2

include("type.jl")
# the getindex and setindex modes with multithreads, multiprocesses, sequential, sharedarray
include("modes/include.jl")
end # module

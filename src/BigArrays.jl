__precompile__()
module BigArrays

abstract type AbstractBigArray <: AbstractArray{Any,Any} end

using Distributed
using OffsetArrays 
using JSON

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

const GZIP_MAGIC_NUMBER = UInt8[0x1f, 0x8b, 0x08]  
const CHUNK_CHANNEL_SIZE = 2
const DEFAULT_MODE = :sequential 
const DEFAULT_FILL_MISSING = true 

include("type.jl")
# the getindex and setindex modes with multithreads, multiprocesses, sequential, sharedarray
include("modes/include.jl")
end # module

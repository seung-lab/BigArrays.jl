__precompile__()

module BigArrays

# basic functions
include("types.jl")
include("Chunks.jl")
include("index.jl")
include("BigArrayIterators.jl")
include("ChunkStores.jl")
include("base.jl")

include("backends.jl")


end

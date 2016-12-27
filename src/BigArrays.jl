__precompile__()

module BigArrays

# basic functions
include("types.jl")
include("index.jl")
include("base.jl")
include("chunk.jl")
include("boundingbox.jl")



# chunkstore, use key-value store as backends
include("context.jl")
include("chunkstore.jl")

# applications
include("iterator.jl")
include("backends.jl")
# include("chunks.jl")

end

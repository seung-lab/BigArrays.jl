__precompile__()

module BigArrays

# basic functions
include("types.jl")
include("chunk.jl")
include("index.jl")
include("base.jl")
# include("boundingbox.jl")

# applications
include("iterator.jl")
include("backends.jl")
# include("chunks.jl")


end

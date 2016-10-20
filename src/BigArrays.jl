__precompile__()

module BigArrays

# basic functions
include("types.jl")
include("chunk.jl")
include("boundingbox.jl")
include("index.jl")
include("base.jl")

# applications
include("iterator.jl")
include("backends.jl")
include("chunks.jl")

end

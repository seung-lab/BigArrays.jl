__precompile__()

module BigArrays

# basic functions
include("types.jl")
include("chunks.jl")
include("index.jl")
include("iterators.jl")

include("base.jl")
include("utils.jl")

include("backends.jl")


end

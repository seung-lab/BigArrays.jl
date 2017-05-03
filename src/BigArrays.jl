__precompile__()

module BigArrays

using Retry

# basic functions
include("coding.jl")
using .Coding
include("types.jl")
include("chunks.jl")
include("index.jl")
include("iterators.jl")

include("base.jl")
include("utils.jl")

include("backends.jl")


end

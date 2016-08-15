# the package design was inspired by GPUArrays.jl

module BigArray

include("types.jl")
include("bouldingbox.jl")
include("index.jl")

include("array.jl")
include("backends/backends.jl")

end

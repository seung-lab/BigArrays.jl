__precompile__()

module BigArrays

# basic functions
include("BackendBase.jl"); using .BackendBase
include("Codings.jl"); using .Codings;
include("types.jl");
include("Chunks.jl"); using .Chunks;
include("Indexes.jl"); using .Indexes;
include("Iterators.jl"); using .Iterators;
include("base.jl")
include("Utils.jl"); using .Utils;
include("backends/include.jl") 
end

module ChunkStores

using Blosc
export ChunkStore, get_config_dict

abstract AbstractChunkStore <: Associative

function __init__()
    # use the same number of threads with Julia
    if haskey(ENV, "BLOSC_NUM_THREADS")
        Blosc.set_num_threads( parse(ENV["BLOSC_NUM_THREADS"]) )
    elseif haskey(ENV, "JULIA_NUM_THREADS")
        Blosc.set_num_threads( parse(ENV["JULIA_NUM_THREADS"]) )
    else
        Blosc.set_num_threads(4)
    end
    # use the default compression method
    # Blosc.set_compressor("blosclz")
end

immutable ChunkStore{T,N} <: AbstractChunkStore
    kvStore     ::Associative
    chunkSize   ::NTuple{Int,N}
    function (::Type{ChunkStore}){T,N}( kvStore::Associative,
                                T::DataType, chunkSize::NTuple{Int,N})
        new{T,N}(kvStore, chunkSize)
    end
end

function ChunkStore{T,N}( d::Associative )
    configDict = get_config_dict( d )
    T = eval(parse(configDict[:dataType]))
    chunkSize = ([configDict[:chunkSize]]...)
    ChunkStore( d, T,  chunkSize)
end

function Base.setindex!( chunkStore::ChunkStore{T,N}, v::Array{T,N}, key )
    chunkStore.kvStore[string(key)] = Blosc.compress(v)
end

function Base.getindex( chunkStore::ChunkStore{T,N}, k )
    v = chunkStore.kvStore[string(k)]
    return reshape(Blosc.decompress(T, v), chunkStore.chunkSize)
end

# a function expected to be inherited by backends
# refer the idea of modular design here:
# http://www.juliabloggers.com/modular-algorithms-for-scientific-computing-in-julia/
# a similar function:
# https://github.com/JuliaDiffEq/DiffEqBase.jl/blob/master/src/DiffEqBase.jl#L62
function get_config_dict end

# function Base.getindex{N}( h::Associative, key::CartesianRange{CartesianIndex{N}})
#     h[string(key)]
# end
#
# function Base.setindex!{T,N}( h::Associative, v::Array{T,N}, key::CartesianRange{CartesianIndex{N}} )
#     h[string(key)] = v
# end

end

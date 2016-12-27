export BigArray

include("types.jl")

using Blosc

function __init__()
    # use the same number of threads with Julia
    Blosc.set_num_threads( parse(ENV["JULIA_NUM_THREADS"]) )
    # use the default compression method
    # Blosc.set_compressor("blosclz")
end

"""
    BigArray
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
immutable BigArray{D<:Associative, T, N} <: AbstractArray
    kvStore     ::D
    chunkSize   ::NTuple{N, Int}
    configDict  ::Dict{Symbol, Any}
end

function BigArray{D,T,N}( kvStore::D, dataType::T,
                            chunkSize::NTuple{N,Int};
                            configDict::Dict{Symbol, Any}=Dict{Symbol, Any}() )
    BigArray{D,T,N}(kvStore, chunkSize, configDict)
end

function BigArray( kvStore::Associative )
    configDict = get_config( kvStore )
    BigArray( kvStore, configDict[:dataType], configDict[:chunkSize];
                configDict = configDict )
end

function Base.ndims{D,T,N}(ba::BigArray{D,T,N})
    return N
end

function Base.eltype{D, T, N}( ba::BigArray{D,T,N} )
    return T
end

function Base.size( ba::BigArray )
    # get size according to the keys
    warn("the size was computed according to the keys, which is a number of chunk sizes and is not accurate")
    error("not implemented")
end

function Base.setindex!{D,T,N}( ba::BigArray{D,T,N}, buf::Array{T,N},
                            idxes::Union{UnitRange, Int, Colon}... )
    idxes = colon2unitRange(buf, idxes)
    baIter = BigArrayIterator(idxes, ba.chunkSize)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        chk = ba.kvStore[chunkGlobalRange]
        chk = reshape(Blosc.decompress(T, chk), ba.chunkSize)
        chk[rangeInChunk] = buf[rangeInBuffer]
        ba.kvStore[chunkGlobalRange] = Blosc.compress(chk)
    end
end

function Base.getindex{D,T,N}( ba::BigArray{D, T, N}, idxes::Union{UnitRange, Int}...)
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)
    baIter = BigArrayIterator(idxes, ba.chunkSize)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        chk = ba.kvStore[chunkGlobalRange]
        chk = reshape(Blosc.decompress(T, chk), ba.chunkSize)
        buf[rangeInBuffer] = chk[rangeInChunk]
    end
    return buf
end

function Base.getindex{N}( h::Associative, key::CartesianRange{CartesianIndex{N}})
    h[string(key)]
end

function Base.setindex!{N}( h::Associative, v, key::CartesianRange{CartesianIndex{N}} )
    h[string(key)] = v
end

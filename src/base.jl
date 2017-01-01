export BigArray

using Iterators
using ChunkStores

export BigArray

"""
    BigArray
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
immutable BigArray{D, T, N} <: AbstractBigArray
    chunkStore  ::ChunkStore{K,T,N}
    function (::Type{BigArray}){D,T,N}( chunkStore::ChunkStore{T,N} )
        new{typeof(chunkStore), T, N}(chunkStore)
    end
end

function Base.ndims{D,T,N}(ba::BigArray{D,T,N})
    return N
end

function Base.eltype{D, T, N}( ba::BigArray{D,T,N} )
    @show T
    return T
end

function Base.size{D,T,N}( ba::BigArray{D,T,N} )
    get size according to the keys
    ret = size( CartesianRange(ba) )
    # if all(s->s==0, ret)
    #     ret = map(typemax(Int), ret)
    # end
    # ret = ([typemax(Int) for i=1:N]...)
    return ret
end

function Base.size(ba::BigArray, i::Int)
    size(ba)[i]
end

function Base.show(ba::BigArray)
    display(ba)
end

function Base.display(ba::BigArray)
    for field in fieldnames(ba)
        println("$field: $(ba.(field))")
    end
end

function Base.reshape{D,T,N}(ba::BigArray{D,T,N}, newShape)
    warn("reshape failed, the shape of bigarray is immutable!")
end

# function Base.linearindexing(ba::BigArray)
#     Base.LinearFast()
# end

function Base.CartesianRange{D,T,N}( ba::BigArray{D,T,N} )
    warn("the size was computed according to the keys, which is a number of chunk sizes and is not accurate")
    keyList = keys(ba.chunkStore)
    ret = CartesianRange(
            CartesianIndex([typemax(Int) for i=1:N]...),
            CartesianIndex([0            for i=1:N]...))
    for key in keyList
        union!(ret, CartesianRange(key))
    end
    ret
end

"""
    put array in RAM to a BigArray
"""
# function Base.setindex!{D,T,N}( ba::BigArray{D,T,N}, buf::Array{T,N},
#                                 idxes::Union{UnitRange, Int, Colon}... )
function Base.setindex!{T,N}( ba::BigArray, buf::Array{T,N},
                                idxes::Union{UnitRange, Int, Colon} ... )
    @assert eltype(ba) == T
    @assert ndims(ba) == N
    @show idxes
    idxes = colon2unitRange(buf, idxes)
    baIter = BigArrayIterator(idxes, ba.chunkSize)
    chk = Array(T, ba.chunkSize)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        # chk = ba.chunkStore[chunkGlobalRange]
        # chk = reshape(Blosc.decompress(T, chk), ba.chunkSize)
        fill!(chk, convert(T, 0))
        chk[rangeInChunk] = buf[rangeInBuffer]
        ba.chunkStore[chunkGlobalRange] = chk
    end
end

function Base.getindex{D,T,N}( ba::BigArray{D, T, N}, idxes::Union{UnitRange, Int}...)
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)
    baIter = BigArrayIterator(idxes, ba.chunkSize)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        chk = ba.chunkStore[chunkGlobalRange]
        buf[rangeInBuffer] = chk[rangeInChunk]
    end
    return buf
end

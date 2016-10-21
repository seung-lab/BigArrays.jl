module Iterators

using ..BigArrays

export BigArrayIterator

immutable BigArrayIterator{N}
    globalRange     ::CartesianRange{CartesianIndex{N}}
    blockSize       ::NTuple{N}
end

function BigArrayIterator( ba::AbstractBigArray )
    BigArrayIterator( ba.globalRange, ba.blockSize )
end

function Base.length( iter::BigArrayIterator )
    length( iter.globalRange )
end

function Base.eltype( iter::BigArrayIterator )
    eltype( iter.globalRange )
end

function Base.start( iter::BigArrayIterator )
    blockID = index2blockid( iter.globalRange.start, iter.blockSize )
    @show blockID
    return blockID
end

"""
    Base.next( iter::BigArrayIterator, state::CartesianRange )

increase start coordinate following the column-order.
"""
function Base.next{N}( iter::BigArrayIterator{N}, blockID::NTuple{N} )
    # get current global range in this block
    start = CartesianIndex(( map((x,y,z)->max((x-1)*y+1, z), blockID,
                            iter.blockSize, iter.globalRange.start )...))
    stop  = CartesianIndex(( map((x,y,z)->min(x*y, z),       blockID,
                            iter.blockSize, iter.globalRange.stop )...))
    globalRange = CartesianRange(start, stop)
    @show globalRange
    blockRange  = global_range2block_range( globalRange, iter.blockSize)
    bufferRange = global_range2buffer_range(globalRange, iter.globalRange)

    # find next blockID
    for i in 1:N
        if blockID[i]*iter.blockSize[i]+1 < iter.globalRange.stop[i]
            newBlockID = (blockID[1:i-1]..., blockID[i]+1, blockID[i+1:end]...)
            return (blockID, globalRange, blockRange, bufferRange), newBlockID
        end
    end
    newBlockID = (blockID[1:N-1]..., blockID[N]+1)
    return (blockID, globalRange, blockRange, bufferRange), newBlockID
end

"""
    Base.done( iter::BigArrayIterator,  state::CartesianRange)

if all the axeses were saturated, stop the iteration.
"""
function Base.done{N}( iter::BigArrayIterator{N},  blockID::NTuple{N})
    if (blockID[N]-1)*iter.blockSize[N]+1 > iter.globalRange.stop[N]
        return true
    else
        return false
    end
end

end # end of module

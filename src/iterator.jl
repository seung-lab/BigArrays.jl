module Iterators

using ..BigArrays

export BigArrayIterator

immutable BigArrayIterator{N}
    globalRange     ::CartesianRange{CartesianIndex{N}}
    blockSize       ::NTuple{N}
    blockIDRange    ::CartesianRange{CartesianIndex{N}}
end

function BigArrayIterator{N}( globalRange::CartesianRange{CartesianIndex{N}},
                            blockSize::NTuple{N})
    blockIDStart = CartesianIndex(index2blockid( globalRange.start, blockSize ))
    blockIDStop  = CartesianIndex(index2blockid( globalRange.stop,  blockSize ))
    blockIDRange = CartesianRange(blockIDStart, blockIDStop)
    BigArrayIterator( globalRange, blockSize, blockIDRange )
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

"""
the state is a tuple {blockID, and the dimension that is increasing}
"""
function Base.start( iter::BigArrayIterator )
    iter.blockIDRange.start
end

"""
    Base.next( iter::BigArrayIterator, state::CartesianRange )

increase start coordinate following the column-order.
"""
function Base.next{N}(  iter    ::BigArrayIterator{N},
                        state   ::CartesianIndex{N} )
    blockIDIndex, state = next(iter.blockIDRange, state)
    blockID = tuple(blockIDIndex.I...)

    # get current global range in this block
    start = CartesianIndex(( map((x,y,z)->max((x-1)*y+1, z), blockID,
                            iter.blockSize, iter.globalRange.start )...))
    stop  = CartesianIndex(( map((x,y,z)->min(x*y, z),       blockID,
                            iter.blockSize, iter.globalRange.stop )...))
    globalRange = CartesianRange(start, stop)
    @show globalRange
    blockRange  = global_range2block_range( globalRange, iter.blockSize)
    bufferRange = global_range2buffer_range(globalRange, iter.globalRange)

    return (blockID, globalRange, blockRange, bufferRange), state
end

"""
    Base.done( iter::BigArrayIterator,  state::CartesianRange )

if all the axeses were saturated, stop the iteration.
"""
function Base.done{N}(  iter::BigArrayIterator{N},
                        state::CartesianIndex{N})
    done(iter.blockIDRange, state)
end

end # end of module

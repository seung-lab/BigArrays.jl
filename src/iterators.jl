module BigArrayIterators

using ..BigArrays

export BigArrayIterator

immutable BigArrayIterator{N}
    globalRange     ::CartesianRange{CartesianIndex{N}}
    chunkSize       ::NTuple{N}
    chunkIDRange    ::CartesianRange{CartesianIndex{N}}
end

function BigArrayIterator{N}( globalRange::CartesianRange{CartesianIndex{N}},
                            chunkSize::NTuple{N})
    chunkIDStart = CartesianIndex(index2chunkid( globalRange.start, chunkSize ))
    chunkIDStop  = CartesianIndex(index2chunkid( globalRange.stop,  chunkSize ))
    chunkIDRange = CartesianRange(chunkIDStart, chunkIDStop)
    BigArrayIterator( globalRange, chunkSize, chunkIDRange )
end

function BigArrayIterator{N}( idxes::Tuple,
                                chunkSize::NTuple{N})
    globalRange = CartesianRange(idxes)
    BigArrayIterator( globalRange, chunkSize )
end

function BigArrayIterator( ba::AbstractBigArray )
    BigArrayIterator( ba.globalRange, ba.chunkSize )
end

function Base.length( iter::BigArrayIterator )
    length( iter.globalRange )
end

function Base.eltype( iter::BigArrayIterator )
    eltype( iter.globalRange )
end

"""
the state is a tuple {chunkID, and the dimension that is increasing}
"""
function Base.start( iter::BigArrayIterator )
    iter.chunkIDRange.start
end

"""
    Base.next( iter::BigArrayIterator, state::CartesianRange )

increase start coordinate following the column-order.
"""
function Base.next{N}(  iter    ::BigArrayIterator{N},
                        state   ::CartesianIndex{N} )
    chunkIDIndex, state = next(iter.chunkIDRange, state)
    chunkID = tuple(chunkIDIndex.I...)

    # get current global range in this chunk
    start = CartesianIndex(( map((x,y,z)->max((x-1)*y+1, z), chunkID,
                            iter.chunkSize, iter.globalRange.start )...))
    stop  = CartesianIndex(( map((x,y,z)->min(x*y, z),       chunkID,
                            iter.chunkSize, iter.globalRange.stop )...))
    # the global range of the cutout in this chunk
    globalRange = CartesianRange(start, stop)
    @show globalRange
    # the range inside this chunk
    rangeInChunk  = global_range2chunk_range( globalRange, iter.chunkSize)
    # the range inside the buffer
    rangeInBuffer = global_range2buffer_range(globalRange, iter.globalRange)
    # the global range of this chunk
    chunkGlobalRange = chunkid2global_range( chunkID, iter.chunkSize )
    return (chunkID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer), state
end

"""
    Base.done( iter::BigArrayIterator,  state::CartesianRange )

if all the axeses were saturated, stop the iteration.
"""
function Base.done{N}(  iter::BigArrayIterator{N},
                        state::CartesianIndex{N})
    done(iter.chunkIDRange, state)
end

end # end of module

module Iterators
using ..BigArrays
using ..BigArrays.Indexes

import ..BigArrays: AbstractBigArray

export Iterator

struct Iterator{N}
    globalRange     ::CartesianRange{CartesianIndex{N}}
    chunkSize       ::NTuple{N}
    chunkIDRange    ::CartesianRange{CartesianIndex{N}}
    # this offset really means the starting coordinate of the real data
    offset          ::CartesianIndex{N} 
end

function Iterator( globalRange::CartesianRange{CartesianIndex{N}},
                           chunkSize::NTuple{N};
                           offset::CartesianIndex{N} = CartesianIndex{N}() - 1 ) where N
    chunkIDStart = CartesianIndex(index2chunkid( globalRange.start, chunkSize; offset=offset ))
    chunkIDStop  = CartesianIndex(index2chunkid( globalRange.stop,  chunkSize; offset=offset ))
    chunkIDRange = CartesianRange(chunkIDStart, chunkIDStop)
    Iterator( globalRange, chunkSize, chunkIDRange, offset )
end

function Iterator(   idxes::Tuple,
                     chunkSize::NTuple{N};
                     offset::CartesianIndex{N} = CartesianIndex{N}()-1) where N
    # the offset in neuroglancer really means the starting coordinate of valid data
    # since bigarray assumes infinite data range, here we only need to use it to adjust the alignment of chunks 
    # so we only use the mod to make offset
    offset = CartesianIndex( map((o,c) -> mod(o,c), offset.I, chunkSize) ) 
    idxes = map(index2unit_range, idxes)
    globalRange = CartesianRange(idxes)
    Iterator( globalRange, chunkSize; offset=offset )
end

function Iterator( ba::AbstractBigArray )
    Iterator( ba.globalRange, ba.chunkSize )
end

function Base.length( iter::Iterator )
    length( iter.globalRange )
end

function Base.eltype( iter::Iterator )
    eltype( iter.globalRange )
end

"""
the state is a tuple {chunkID, and the dimension that is increasing}
"""
function Base.start( iter::Iterator )
    iter.chunkIDRange.start
end

"""
    Base.next( iter::Iterator, state::CartesianRange )

increase start coordinate following the column-order.
"""
function Base.next(  iter    ::Iterator{N},
                     state   ::CartesianIndex{N} ) where N
    chunkIDIndex, state = next(iter.chunkIDRange, state)
    chunkID = chunkIDIndex.I

    # get current global range in this chunk
    start = CartesianIndex( map((x,y,z,o)->max((x-1)*y+1+o, z), chunkID,
                            iter.chunkSize, iter.globalRange.start.I,
                            iter.offset.I ))
    stop  = CartesianIndex( map((x,y,z,o)->min(x*y+o, z),       chunkID,
                            iter.chunkSize, iter.globalRange.stop.I,
                            iter.offset.I ))
    # the global range of the cutout in this chunk
    globalRange = CartesianRange(start, stop)
    # the range inside this chunk
    rangeInChunk  = global_range2chunk_range( globalRange, iter.chunkSize; offset=iter.offset)
    # the range inside the buffer
    rangeInBuffer = global_range2buffer_range(globalRange, iter.globalRange)
    # the global range of this chunk
    chunkGlobalRange = chunkid2global_range( chunkID, iter.chunkSize; offset=iter.offset )
    return (chunkID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer), state
end

"""
    Base.done( iter::Iterator,  state::CartesianRange )

if all the axeses were saturated, stop the iteration.
"""
function Base.done(  iter::Iterator{N},
                     state::CartesianIndex{N}) where N
    done(iter.chunkIDRange, state)
end

end # end of module

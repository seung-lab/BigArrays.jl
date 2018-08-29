module Iterators
using ..BigArrays
using ..BigArrays.Indexes

import ..BigArrays: AbstractBigArray

export Iterator

struct Iterator{N}
    globalRange     ::CartesianIndices{N}
    chunkSize       ::NTuple{N}
    chunkIDRange    ::CartesianIndices{N}
    # this offset really means the starting coordinate of the real data
    offset          ::CartesianIndex{N} 
end

function Iterator( globalRange::CartesianIndices{N},
                           chunkSize::NTuple{N};
                           offset::CartesianIndex{N} = CartesianIndex{N}() - 1 ) where N
    chunkIDStart = CartesianIndex(index2chunkid( first(globalRange), chunkSize; offset=offset ))
    chunkIDStop  = CartesianIndex(index2chunkid( last(globalRange),  chunkSize; offset=offset ))
    chunkIDRange = CartesianIndices(chunkIDStart, chunkIDStop)
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
    globalRange = CartesianIndices(idxes)
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

function Base.iterate(iter::Iterator, state=iter.chunkIdRange.start)
    if done(iter.chunkIDRange, state)
        return nothing 
    end 
    #chunkIDIndex, state = next(iter.chunkIDRange, state)
    #chunkID = chunkIDIndex.I
    chunkID = state.I

    # get current global range in this chunk
    start = CartesianIndex( map((x,y,z,o)->max((x-1)*y+1+o, z), chunkID,
                                iter.chunkSize, first(iter.globalRange).I,
                            iter.offset.I ))
    stop  = CartesianIndex( map((x,y,z,o)->min(x*y+o, z),       chunkID,
                                iter.chunkSize, last(iter.globalRange).I,
                            iter.offset.I ))
    # the global range of the cutout in this chunk
    cutoutGlobalRange = CartesianIndices(start, stop)
    # the range inside this chunk
    rangeInChunk  = global_range2chunk_range( cutoutGlobalRange, iter.chunkSize; offset=iter.offset)
    # the range inside the buffer
    rangeInBuffer = global_range2buffer_range(cutoutGlobalRange, iter.globalRange)
    # the global range of this chunk
    chunkGlobalRange = chunkid2global_range( chunkID, iter.chunkSize; offset=iter.offset )

    nextChunkIDIndex, nextState = next(iter.chunkIDRange, state)

    return (chunkID, chunkGlobalRange, cutoutGlobalRange, rangeInChunk, rangeInBuffer), nextState
end  

end # end of module

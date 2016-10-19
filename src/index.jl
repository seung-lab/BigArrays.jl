module BigArrayIterators

using ..BigArrays

export BigArrayIterator, global_range2buffer_range, global_range2block_range
export colon2unitRange, blockid2global_range, index2blockid

# iteration for CartesianIndex
function Base.start{N}( idx::CartesianIndex{N} )
    1
end

function Base.next{N}( idx::CartesianIndex{N}, state::Integer )
    return idx[state], state+1
end

function Base.done{N}( idx::CartesianIndex{N}, state::Integer )
    state > N
end

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
    blockRange  = global_range2block_range( globalRange, iter.blockSize)
    bufferRange = global_range2buffer_range(globalRange, iter.globalRange)

    # find next blockID
    for i in 1:N
        if blockID[i]*iter.blockSize[i]+1 < iter.globalRange.stop[i]
            newBlockID = (blockID[1:i-1]..., blockID[i]+1, blockID[i+1:end]...)
            return globalRange, newBlockID
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

"""
    global_range2buffer_range(globalRange::CartesianRange, bufferGlobalRange::CartesianRange)

Transform a global range to a range inside buffer.
"""
function global_range2buffer_range{N}(globalRange::CartesianRange{CartesianIndex{N}},
                                    bufferGlobalRange::CartesianRange{CartesianIndex{N}})
    start = globalRange.start - bufferGlobalRange.start + 1
    stop  = globalRange.stop  - bufferGlobalRange.start + 1
    return CartesianRange( start, stop )
end

"""
    global_range2buffer_range(globalRange::CartesianRange, bufferGlobalRange::CartesianRange)

Transform a global range to a range inside block.
"""
function global_range2block_range{N}(globalRange::CartesianRange{CartesianIndex{N}},
                                    blockSize::NTuple{N})
    blockID = index2blockid(globalRange.start, blockSize)
    start = CartesianIndex((map((x,y,z)->x-(y-1)*z, globalRange.start,
                                blockID, blockSize)...))
    stop  = CartesianIndex((map((x,y,z)->x-(y-1)*z, globalRange.stop,
                                blockID, blockSize)...))
    return CartesianRange(start, stop)
end

function index2blockid{N}(idx::CartesianIndex{N}, blockSize::NTuple{N})
    ( map((x,y)->div(x-1, y)+1, idx, blockSize) ... )
end

function blockid2global_range{N}(blockID::NTuple{N}, blockSize::NTuple{N})
    start = CartesianIndex( map((x,y)->(x-1)*y+1, blockID, blockSize) )
    stop  = CartesianIndex( map((x,y)->x*y,       blockID, blockSize) )
    return CartesianRange(start, stop)
end

"""
replace Colon of indexes by UnitRange
"""
function colon2unitRange{N}(buf::Union{Array,AbstractBigArray}, indexes::NTuple{N})
    colon2unitRange(size(buf), indexes)
end

function colon2unitRange{N}(sz::NTuple{N}, indexes::NTuple{N})
    map((x,y)-> x==Colon() ? UnitRange(1:y):x, indexes, sz)
end

end # end of module

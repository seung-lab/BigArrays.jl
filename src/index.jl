export blockid, GlobalIndex, globalRange2localRange, globalIndex2blockIndex, globalIndex2bufferIndex, globalIndexes2bufferIndexes, globalIndexes2blockIndexes
export colon2unitRange

"""
get blockid from a coordinate
"""
function blockid(c::Int, blockSize::Integer)
    div(c-1, blockSize)+1
end

function blockid(idx::UnitRange, blockSize::Integer)
    bid1 = blockid(idx.start, blockSize)
    bid2 = blockid(idx.stop, blockSize)
    @assert bid1 == bid2
    bid1
end

function blockid(t::Tuple{UnitRange, Int})
    blockid(t[1], t[2])
end

function blockid(idxes::Tuple, blockSize::Union{Vector, Tuple})
    map(x->blockid(x[1], x[2]), zip(idxes, blockSize))
end

"""
transform one global UnitRange (inside a block) to local UnitRange in a block
"""
function globalRange2localRange(globalRange::UnitRange, blockSize::Integer)
    # make sure that this range is within a block
    @assert length(globalRange) <= blockSize
    # they belong to a same block
    @assert blockid(globalRange.start, blockSize) == blockid(globalRange.stop, blockSize)
    # block id
    ((globalRange.start-1)%blockSize+1) : ((globalRange.stop-1)%blockSize+1)
end

"""
transform global UnitRange (inside a block) to local UnitRange
"""
function globalRange2localRange(globalRange::Int, blockSize::Int)
    (globalRange-1)%blockSize+1
end

# iterater of global index
type GlobalIndex
    idx::Union{UnitRange, Int}
    blockSize::Integer
end

function GlobalIndex( t::Tuple{UnitRange, Int})
    GlobalIndex(t[1], t[2])
end

function Base.length( gidx::GlobalIndex )
    length(gidx.idx)
end

function Base.start(globalIndex::GlobalIndex)
    if isa(globalIndex.idx, Int)
        # @show globalIndex
        return globalIndex.idx
    else
        @assert isa(globalIndex.idx, UnitRange)
        start = globalIndex.idx.start
        # block id of the first
        bid = blockid(globalIndex.idx.start, globalIndex.blockSize)
        stop = min(globalIndex.idx.stop, bid*globalIndex.blockSize)
        # @show globalIndex, start, stop, bid
        return start:stop
    end
end

function Base.done(globalIndex::GlobalIndex, idx::UnitRange)
    idx.start > globalIndex.idx.stop
end

function Base.done(globalIndex::GlobalIndex, idx::Int)
    idx > globalIndex.idx
end

function Base.done(globalIndex::GlobalIndex, state::Tuple)
    idx, bid = state
    done(globalIndex, idx)
end

function Base.next(globalIndex::GlobalIndex, idx::UnitRange)
    # next blockid
    nbid = blockid(idx, globalIndex.blockSize) + 1
    # get new index state
    nstart = (nbid-1) * globalIndex.blockSize + 1
    nstop = min(globalIndex.idx.stop, idx.stop+globalIndex.blockSize)
    # return current index and next index
    return idx, nstart:nstop
end

function Base.next(globalIndex::GlobalIndex, idx::Int)
    # next block id
    nbid = blockid(idx, globalIndex.blockSize) + 1
    nstart = (nbid-1) * globalIndex.blockSize + 1
    # return current index and next index
    return idx, nstart
end



"""
compute the index inside a block based on global index, block size and block id
"""
function globalIndex2blockIndex(globalIndex::Union{UnitRange, Int}, blockSize::Integer)
    bid = blockid(globalIndex, blockSize)
    globalIndex - (bid-1)*blockSize
end

function globalIndexes2blockIndexes(globalIndexes::Tuple, blockSize::Union{Vector, Tuple})
    @assert length(globalIndexes) == length(blockSize)
    ret = []
    for i = 1:length(blockSize)
        push!(ret, globalIndex2blockIndex(globalIndexes[i], blockSize[i]))
    end
    return (ret...)
    # map(globalIndex2blockIndex, zip(globalIndexes, blockSize))
end

function getstart(idx::UnitRange)
    idx.start
end
function getstart(idx::Int)
    idx
end

"""
compute buffer index
"""
function globalIndex2bufferIndex(globalIndex::Union{UnitRange, Int, Colon}, bufferIndex::Union{Int, UnitRange, Colon})
    bufstart = getstart(bufferIndex)
    globalIndex - bufstart + 1
end

function globalIndexes2bufferIndexes(globalIndexes::Tuple, bufferIndexes::Tuple)
    map(globalIndex2bufferIndex, zip(globalIndexes, bufferIndexes))
end

"""
replace Colon of indexes by UnitRange
"""
function colon2unitRange(buf::Union{Array,AbstractBigArray}, indexes::Tuple)
    colon2unitRange(size(buf), indexes)
end

function colon2unitRange(sz::Tuple, indexes::Tuple)
    map((x,y)-> x==Colon() ? UnitRange(1:y):x, indexes, sz)
end

export blockid, GlobalIndex, globalRange2localRange, globalIndex2blockIndex, globalIndex2bufferIndex, globalIndexes2bufferIndexes, globalIndexes2blockIndexes

"""
get blockid from a coordinate
"""
function blockid(c::Int, blockSize::Int)
    div(c-1, blockSize)+1
end

function blockid(idx::UnitRange, blockSize::Int)
    bid1 = blockid(idx.start, blockSize)
    bid2 = blockid(idx.stop, blockSize)
    @assert bid1 == bid2
    bid1
end

function blockid(idxs::Tuple, blockSize::Union{Vector, Tuple})
    bidx = blockid(idxs[1], blockSize[1])
    bidy = blockid(idxs[2], blockSize[2])
    bidz = blockid(idxs[3], blockSize[3])
    (bidx, bidy, bidz)
end

"""
transform one global UnitRange (inside a block) to local UnitRange in a block
"""
function globalRange2localRange(globalRange::UnitRange, blockSize::Int)
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
    blockSize::Int
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
function globalIndex2blockIndex(globalIndex::Union{UnitRange, Int}, blockSize::Int)
    bid = blockid(globalIndex, blockSize)
    globalIndex - (bid-1)*blockSize
end

function globalIndexes2blockIndexes(globalIndexes::Tuple, blockSize::Union{Vector, Tuple})
    # @assert length(globalIndexes) == length(blockSize)
    blkix = globalIndex2blockIndex(globalIndexes[1], blockSize[1])
    blkiy = globalIndex2blockIndex(globalIndexes[2], blockSize[2])
    blkiz = globalIndex2blockIndex(globalIndexes[3], blockSize[3])
    (blkix, blkiy, blkiz)
    # if length(globalIndexes)==3
    #   return (blkix, blkiy, blkiz)
    # else
    #   return (blkix, blkiy, blkiz, :)
    # end
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
    # @show globalIndex, bufferIndex
    bufstart = getstart(bufferIndex)
    globalIndex - bufstart + 1
end

function globalIndexes2bufferIndexes(globalIndexes::Tuple, bufferIndexes::Tuple)
  # @show globalIndexes
  # @show bufferIndexes
    # @assert length(globalIndexes) == length(bufferIndexes)
    bufix = globalIndex2bufferIndex(globalIndexes[1], bufferIndexes[1])
    bufiy = globalIndex2bufferIndex(globalIndexes[2], bufferIndexes[2])
    bufiz = globalIndex2bufferIndex(globalIndexes[3], bufferIndexes[3])
    (bufix, bufiy, bufiz)
    # if length(globalIndexes)==3
    #   return (bufix, bufiy, bufiz)
    # else
    #   return (bufix, bufiy, bufiz, :)
    # end
end

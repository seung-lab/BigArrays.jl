
"""
get blockid from a coordinate
"""
function blockid(c::Int, bsz::Int)
    div(c-1, bsz)+1
end

function blockid(idx::UnitRange, bsz::Int)
    bid1 = blockid(idx.start, bsz)
    bid2 = blockid(idx.stop, bsz)
    @assert bid1 == bid2
    bid1
end

function blockid(idxs::Tuple, blocksz::Vector)
    bidx = blockid(idxs[1], blocksz[1])
    bidy = blockid(idxs[2], blocksz[2])
    bidz = blockid(idxs[3], blocksz[3])
    (bidx, bidy, bidz)
end

"""
transform one global UnitRange (inside a block) to local UnitRange in a block
"""
function gr2lr(gr::UnitRange, bsz::Int)
    # make sure that this range is within a block
    @assert length(gr) <= bsz
    # they belong to a same block
    @assert blockid(gr.start, bsz) == blockid(gr.stop, bsz)
    # block id
    ((gr.start-1)%bsz+1) : ((gr.stop-1)%bsz+1)
end

"""
transform global UnitRange (inside a block) to local UnitRange
"""
function gr2lr(gr::Int, bsz::Int)
    (gr-1)%bsz+1
end

# iterater of global index
type TGIdxs
    idx::Union{UnitRange, Int}
    bsz::Int
end

function Base.start(gidxs::TGIdxs)
    if isa(gidxs.idx, Int)
        # @show gidxs
        return gidxs.idx
    else
        @assert isa(gidxs.idx, UnitRange)
        start = gidxs.idx.start
        # block id of the first
        bid = blockid(gidxs.idx.start, gidxs.bsz)
        stop = min(gidxs.idx.stop, bid*gidxs.bsz)
        # @show gidxs, start, stop, bid
        return start:stop
    end
end

function Base.done(gidxs::TGIdxs, idx::UnitRange)
    idx.start > gidxs.idx.stop
end

function Base.done(gidxs::TGIdxs, idx::Int)
    idx > gidxs.idx
end

function Base.done(gidxs::TGIdxs, state::Tuple)
    idx, bid = state
    done(gidxs, idx)
end

function Base.next(gidxs::TGIdxs, idx::UnitRange)
    # next blockid
    nbid = blockid(idx, gidxs.bsz) + 1
    # get new index state
    nstart = (nbid-1) * gidxs.bsz + 1
    nstop = min(gidxs.idx.stop, idx.stop+gidxs.bsz)
    # return current index and next index
    return idx, nstart:nstop
end

function Base.next(gidxs::TGIdxs, idx::Int)
    # next block id
    nbid = blockid(idx, gidxs.bsz) + 1
    nstart = (nbid-1) * gidxs.bsz + 1
    # return current index and next index
    return idx, nstart
 end

"""
compute the index inside a block based on global index, block size and block id
"""
function gidx2blkidx(gidx::Union{UnitRange, Int}, bsz::Int)
    bid = blockid(gidx, bsz)
    gidx - (bid-1)*bsz
end

function gidx2blkidx(gidxs::Tuple, blocksz::Vector)
    @assert length(gidxs) == length(blocksz)
    blkix = gidx2blkidx(gidxs[1], blocksz[1])
    blkiy = gidx2blkidx(gidxs[2], blocksz[2])
    blkiz = gidx2blkidx(gidxs[3], blocksz[3])
    (blkix, blkiy, blkiz)
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
function gidx2bufidx(gidx::Union{UnitRange, Int}, bufidx::Union{Int, UnitRange})
    # @show gidx, bufidx
    bufstart = getstart(bufidx)
    gidx - bufstart + 1
end

function gidx2bufidx(gidxs::Tuple, bufidxs::Tuple)
    @assert length(gidxs) == length(bufidxs)
    bufix = gidx2bufidx(gidxs[1], bufidxs[1])
    bufiy = gidx2bufidx(gidxs[2], bufidxs[2])
    bufiz = gidx2bufidx(gidxs[3], bufidxs[3])
    (bufix, bufiy, bufiz)
end

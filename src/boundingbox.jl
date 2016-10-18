# __precompile__()
# module BoundingBox

export BoundingBox, adjust_range!

# type of bounding box
# typealias BoundingBox Tuple
type BoundingBox
    start::Vector{Int}
    stop::Vector{Int}
end

function BoundingBox()
    BoundingBox([Inf,Inf,Inf], [-Inf,-Inf,-Inf])
end

"""
get size of the bounding box
"""
function Base.size(bb::BoundingBox)
    bb.stop .- bb.start .+ 1
end

"""
fit in one more chunk to adjust boundingbox
"""
function blendchunk!(bb::BoundingBox, chk::Chunk)
    bb.start = min(bb.start, chk.origin)
    bb.stop  = max(bb.stop,  chk.origin .+ [size(chk.data)...] .- 1 )
end

function blendchunk!(bb::BoundingBox, idxes::Union{UnitRange, Int, Colon}...)
    bb.start = min(bb.start, [map(first, idxes)...])
    bb.stop  = max(bb.stop,  [map(last,  idxes)...])
end

"""
adjust bigarray bounding box range when fitting in new subarray
"""
function adjust_range!(ba::AbstractBigArray, idxes::CartesianRange)
    @assert length(ba.cartesianRange.start) == length(idxes.start)
    start = min(ba.cartesianRange.start, idxes.start)
    stop  = max(ba.cartesianRange.stop,  idxes.stop)
    ba.cartesianRange = CartesianRange(start, stop)
    @show ba.cartesianRange
end

function adjust_range!(ba::AbstractBigArray, idxes::Tuple)
    adjust_range!(ba, CartesianRange(idxes))
end

# end # end of module BoundingBox

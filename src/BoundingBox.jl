export BoundingBox

# type of bounding box
# typealias BoundingBox Tuple
type BoundingBox
    start::Vector{Int}
    stop::Vector{Int}
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

function blendchunk!(bb::BoundingBox, buf::Array, idxes::Union{UnitRange, Int, Colon}...)
    bb.start = min(bb.start, [map(first, idxes)...])
    bb.stop  = max(bb.stop,  [map(last,  idxes)...])
end

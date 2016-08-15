module BoundingBox

export BoundingBox

# type of bounding box
typealias BoundingBox Tuple{UnitRange, UnitRange, UnitRange}

"""
get size of the bounding box
"""
function Base.size(bb::BoundingBox)
    sz = Vector{Int}()
    for idx in bb
        push!(sz, length(idx))
    end
    (sz...)
end

end

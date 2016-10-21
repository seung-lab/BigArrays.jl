export colon2unitRange, blockid2global_range, index2blockid
export global_range2buffer_range, global_range2block_range
export cartesian_range2unitrange

# make Array accept cartetian range as index
function cartesian_range2unitrange(r::CartesianRange)
    ( map((x,y)->x:y, r.start, r.stop)...)
end

function Base.getindex{T,N}(A::Array{T,N},
                            range::CartesianRange{CartesianIndex{N}})
    ur = cartesian_range2unitrange( range )
    @show ur
    A[ur...]
end

function Base.setindex!{T,N}(A::Array{T,N}, buf::Array{T,N},
                                range::CartesianRange{CartesianIndex{N}})
    @assert size(buf) == size(range)
    ur = cartesian_range2unitrange( range )
    @show ur
    A[ur...] = buf
end

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

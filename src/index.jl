export colon2unitRange, blockid2global_range, index2blockid
export global_range2buffer_range, global_range2block_range
export cartesian_range2unitrange

# make Array accept cartetian range as index
function cartesian_range2unitrange{N}(r::CartesianRange{CartesianIndex{N}})
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
    ( map((x,y)->fld(x-1, y)+1, idx, blockSize) ... )
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

function Base.CartesianIndex(idx::Union{Tuple,Vector})
    CartesianIndex((idx...))
end


function Base.string{N}(r::CartesianRange{CartesianIndex{N}})
    ret = ""
    for d in 1:N
        s = r.start.I[d]
        e = r.stop.I[d]
        ret *= "$s:$e_"
    end
    return ret[1:end-1]
end

function Base.string( idxes::UnitRange...)
    ret = ""
    ret = map(x->"$(start(x)):$(x[end])_", idxes)
    return ret[1:end-1]
end

function Base.CartesianRange( str::String )
    secs = split(str, "_")
    N = length(secs)
    s = CartesianIndex{N}()
    e = CartesianIndex{N}()
    for i in 1:N
        s[i] = parse( split(secs[i+1],":")[1] )
        e[i] = parse( split(secs[i+1],":")[2] )
    end
    return CartesianRange(s,e)
end

"""
    adjust bounding box range when fitting in new subarray
"""
function Base.union(globalRange::CartesianRange, idxes::CartesianRange)
    start = min(globalRange.start, idxes.start)
    stop  = max(globalRange.stop,  idxes.stop)
    return CartesianRange(start, stop)
end
function Base.union!(r1::CartesianRange, r2::CartesianRange)
    r1 = union(r1, r2)
end

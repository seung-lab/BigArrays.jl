export colon2unitRange, chunkid2global_range, index2chunkid
export global_range2buffer_range, global_range2chunk_range
export cartesian_range2unitrange

# make Array accept cartetian range as index
function cartesian_range2unitrange{N}(r::CartesianRange{CartesianIndex{N}})
    ( map((x,y)->x:y, r.start, r.stop)...)
end

function Base.getindex{T,N}(A::Array{T,N},
                            range::CartesianRange{CartesianIndex{N}})
    ur = cartesian_range2unitrange( range )
    # @show ur
    A[ur...]
end

function Base.setindex!{T,N}(A::Array{T,N}, buf::Array{T,N},
                                range::CartesianRange{CartesianIndex{N}})
    @assert size(buf) == size(range)
    ur = cartesian_range2unitrange( range )
    # @show ur
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

Transform a global range to a range inside chunk.
"""
function global_range2chunk_range{N}(globalRange::CartesianRange{CartesianIndex{N}},
                                    chunkSize::NTuple{N})
    chunkID = index2chunkid(globalRange.start, chunkSize)
    start = CartesianIndex((map((x,y,z)->x-(y-1)*z, globalRange.start,
                                chunkID, chunkSize)...))
    stop  = CartesianIndex((map((x,y,z)->x-(y-1)*z, globalRange.stop,
                                chunkID, chunkSize)...))
    return CartesianRange(start, stop)
end

function index2chunkid{N}(idx::CartesianIndex{N}, chunkSize::NTuple{N})
    ( map((x,y)->fld(x-1, y)+1, idx, chunkSize) ... )
end

function chunkid2global_range{N}(chunkID::NTuple{N}, chunkSize::NTuple{N})
    start = CartesianIndex( map((x,y)->(x-1)*y+1, chunkID, chunkSize) )
    stop  = CartesianIndex( map((x,y)->x*y,       chunkID, chunkSize) )
    return CartesianRange(start, stop)
end

"""
replace Colon of indexes by UnitRange
"""
function colon2unitRange(buf::Union{Array,AbstractBigArray}, indexes::Tuple)
    colon2unitRange(size(buf), indexes)
end

function colon2unitRange{N}(sz::NTuple{N}, indexes::Tuple)
    # @show sz
    map((x,y)-> x==Colon() ? UnitRange(1:y):x, indexes, sz)
end

function Base.CartesianIndex(idx::Union{Tuple,Vector})
    CartesianIndex((idx...))
end


# function Base.string{N}(r::CartesianRange{CartesianIndex{N}})
#     ret = ""
#     for d in 1:N
#         s = r.start.I[d]
#         e = r.stop.I[d]
#         ret *= "$s:$e_"
#     end
#     return ret[1:end-1]
# end

function Base.string( idxes::UnitRange...)
    ret = ""
    ret = map(x->"$(start(x)-1)-$(x[end])_", idxes)
    return ret[1:end-1]
end

function Base.string{N}( r::CartesianRange{CartesianIndex{N}} )
    ret = ""
    for i in 1:N
        ret *= "$(r.start[i]-1)-$(r.stop[i])_"
    end
    return ret[1:end-1]
end

# function Base.CartesianRange( str::String )
#     secs = split(str, "_")
#     N = length(secs)
#     s = CartesianIndex{N}()
#     e = CartesianIndex{N}()
#     for i in 1:N
#         s[i] = parse( split(secs[i+1],":")[1] )
#         e[i] = parse( split(secs[i+1],":")[2] )
#     end
#     return CartesianRange(s,e)
# end

"""
    adjust bounding box range when fitting in new subarray
"""
function Base.union(globalRange::CartesianRange, idxes::CartesianRange)
    start = map(min, globalRange.start, idxes.start)
    stop  = map(max, globalRange.stop,  idxes.stop)
    return CartesianRange(CartesianIndex(start...), CartesianIndex(stop...))
end
function Base.union!(r1::CartesianRange, r2::CartesianRange)
    r1 = union(r1, r2)
end


"""
    transform x1:x2_y1:y2_z1:z2 style string to CartesianRange
"""
function Base.CartesianRange( s::String )
    secs = split(s, "_")
    starts = map( x->parse(split(x,"-")[1])+1, secs )
    stops  = map( x->parse(split(x,"-")[2]), secs )
    CartesianRange( CartesianIndex(starts...), CartesianIndex( stops... ) )
end

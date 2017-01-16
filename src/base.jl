using .BigArrayIterators
using Blosc


export BigArray, get_config_dict

function __init__()
    # use the same number of threads with Julia
    if haskey(ENV, "BLOSC_NUM_THREADS")
        Blosc.set_num_threads( parse(ENV["BLOSC_NUM_THREADS"]) )
    elseif haskey(ENV, "JULIA_NUM_THREADS")
        Blosc.set_num_threads( parse(ENV["JULIA_NUM_THREADS"]) )
    else
        Blosc.set_num_threads(4)
    end
    # use the default compression method
    # Blosc.set_compressor("blosclz")
end

# a function expected to be inherited by backends
# refer the idea of modular design here:
# http://www.juliabloggers.com/modular-algorithms-for-scientific-computing-in-julia/
# a similar function:
# https://github.com/JuliaDiffEq/DiffEqBase.jl/blob/master/src/DiffEqBase.jl#L62
function get_config_dict end

"""
    BigArray
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
immutable BigArray{D<:Associative, T<:Integer, N} <: AbstractBigArray
    kvStore     ::D
    chunkSize   ::NTuple{N}
    function (::Type{BigArray}){N}( kvStore::Associative,
                                    T::DataType, chunkSize::NTuple{N} )
        D = typeof(kvStore)
        @show D
        new{D, T, N}(kvStore, chunkSize)
    end
end


BigArray{D<:Associative,N}(kvStore::D, elementDataType::DataType, chunkSize::NTuple{N}) = BigArray(kvStore, elementDataType, chunkSize)
# BigArray{D<:Associative,N}(kvStore::D, elementDataType::DataType, chunkSize::NTuple{N}) = BigArray{D,elementDataType,N}(kvStore, chunkSize)

function BigArray( d::Associative )
    configDict = get_config_dict( d )
    T = eval(parse(configDict[:dataType]))
    @show T
    chunkSize = ([configDict[:chunkSize]]...)
    N = length(chunkSize)
    BigArray( d, T, chunkSize )
end

function Base.ndims{D,T,N}(ba::BigArray{D,T,N})
    return N
end

function Base.eltype{D, T, N}( ba::BigArray{D,T,N} )
    @show T
    return T
end

function Base.size{D,T,N}( ba::BigArray{D,T,N} )
    # get size according to the keys
    ret = size( CartesianRange(ba) )
    # if all(s->s==0, ret)
    #     ret = map(typemax(Int), ret)
    # end
    # ret = ([typemax(Int) for i=1:N]...)
    return ret
end

function Base.size(ba::BigArray, i::Int)
    size(ba)[i]
end

function Base.show(ba::BigArray)
    display(ba)
end

function Base.display(ba::BigArray)
    for field in fieldnames(ba)
        println("$field: $(ba.(field))")
    end
end

function Base.reshape{D,T,N}(ba::BigArray{D,T,N}, newShape)
    warn("reshape failed, the shape of bigarray is immutable!")
end

function Base.CartesianRange{D,T,N}( ba::BigArray{D,T,N} )
    warn("the size was computed according to the keys, which is a number of chunk sizes and is not accurate")
    keyList = keys(ba.chunkStore)
    ret = CartesianRange(
            CartesianIndex([typemax(Int) for i=1:N]...),
            CartesianIndex([0            for i=1:N]...))
    for key in keyList
        union!(ret, CartesianRange(key))
    end
    ret
end

function Base.string{N}( r::CartesianRange{CartesianIndex{N}} )
    ret = ""
    for i in 1:N
        ret *= "$(r.start[i]):$(r.stop[i])_"
    end
    return ret[1:end-1]
end

"""
    transform x1:x2_y1:y2_z1:z2 style string to CartesianRange
"""
function Base.CartesianRange( s::String )
    error("not implemented")
end


"""
    put array in RAM to a BigArray
"""
function Base.setindex!{T,N}( ba::BigArray{D,T,N}, buf::Array{T,N},
                                idxes::Union{UnitRange, Int, Colon} ... )
    @assert eltype(ba) == T
    @assert ndims(ba) == N
    @show idxes
    idxes = colon2unitRange(buf, idxes)
    baIter = BigArrayIterator(idxes, ba.chunkSize)
    chk = Array(T, ba.chunkSize)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        # chk = ba.chunkStore[chunkGlobalRange]
        # chk = reshape(Blosc.decompress(T, chk), ba.chunkSize)
        fill!(chk, convert(T, 0))
        chk[rangeInChunk] = buf[rangeInBuffer]
        ba.kvStore[ string(chunkGlobalRange) ] = Blosc.compress(chk)
    end
end

function Base.getindex{D,T,N}( ba::BigArray{D, T, N}, idxes::Union{UnitRange, Int}...)
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)
    baIter = BigArrayIterator(idxes, ba.chunkSize)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        v = ba.kvStore[string(chunkGlobalRange)]
        chk = reshape(Blosc.decompress(T, v), ba.chunkSize)
        buf[rangeInBuffer] = chk[rangeInChunk]
    end
    return buf
end

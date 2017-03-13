using .BigArrayIterators
using .Coding

export BigArray, get_config_dict, get_chunk_size

const DEFAULT_CODING = RawCoding

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
immutable BigArray{D<:Associative, T<:Real, N, C<:AbstractBigArrayCoding} <: AbstractBigArray
    kvStore     :: D
    chunkSize   :: NTuple{N}
    function (::Type{BigArray}){D,T,N,C}(
                            kvStore     ::D,
                            foo         ::Type{T},
                            chunkSize   ::NTuple{N},
                            coding      ::Type{C} )
        new{D, T, N, C}(kvStore, chunkSize)
    end
end

function BigArray( d::Associative )
    configDict = get_config_dict( d )
    return BigArray(d, configDict)
end

function BigArray( d::Associative, configDict::Dict{Symbol, Any} )
    T = eval(parse(configDict[:dataType]))
    # @show T
    chunkSize = (configDict[:chunkSize]...)
    if haskey( configDict, :coding )
        if contains( configDict[:coding], "raw" )
            coding = RawCoding
        elseif contains(  configDict[:coding], "jpeg")
            coding = JPEGCoding
        elseif contains( configDict[:coding], "blosclz")
            coding = BlosclzCoding
        else
            error("unknown coding")
        end
    else
        coding = DEFAULT_CODING
    end
    # N = length(chunkSize)
    BigArray( d, T, chunkSize, coding )
end

function Base.ndims{D,T,N}(ba::BigArray{D,T,N})
    N
end

function Base.eltype{D, T, N}( ba::BigArray{D,T,N} )
    # @show T
    return T
end

function Base.size{D,T,N}( ba::BigArray{D,T,N} )
    # get size according to the keys
    ret = size( CartesianRange(ba) )
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
    keyList = keys(ba.kvStore)
    ret = CartesianRange(
            CartesianIndex([typemax(Int) for i=1:N]...),
            CartesianIndex([0            for i=1:N]...))
    for key in keyList
        if !isempty(key)
            union!(ret, CartesianRange(key))
        end
    end
    ret
end

"""
    put array in RAM to a BigArray
"""
function Base.setindex!{D,T,N,C}( ba::BigArray{D,T,N,C}, buf::Array{T,N},
                                idxes::Union{UnitRange, Int, Colon} ... )
    @assert eltype(ba) == T
    @assert ndims(ba) == N
    # @show idxes
    idxes = colon2unitRange(buf, idxes)
    baIter = BigArrayIterator(idxes, ba.chunkSize)
    chk = Array(T, ba.chunkSize)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        # chk = ba.chunkStore[chunkGlobalRange]
        # chk = reshape(Blosc.decompress(T, chk), ba.chunkSize)
        fill!(chk, convert(T, 0))
        chk[rangeInChunk] = buf[rangeInBuffer]
        ba.kvStore[ string(chunkGlobalRange) ] = encoding( chk, C)
    end
end

function Base.getindex{D,T,N,C}( ba::BigArray{D, T, N, C}, idxes::Union{UnitRange, Int}...)
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)
    baIter = BigArrayIterator(idxes, ba.chunkSize)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        v = ba.kvStore[string(chunkGlobalRange)]
        if isa(v, Array)
            #@show C
            chk = decoding(v, C)
            chk = reshape(reinterpret(T, chk), ba.chunkSize)
            buf[rangeInBuffer] = chk[rangeInChunk]
        else
            # otherwise v is an error, which means that it is all zero, do nothing
            println("get all zero chunk")
        end
    end
    return buf
end

function get_chunk_size(ba::AbstractBigArray)
    ba.chunkSize
end

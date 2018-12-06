"""
    BigArray
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
struct BigArray{D<:AbstractBigArrayBackend, T<:Real, N, C<:AbstractBigArrayCoding} <: AbstractBigArray
    kvStore     :: D
    chunkSize   :: NTuple{N}
    volumeSize  :: NTuple{N}
    offset      :: CartesianIndex{N}
    fillMissing :: Bool 
    mode        :: Symbol
    function BigArray(
                    kvStore     ::D,
                    foo         ::Type{T},
                    chunkSize   ::NTuple{N},
                    volumeSize  ::NTuple{N},
                    coding      ::Type{C};
                    offset      ::CartesianIndex{N} = CartesianIndex{N}() - 1,
                    fillMissing ::Bool=true,
                    mode        ::Symbol=:multithreads) where {D,T,N,C}
        new{D, T, N, C}(kvStore, chunkSize, volumeSize, offset, fillMissing, mode)
    end
end

function BigArray( d::AbstractBigArrayBackend)
    info = get_info(d)
    return BigArray(d, info)
end

function BigArray( d::AbstractBigArrayBackend, info::Vector{UInt8})
    Codings.decode(info, GzipCoding) 
    BigArray(d, String(info))
end 

function BigArray( d::AbstractBigArrayBackend, info::AbstractString )
    BigArray(d, JSON.parse( info, dicttype=Dict{Symbol, Any} ))
end 

function BigArray( d::AbstractBigArrayBackend, infoConfig::Dict{Symbol, Any}) 
    # chunkSize
    scale_name = get_scale_name(d)
    T = DATATYPE_MAP[infoConfig[:data_type]]
    local offset::Tuple, encoding, chunkSize::Tuple, volumeSize::Tuple 
    for scale in infoConfig[:scales]
        if scale[:key] == scale_name 
            chunkSize = (scale[:chunk_sizes][1]...,)
            offset = (scale[:voxel_offset]...,)
            volumeSize = (scale[:size]...,)
            encoding = CODING_MAP[ scale[:encoding] ]
            if infoConfig[:num_channels] > 1
                chunkSize = (chunkSize..., infoConfig[:num_channels])
                volumeSize = (volumeSize..., infoConfig[:num_channels])
                offset = (offset..., 0)
            end
            break 
        end 
    end 
    BigArray(d, T, chunkSize, volumeSize, encoding; offset=CartesianIndex(offset)) 
end

######################### base functions #######################

function Base.ndims(ba::BigArray{D,T,N}) where {D,T,N}
    N
end

function Base.eltype( ba::BigArray{D,T,N} ) where {D, T, N}
    return T
end

function Base.size( ba::BigArray{D,T,N} ) where {D,T,N}
    # get size according to the keys
    return ba.volumeSize
end

function Base.size(ba::BigArray, i::Int)
    size(ba)[i]
end

function Base.show(ba::BigArray) show(ba.chunkSize) end

function Base.display(ba::BigArray)
    for field in fieldnames(ba)
        println("$field: $(getfield(ba,field))")
    end
end

function Base.reshape(ba::BigArray{D,T,N}, newShape) where {D,T,N}
    @warn("reshape failed, the shape of bigarray is immutable!")
end

"""
    Base.setindex!( ba::BigArray{D,T,N,C}, buf::Array{T,N},

setindex with different mode: sharedarray, multithreads, multiprocesses, sequential 
"""
function Base.setindex!( ba::BigArray{D,T,N,C}, buf::Array{T,N},
            idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N,C}
    if ba.mode == :multithreads 
        setindex_multithreads!(ba, buf, idxes...)
    elseif ba.mode == :multiprocesses 
        setindex_multiprocesses!(ba, buf, idxes...)
    elseif ba.mode == :sharedarray 
        setindex_sharedarray!(ba, buf, idxes...,)
    elseif ba.mode == :sequential 
        setindex_sequential!(ba, buf, idxes...)
    else 
        error("only support modes of multithreads, multiprocesses, sharedarray, sequential")
    end 
end 

@inline function Base.CartesianIndices(ba::BigArray{D,T,N,C}) where {D,T,N,C}
    start = ba.offset + one(CartesianIndex{N})
    stop = ba.offset + CartesianIndex(ba.volumeSize)
    ranges = map((x,y)->x:y, start.I, stop.I)
    return CartesianIndices( ranges )
end 

"""
adjust the global and buffer range according to total volume size.
shrink the range stop if the ranges passes the volume boundary.
"""
function adjust_volume_boundary(ba::BigArray, chunkGlobalRange::CartesianIndices,
                                globalRange::CartesianIndices,
                                rangeInChunk::CartesianIndices, 
                                rangeInBuffer::CartesianIndices)
    volumeStop = map(+, ba.offset.I, ba.volumeSize)
    chunkGlobalRangeStop = [last(chunkGlobalRange).I ...,]
    globalRangeStop = [last(globalRange).I ...,]
    rangeInBufferStop = [last(rangeInBuffer).I ...,]
    rangeInChunkStop = [last(rangeInChunk).I...,] 

    for (i,s) in enumerate(volumeStop)
        if chunkGlobalRangeStop[i] > s
            chunkGlobalRangeStop[i] = s
        end
        distanceOverBorder = globalRangeStop[i] - s
        if distanceOverBorder > 0
            globalRangeStop[i] -= distanceOverBorder
            @assert globalRangeStop[i] == s
            @assert globalRangeStop[i] > first(globalRange).I[i]
            rangeInBufferStop[i] -= distanceOverBorder
            rangeInChunkStop[i] -= distanceOverBorder
        end
    end
    start = first(chunkGlobalRange).I
    stop =  (chunkGlobalRangeStop...,) 
    chunkGlobalRange = CartesianIndices( map((x,y)->x:y, start, stop) )

    start = first(globalRange).I 
    stop = (globalRangeStop...,) 
    globalRange = CartesianIndices( map((x,y)->x:y, start, stop) )

    start = first(rangeInBuffer).I 
    stop = (rangeInBufferStop...,)
    rangeInBuffer = CartesianIndices( map((x,y)->x:y, start, stop) )

    start = first(rangeInChunk).I 
    stop = (rangeInChunkStop...,) 
    rangeInChunk = CartesianIndices( map((x,y)->x:y, start, stop) )
    return chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer
end 

"""
    Base.getindex( ba::BigArray{D, T, N, C}, idxes::Union{UnitRange, Int}...) where {D,T,N,C}

get index with different modes: sharedarray, multi_processes, multithreads, sequential 
"""
function Base.getindex( ba::BigArray{D, T, N, C}, idxes::Union{UnitRange, Int}...) where {D,T,N,C}
    if ba.mode == :sharedarray 
        getindex_sharedarray(ba, idxes...,)
    elseif ba.mode == :multi_processes 
        getindex_multiprocesses(ba, idxes...)
    elseif ba.mode == :multithreads 
        getindex_multithreads(ba, idxes...)
    elseif ba.mode == :sequential 
        getindex_sequential(ba, idxes...)
    else 
        error("only support mode of (sharedarray, multi_processes, multithreads, sequential)")
    end 
end

function get_chunk_size(ba::AbstractBigArray)
    ba.chunkSize
end

###################### utils ####################
"""
    get_num_chunks(ba::BigArray, idxes::Union{UnitRange,Int}...)
get number of chunks needed to do cutout from this range 
"""
function get_num_chunks(ba::BigArray, idxes::Union{UnitRange, Int}...)
    chunkNum = 0
    baIter = ChunkIterator(idxes, ba.chunkSize; offset=ba.offset)                          
	for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        chunkNum += 1
	end                                                                                
    chunkNum
end 

"""
    list_missing_chunks(ba::BigArray, idxes::Union{UnitRange, Int}...)
list the non-existing keys in the index range
if the returned list is empty, then all the chunks exist in the storage backend.
"""
function list_missing_chunks(ba::BigArray, idxes::Union{UnitRange, Int}...) 
    t1 = time()
    sz = map(length, idxes)
    missingChunkList = Vector{CartesianIndices}()
    baIter = ChunkIterator(idxes, ba.chunkSize; offset=ba.offset)
    @sync begin 
        for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
            @async begin 
                if !haskey(ba.kvStore, cartesian_range2string(chunkGlobalRange))
                    push!(missingChunkList, chunkGlobalRange)
                end
            end
        end
    end 
    missingChunkList 
end

function list_missing_chunks(ba::BigArray, keySet::Set{String}, 
                             idxes::Union{UnitRange, Int}...)
    t1 = time()
    sz = map(length, idxes)
    missingChunkList = Vector{CartesianIndices}()
    baIter = ChunkIterator(idxes, ba.chunkSize; offset=ba.offset)
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        if !(cartesian_range2string(chunkGlobalRange) in keySet)
            push!(missingChunkList, chunkGlobalRange)
        end 
    end
    missingChunkList
end 



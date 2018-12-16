
const DEFAULT_MODE = :multithreads 

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
                    mode        ::Symbol=DEFAULT_MODE) where {D,T,N,C}
        new{D, T, N, C}(kvStore, chunkSize, volumeSize, offset, fillMissing, mode)
    end
end

"""
    BigArray(layerPath::AbstractString; fillMissing=true, mod::Symbol=DEFAULT_MODE)
"""
function BigArray(layerPath::AbstractString; fillMissing=true, mode::Symbol=DEFAULT_MODE)
    if isdir(layerPath) || startswith(layerPath, "file://")
        d = BinDict(layerPath)
    elseif startswith(layerPath, "gs://")
        d = GSDict(layerPath)
    elseif startswith(layerPath, "s3://")
        d = S3Dict(layerPath)
    else
        @error "only support protocols of {file, gs, s3}, but got: " layerPath 
    end 
    BigArray(d; fillMissing=fillMissing, mode=mode)
end 

@inline function BigArray(d::AbstractBigArrayBackend; fillMissing::Bool=true, mode::Symbol=DEFAULT_MODE)  
    info = get_info(d) |> Info 
    return BigArray(d, info; fillMissing=fillMissing, mode=mode)
end

@inline function BigArray( d::AbstractBigArrayBackend, info::Vector{UInt8};
                  fillMissing::Bool=true,
                  mode::Symbol=DEFAULT_MODE) 
    info = Codings.decode(info, GzipCoding) |> Info 
    BigArray(d, String(info); fillMissing=fillMissing, mode=mode)
end 

@inline function BigArray( d::AbstractBigArrayBackend, info::AbstractString;
                fillMissing::Bool=true,
                mode::Symbol=DEFAULT_MODE)  
    BigArray(d, Info(info); fillMissing=fillMissing, mode=mode)
end 

@inline function BigArray( d::AbstractBigArrayBackend, infoConfig::Dict{Symbol, Any};
                    fillMissing::Bool=fillMissing, mode::Symbol=DEFAULT_MODE) 
    info = Info(infoConfig)
    BigArray(d, info; mip=mip, fillMissing=fillMissing, mode=mode) 
end

"""
    BigArray( d::AbstractBigArrayBackend, info::Info;
                  mip::Int = 0,
                  fillMissing::Bool=fillMissing,
                  mode::Symbol=DEFAULT_MODE)

Parameters:
    d: the bigarray storage backend 
    info: the info containing the metadata
    mip: mip level. 0 is the highest resolution. 
            the mip level is like a image pyramid with difference downsampled levels.
    fillMissing: whether fill the missing blocks in the storage backend with zeros or not. 
    mode: the io mode with options in {multithreading, sequential, multiprocesses, sharedarray}
"""
@inline function BigArray( d::AbstractBigArrayBackend, info::Info;
                  fillMissing::Bool=fillMissing,
                  mode::Symbol=DEFAULT_MODE)
    dataType = Infos.get_data_type(info) 
    key = get_scale_name(d) |> Symbol
    chunkSize, encoding, resolution, voxelOffset, volumeSize = 
                                Infos.get_properties_in_mip_level(info, key)
    @debug chunkSize, encoding, resolution, voxelOffset, volumeSize
    BigArray(d, dataType, chunkSize, volumeSize, encoding; 
             offset=voxelOffset, fillMissing=fillMissing, mode=mode) 
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

@inline function get_chunk_size(ba::BigArray) ba.chunkSize end
@inline function set_chunk_size(ba::BigArray, chunkSize::NTuple{3,Int})
    ba.chunkSize = chunkSize 
end 

@inline function get_mode(self::BigArray) self.mode end 
@inline function set_mode(self::BigArray{D,T,N,C}, mode::Symbol) where {D,T,N,C}
    ba.mode = mode 
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



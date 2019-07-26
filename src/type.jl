
const DEFAULT_MODE = :multithreads 
const DEFAULT_FILL_MISSING = true 

"""
    BigArray
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
struct BigArray{D<:AbstractBigArrayBackend, T<:Real, N} <: AbstractBigArray
    kvStore     :: D
    info        :: Info{T,N}
    mip         :: Integer
    fillMissing :: Bool 
    mode        :: Symbol
end

"""
    BigArray(layerPath::AbstractString; mip=1, fillMissing=true, mod::Symbol=DEFAULT_MODE)
"""
function BigArray(layerPath::AbstractString; mip::Integer=1, fillMissing::Bool=DEFAULT_FILL_MISSING, 
                  mode::Symbol=DEFAULT_MODE)
    if isdir(layerPath) || startswith(layerPath, "file://")
        layerPath = replace(layerPath, "file://"=>"/", count=1)
        d = BinDict(layerPath)
    elseif startswith(layerPath, "gs://")
        d = GSDict(layerPath)
    elseif startswith(layerPath, "s3://")
        d = S3Dict(layerPath)
    else
        @error "only support protocols of {file, gs, s3}, but got: " layerPath 
    end 
    BigArray(d; mip=mip, fillMissing=fillMissing, mode=mode)
end 

@inline function BigArray(d::AbstractBigArrayBackend; mip::Integer=1, 
                          fillMissing::Bool=DEFAULT_FILL_MISSING, 
                          mode::Symbol=DEFAULT_MODE)  
    info = d["info"] |> Info
    return BigArray(d, info, mip, fillMissing, mode)
end

@inline function BigArray( d::AbstractBigArrayBackend, info::Vector{UInt8};
                  mip::Integer=1, fillMissing::Bool=DEFAULT_FILL_MISSING,
                  mode::Symbol=DEFAULT_MODE) 
    info = Codings.decode(info, GzipCoding) |> Info 
    BigArray(d, string(info), mip, fillMissing, mode)
end 

@inline function BigArray( d::AbstractBigArrayBackend, info::AbstractString;
                            mip::Integer=1, fillMissing::Bool=DEFAULT_FILL_MISSING,
                            mode::Symbol=DEFAULT_MODE)  
    BigArray(d, Info(info), mip, fillMissing, mode)
end 

@inline function BigArray( d::AbstractBigArrayBackend, infoConfig::Dict{Symbol, Any};
                            mip::Integer=1, fillMissing::Bool=DEFAULT_FILL_MISSING, 
                            mode::Symbol=DEFAULT_MODE) 
    BigArray(d, Info(infoConfig)info, mip, fillMissing, mode) 
end

"""
    BigArray( d::AbstractBigArrayBackend, info::Info;
                  fillMissing::Bool=fillMissing,
                  mode::Symbol=DEFAULT_MODE)

Parameters:
    d: the bigarray storage backend 
    info: the info containing the metadata
    mip: mip level. Note that mip 1 is the highest resolution. 
            the mip level is like a image pyramid with difference downsampled levels.
    fillMissing: whether fill the missing blocks in the storage backend with zeros or not. 
    mode: the io mode with options in {multithreading, sequential, multiprocesses, sharedarray}
"""
function BigArray( d::AbstractBigArrayBackend, info::Info{T,N};
                  mip::Integer=1, fillMissing::Bool=DEFAULT_FILL_MISSING,
                  mode::Symbol=DEFAULT_MODE) where {T,N} 
    BigArray(d, info, mip, fillMissing, mode) 
end

"""
    BigArray(info::Info; mip::Integer=1, fillMissing::Bool=DEFAULT_FILL_MISSING, mode=DEFAULT_MODE)
    
create a new directory with random name. 
this function was designed for test and benchmark.
we need another function the clear the whole array 
"""
function BigArray(info::Info{T,N}; mip::Integer=1,
                  fillMissing::Bool=DEFAULT_FILL_MISSING, 
                  mode=DEFAULT_MODE) where {T,N}
    # prepare directory
    layerDir = tempname()
    datasetDir = joinpath(layerDir, string(Infos.get_key(info, 1))) 
    mkdir(layerDir)
    mkdir(datasetDir)
    d = BinDict(layerDir)
    
    # write the info as file 
    write(joinpath(layerDir, "info"), JSON.json(Dict(info)))

    return BigArray(d, info, mip, fillMissing, mode)
end

######################### base functions #######################

function Base.ndims(ba::BigArray{D,T,N}) where {D,T,N}
    N
end

function Base.eltype( ba::BigArray{D,T,N} ) where {D, T, N}
    return T
end

function Base.size( ba::BigArray ) 
    # get size according to the size in info file 
    return get_volume_size(ba)
end

function Base.size(ba::BigArray, i::Int)
    size(ba)[i]
end

@inline function Base.show(io::IO, ba::BigArray)
    #show(io, ba.kvStore)
    show(io, get_chunk_size(ba)) 
end

function Base.display(ba::BigArray)
    for field in fieldnames(ba)
        println("$field: $(getfield(ba,field))")
    end
end

"""
    Base.setindex!( ba::BigArray{D,T,N}, buf::Array{T,N},

setindex with different mode: taskthreads, multithreads, sequential 
"""
@inline function Base.setindex!( ba::BigArray{D,T,N}, buf::Array{T,N},
            idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N}
    if ba.mode == :taskthreads 
        setindex_fun! = setindex_taskthreads!
    elseif ba.mode == :multithreads
        setindex_fun! = setindex_multithreads!
    elseif ba.mode == :sequential 
        setindex_fun! = setindex_sequential!
    else 
        error("only support modes of multithreads, multiprocesses, sharedarray, sequential")
    end
    setindex_fun!(ba, buf, idxes...) 
end 

@inline function Base.CartesianIndices(ba::BigArray{D,T,N}) where {D,T,N}
    offset = get_offset(ba)
    start = offset + one(CartesianIndex{N})
    stop = offset + CartesianIndex(get_volume_size(ba))
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
    offset = get_offset(ba)
    volumeStop = map(+, offset.I, get_volume_size(ba))
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
    Base.getindex( ba::BigArray, idxes::Union{UnitRange, Int}...) 

get index with different modes: taskthreads, multithreads, sequential 
"""
@inline function Base.getindex( ba::BigArray, idxes::Union{UnitRange, Int}...) 
    if ba.mode == :taskthreads 
        getindex_fun = getindex_taskthreads
    elseif ba.mode == :multithreads
        getindex_fun = getindex_multithreads
    elseif ba.mode == :sequential 
        getindex_fun = getindex_sequential
    else 
        error("only support mode of (sharedarray, multi_processes, multithreads, sequential)")
    end
    getindex_fun(ba, idxes...) 
end

@inline function get_key_value_store(ba::BigArray) ba.kvStore end 
@inline function get_info(ba::BigArray) ba.info end
@inline function get_mip(ba::BigArray) ba.mip end 
@inline function get_encoding(ba::BigArray)
    info = get_info(ba)
    mip = get_mip(ba)
    Infos.get_encoding(info, mip)
end

@inline function get_num_channels(ba::BigArray)
    Infos.get_num_channels(get_info(ba))
end 

@inline function get_mip_level_name(ba::BigArray)
    Infos.get_key(get_info(ba), get_mip(ba)) |> string 
end 

@inline function get_chunk_size(ba::BigArray{D,T,N}) where {D,T,N} 
    chunkSize = Infos.get_chunk_size(get_info(ba), get_mip(ba))
    if N == 3
        return chunkSize 
    else 
        return (chunkSize..., get_num_channels(ba))
    end 
end

@inline function set_chunk_size(ba::BigArray, chunkSize::Tuple{Int})
    info = get_info(ba)
    # if there are multiple channels, ignore the channel number
    Infos.set_chunk_size(info, chunkSize[1:3])
end 

@inline function get_offset(ba::BigArray{D,T,N}) where {D,T,N}
    offset = Infos.get_offset(get_info(ba), get_mip(ba))
    if N == 3
        return offset 
    else 
        return CartesianIndex(offset.I..., 0)
    end 
end

@inline function get_volume_size(ba::BigArray{D,T,N}) where {D,T,N}
    volumeSize = Infos.get_volume_size(get_info(ba), get_mip(ba))
    if N == 3
        return volumeSize 
    else 
        return (volumeSize..., get_num_channels(ba))
    end 
end 

@inline function get_mode(ba::BigArray) ba.mode end 
@inline function set_mode(ba::BigArray, mode::Symbol) 
    ba.mode = mode 
end 

###################### utils ####################
"""
    get_num_chunks(ba::BigArray, idxes::Union{UnitRange,Int}...)
get number of chunks needed to do cutout from this range 
"""
function get_num_chunks(ba::BigArray, idxes::Union{UnitRange, Int}...)
    chunkNum = 0
    baIter = ChunkIterator(idxes, get_chunk_size(ba); offset=get_offset(ba))                          
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
    baIter = ChunkIterator(idxes, get_chunk_size(ba); offset=get_offset(ba))
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
    baIter = ChunkIterator(idxes, get_chunk_size(ba); offset=get_offset(ba))
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        if !(cartesian_range2string(chunkGlobalRange) in keySet)
            push!(missingChunkList, chunkGlobalRange)
        end 
    end
    missingChunkList
end 

"""
    commit_info(ba::BigArray)

write info to the storage backend 
"""
function commit_info(ba::BigArray)
    info = get_info(ba)
    d = get_key_value_store(ba)
    d["info"] = string(info)
end 

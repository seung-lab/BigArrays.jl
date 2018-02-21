module MipLevels

# basic functions
include("BackendBase.jl"); using .BackendBase
include("Codings.jl"); using .Codings;
include("Indexes.jl"); using .Indexes;
include("Iterators.jl"); using .Iterators;
include("backends/include.jl") 

using OffsetArrays 
using JSON
using TranscodingStreams, CodecZlib 

export MipLevel  

function __init__()
    #global const WORKER_POOL = WorkerPool( workers() )
    #@show WORKER_POOL 
    global const GZIP_MAGIC_NUMBER = UInt8[0x1f, 0x8b, 0x08]  
    global const TASK_NUM = 16
    global const CHUNK_CHANNEL_SIZE = 2
    global const CODING_MAP = Dict{String,Any}(
        # note that the raw encoding in cloud storage will be automatically gzip encoded!
        "raw"       => GzipCoding,
        "jpeg"      => JPEGCoding,
        "blosclz"   => BlosclzCoding,
        "gzip"      => GzipCoding, 
        "zstd"      => ZstdCoding )
end 


"""
   MipLevel 
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
struct MipLevel{D<:AbstractMipLevelBackend, T<:Real, N<:Integer, 
                                            C<:AbstractMipLevelCoding} <: AbstractMipLevel
    mipLevelIndex   :: Int
    chunkSize       :: NTuple{N}
    offset          :: CartesianIndex{N}
    function MipLevel(
                kvStore     ::D,
                foo         ::Type{T},
                chunkSize   ::NTuple{N},
                coding      ::Type{C};
                mipLevel    ::Int=0,
                offset      ::CartesianIndex{N}=CartesianIndex{N}()-1) where {D,T,N,C}
        new{D, T, N, C}(kvStore, chunkSize, offset)
    end
end

function MipLevel( d::AbstractMipLevelBackend )
    info = get_info(d)
    return MipLevel(d, info)
end

function MipLevel( d::AbstractMipLevelBackend, info::Vector{UInt8} )
    if all(info[1:3] .== GZIP_MAGIC_NUMBER)
        info = transcode(GzipDecompressor, info)
    end 
    MipLevel(d, String(info))
end 

function MipLevel( d::AbstractMipLevelBackend, info::AbstractString )
    MipLevel(d, NeuroglancerInfo(info) )
end 

function MipLevel( d::AbstractMipLevelBackend, infoConfig::Dict{Symbol, Any} )
    NeuroglancerInfo = NeuroglancerInfo(infoConfig)
    # chunkSize
    scale_name = get_scale_name(d)
    T = DATATYPE_MAP[infoConfig[:data_type]]
    local offset::Tuple, encoding, chunkSize::Tuple 
    for scale in infoConfig[:scales]
        if scale[:key] == scale_name 
            chunkSize = (scale[:chunk_sizes][1]...)
            offset = (scale[:voxel_offset]...)
            encoding = CODING_MAP[ scale[:encoding] ]
            if infoConfig[:num_channels] > 1
                chunkSize = (chunkSize..., infoConfig[:num_channels])
                offset = (offset..., 0)
            end
            break 
        end 
    end 
    MipLevel(d, T, chunkSize, encoding; offset=CartesianIndex(offset)) 
end

######################### base functions #######################

function Base.ndims(self::MipLevel{D,T,N}) where {D,T,N}
    N
end

function Base.eltype( self::MipLevel{D,T,N} ) where {D, T, N}
    return T
end

function Base.size( self::MipLevel{D,T,N} ) where {D,T,N}
    # get size according to the keys
    ret = size( CartesianRange(self) )
    return ret
end

function Base.size(self::MipLevel, i::Int)
    size(self)[i]
end

function Base.show(self::MipLevel) show(self.chunkSize) end

function Base.display(self::MipLevel)
    for field in fieldnames(self)
        println("$field: $(getfield(self,field))")
    end
end

function Base.reshape(self::MipLevel{D,T,N}, newShape) where {D,T,N}
    warn("reshape failed, the shape of bigarray is immutable!")
end

function Base.CartesianRange( self::MipLevel{D,T,N} ) where {D,T,N}
    warn("the size was computed according to the keys, which is a number of chunk sizes and is not accurate")
    ret = CartesianRange(
            CartesianIndex([typemax(Int) for i=1:N]...),
            CartesianIndex([0            for i=1:N]...))
    warn("boundingbox function abanduned due to the malfunction of keys in S3Dicts")
    return ret
end

function do_work_setindex( self::MipLevel{D,T,N,C}, channel::Channel{Tuple}, 
                                                        buf::Array{T,N}) where {D,T,N,C}
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in channel
        # println("global range of chunk: $(cartesian_range2string(chunkGlobalRange))")
		# only accept aligned writting
        delay = 0.05
        for t in 1:4
            try
                chk = buf[cartesian_range2unit_range(rangeInBuffer)...]
                key = cartesian_range2string( chunkGlobalRange )
                self.kvStore[ key ] = encode( chk, C)
                @assert haskey(self.kvStore, key)
                break
            catch e
                println("catch an error while saving in MipLevel: $e")
                @show typeof(e)
                @show stacktrace()
                if t==4
                    println("rethrow the error: $e")
                    rethrow()
                end 
                sleep(delay*(0.8+(0.4*rand())))
                delay *= 10
                println("retry for the $(t)'s time: $(string(chunkGlobalRange))")
            end
        end
    end 
end 

"""
    put array in RAM to a MipLevel
this version uses channel to control the number of asynchronized request
"""
function Base.setindex!( self::MipLevel{D,T,N,C}, buf::Array{T,N},
                       idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N,C}
    idxes = colon2unit_range(buf, idxes)
    @show idxes
    # check alignment
    @assert all(map((x,y,z)->mod(x.start - 1 - y, z), idxes, self.offset.I, self.chunkSize).==0) "the start of index should align with MipLevel chunk size" 
    @assert all(map((x,y,z)->mod(x.stop-y, z), idxes, self.offset.I, self.chunkSize).==0) "the stop of index should align with MipLevel chunk size"
    taskNum = TASK_NUM 
    t1 = time() 
    baIter = Iterator(idxes, self.chunkSize; offset=self.offset)
    @sync begin 
        channel = Channel{Tuple}( CHUNK_CHANNEL_SIZE )
        @async begin 
            for iter in baIter
                put!(channel, iter)
            end
            close(channel)
        end
        for i in 1:taskNum 
            @async do_work_setindex(self, channel, buf)
        end
    end 
    totalSize = length(buf) * sizeof(eltype(buf)) / 1024/1024 # MB
    elapsed = time() - t1 # sec
    println("saving speed: $(totalSize/elapsed) MB/s")
end 

function Base.merge(self::MipLevel{D,T,N,C}, arr::OffsetArray{T,N, Array{T,N}}) where {D,T,N,C}
    @unsafe self[indices(arr)...] = arr |> parent
end 

function do_work_getindex!(self::MipLevel{D,T,N,C}, chan::Channel{Tuple}, 
                                                            buf::Array{T,N}) where {D,T,N,C}
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in chan 
        # explicit error handling to deal with EOFError
        delay = 0.05
        for t in 1:3
            try 
                #println("global range of chunk: $(cartesian_range2string(chunkGlobalRange))") 
                v = self.kvStore[cartesian_range2string(chunkGlobalRange)]
                chk = Codings.decode(v, C)
                chk = reshape(reinterpret(T, chk), self.chunkSize)
                @inbounds buf[cartesian_range2unit_range(rangeInBuffer)...] = 
                                        chk[cartesian_range2unit_range(rangeInChunk)...]
                break 
            catch err 
                if isa(err, KeyError)
                    println("no suck key in kvstore: $(err), will fill this block as zeros")
                    break
                else
                    println("catch an error while getindex in MipLevel: $err with type of $(typeof(err))")
                    if t==3
                        rethrow()
                    end
                    sleep(delay*(0.8+(0.4*rand())))
                    delay *= 10
                end
            end 
        end
    end
end

function Base.getindex( self::MipLevel{D, T, N, C}, idxes::Union{UnitRange, Int}...) where {D,T,N,C}
    taskNum = TASK_NUM
    t1 = time()
    sz = map(length, idxes)
    buf = zeros(eltype(self), sz)
    baIter = Iterator(idxes, self.chunkSize; offset=self.offset)
    @sync begin
        channel = Channel{Tuple}( CHUNK_CHANNEL_SIZE )
        @async begin
            for iter in baIter
                put!(channel, iter)
            end
            close(channel)
        end
        # control the number of concurrent requests here
        for i in 1:taskNum 
            @async do_work_getindex!(self, channel, buf)
        end
    end
    totalSize = length(buf) * sizeof(eltype(buf)) / 1024/1024 # mega bytes
    elapsed = time() - t1 # seconds 
    println("cutout speed: $(totalSize/elapsed) MB/s")
    OffsetArray(buf, idxes...)
end

function get_chunk_size(self::AbstractMipLevel)
    self.chunkSize
end

###################### utils ####################
"""
    get_num_chunks(self::MipLevel, idxes::Union{UnitRange,Int}...)
get number of chunks needed to do cutout from this range 
"""
function get_num_chunks(self::MipLevel, idxes::Union{UnitRange, Int}...)
    chunkNum = 0
    baIter = Iterator(idxes, self.chunkSize; offset=self.offset)                          
	for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        chunkNum += 1
	end                                                                                
    chunkNum
end 

"""
    list_missing_chunks(self::MipLevel, idxes::Union{UnitRange, Int}...)
list the non-existing keys in the index range
if the returned list is empty, then all the chunks exist in the storage backend.
"""
function list_missing_chunks(self::MipLevel, idxes::Union{UnitRange, Int}...) 
    t1 = time()
    sz = map(length, idxes)
    missingChunkList = Vector{CartesianRange}()
    baIter = Iterator(idxes, self.chunkSize; offset=self.offset)
    @sync begin 
        for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
            @async begin 
                if !haskey(self.kvStore, cartesian_range2string(chunkGlobalRange))
                    push!(missingChunkList, chunkGlobalRange)
                end
            end
        end
    end 
    missingChunkList 
end

function list_missing_chunks(self::MipLevel, keySet::Set{String}, idxes::Union{UnitRange, Int}...)
    t1 = time()
    sz = map(length, idxes)
    missingChunkList = Vector{CartesianRange}()
    baIter = Iterator(idxes, self.chunkSize; offset=self.offset)
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        if !(cartesian_range2string(chunkGlobalRange) in keySet)
            push!(missingChunkList, chunkGlobalRange)
        end 
    end
    missingChunkList
end 

end # module

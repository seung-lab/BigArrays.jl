__precompile__()

module BigArrays

abstract type AbstractBigArray <: AbstractArray{Any,Any} end

# basic functions
include("BackendBase.jl"); using .BackendBase
include("Codings.jl"); 
using .Codings;
include("Chunks.jl"); using .Chunks;
include("Indexes.jl"); using .Indexes;
include("Iterators.jl"); using .Iterators;
include("backends/include.jl") 

using OffsetArrays 
using JSON
#import .BackendBase: AbstractBigArrayBackend  
# Note that DenseArray only works for memory stored Array
# http://docs.julialang.org/en/release-0.4/manual/arrays/#implementation
export AbstractBigArray, BigArray 

function __init__()
    global const WORKER_POOL = WorkerPool( workers() )
    @show WORKER_POOL 
end 

const TASK_NUM = 20
# map datatype of python to Julia 
const DATATYPE_MAP = Dict{String, DataType}( 
    "uint8"     => UInt8, 
    "uint16"    => UInt16, 
    "uint32"    => UInt32, 
    "uint64"    => UInt64, 
    "float32"   => Float32, 
    "float64"   => Float64 
)  

const CODING_MAP = Dict{String,Any}(
    # note that the raw encoding in cloud storage will be automatically encoded using gzip!
    "raw"       => GZipCoding,
    "jpeg"      => JPEGCoding,
    "blosclz"   => BlosclzCoding,
    "gzip"      => GZipCoding 
)


function __init__()
#    addprocs(Sys.CPU_CORES - nworkers())
end 

"""
    BigArray
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
@everywhere struct BigArray{D<:AbstractBigArrayBackend, T<:Real, 
                            N<:Integer, C<:AbstractBigArrayCoding} <: AbstractBigArray
    kvStore     :: D
    chunkSize   :: NTuple{N}
    offset      :: CartesianIndex{N}
    function BigArray(
                    kvStore     ::D,
                    foo         ::Type{T},
                    chunkSize   ::NTuple{N},
                    coding      ::Type{C};
                    offset      ::CartesianIndex{N} = CartesianIndex{N}() - 1) where {D,T,N,C}
        # force the offset to be 0s to shutdown the functionality of offset for now
        # because it corrupted all the other bigarrays in aws s3
        new{D, T, N, C}(kvStore, chunkSize, offset)
    end
end

function BigArray( d::AbstractBigArrayBackend)
    info = get_info(d)
    return BigArray(d, info)
end

function BigArray( d::AbstractBigArrayBackend, info::Vector{UInt8})
    if ismatch(r"^{", String(info) )
        info = String(info)
    else
        # gzip compressed
        info = String(Libz.decompress(info))
    end 
   BigArray(d, info)
end 

function BigArray( d::AbstractBigArrayBackend, info::AbstractString )
    BigArray(d, JSON.parse( info, dicttype=Dict{Symbol, Any} ))
end 

function BigArray( d::AbstractBigArrayBackend, infoConfig::Dict{Symbol, Any}) 
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
    BigArray(d, T, chunkSize, encoding; offset=CartesianIndex(offset)) 
end

######################### base functions #######################

function Base.ndims(ba::BigArray{D,T,N}) where {D,T,N} N end

function Base.eltype( ba::BigArray{D,T,N} ) where {D, T, N} T end

function Base.size( ba::BigArray{D,T,N} ) where {D,T,N}
    # get size according to the keys
    ret = size( CartesianRange(ba) )
    return ret
end

function Base.size(ba::BigArray, i::Int)  size(ba)[i] end

function Base.show(ba::BigArray) show(ba.chunkSize) end

function Base.display(ba::BigArray)
    for field in fieldnames(ba)
        println("$field: $(getfield(ba,field))")
    end
end

function Base.reshape(ba::BigArray{D,T,N}, newShape) where {D,T,N}
    warn("reshape failed, the shape of bigarray is immutable!")
end

function Base.CartesianRange( ba::BigArray{D,T,N} ) where {D,T,N}
    warn("the size was computed according to the keys, which is a number of chunk sizes and is not accurate")
    ret = CartesianRange(
            CartesianIndex([typemax(Int) for i=1:N]...),
            CartesianIndex([0            for i=1:N]...))
    warn("boundingbox function abanduned due to the malfunction of keys in S3Dicts")
    return ret
end

function do_work_setindex( block::Array{T,N}, ba::BigArray{D,T,N,C}, chunkGlobalRange ) 
                                                                            where {D,T,N,C}
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in channel
        # println("global range of chunk: $(cartesian_range2string(chunkGlobalRange))")
		# only accept aligned writting
        delay = 0.05
        for t in 1:4
            try
                ba.kvStore[ cartesian_range2string(chunkGlobalRange) ] = encode(block, C)
                break
            catch e
                println("catch an error while saving in BigArray: $e")
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
    put array in RAM to a BigArray
this version uses channel to control the number of asynchronized request
"""
function setindex_V2!( ba::BigArray{D,T,N,C}, buf::Array{T,N},
                       idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N,C}
    idxes = colon2unit_range(buf, idxes)
    

    # check alignment
    @assert all(map((x,y,z)->mod(x.start - 1 - y, z), idxes, ba.offset.I, ba.chunkSize).==0) "the start of index should align with BigArray chunk size" 
    @assert all(map((x,y,z)->mod(x.stop-y, z), idxes, ba.offset.I, ba.chunkSize).==0) "the stop of index should align with BigArray chunk size"

    taskNum = get_task_num(ba)
    t1 = time() 
    baIter = Iterator(idxes, ba.chunkSize; offset=ba.offset)
    workerPool = WorkerPool(workers())       
    const jobs = RemoteChannel(()->Channel{Tuple}( nworkers() ))         
    const resutls = RemoteChannel(()->Channel{OffsetArray}( nworkers() ))

    @sync begiin
        for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
            block = buf[cartesian_range2unit_range(rangeInBuffer)...]
            @async remote_do(do_work_setindex, workerPool, block, chunkGlobalRange)
        end 
    end 
    totalSize = length(buf) * sizeof(eltype(buf)) / 1024/1024 # MB
    elapsed = time() - t1 # sec
    println("saving speed: $(totalSize/elapsed) MB/s")
end 

function setindex_remote_worker(block::Array{T,N}, ba::BigArray{D,T,N,C}, 
                                        chunkGlobalRange::CartesianRange) where {D,T,N,C}
    delay = 0.05
	for t in 1:4
		try
			ba.kvStore[ cartesian_range2string(chunkGlobalRange) ] = encode( block, C)
			break
		catch e
			println("catch an error while saving in BigArray: $e")
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

"""
    put array in RAM to a BigArray
this version uses channel to control the number of asynchronized request
"""
function Base.setindex!( ba::BigArray{D,T,N,C}, buf::Array{T,N},
                       idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N,C}
    idxes = colon2unit_range(buf, idxes)
    # check alignment
    @assert all(map((x,y,z)->mod(x.start - 1 - y, z), idxes, ba.offset.I, ba.chunkSize).==0) "the start of index should align with BigArray chunk size" 
    @assert all(map((x,y,z)->mod(x.stop-y, z), idxes, ba.offset.I, ba.chunkSize).==0) "the stop of index should align with BigArray chunk size"

    t1 = time() 
    baIter = Iterator(idxes, ba.chunkSize; offset=ba.offset)
    @sync begin  
        for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
            block = buf[cartesian_range2unit_range(rangeInBuffer)...]
            @async remotecall_fetch(setindex_remote_worker, WORKER_POOL, block, ba, 
                                                            chunkGlobalRange)
        end 
    end 
    totalSize = length(buf) * sizeof(eltype(buf)) / 1024/1024 # MB
    elapsed = time() - t1 # sec
    println("saving speed: $(totalSize/elapsed) MB/s")
end 


"""
sequential function, good for debuging
"""
# function Base.setindex!{D,T,N,C}( ba::BigArray{D,T,N,C}, buf::Array{T,N},
function setindex_V1!( ba::BigArray{D,T,N,C}, buf::Array{T,N},
                       idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N,C}
    @assert eltype(ba) == T
    @assert ndims(ba) == N
    idxes = colon2unit_range(buf, idxes)
    baIter = Iterator(idxes, ba.chunkSize; offset=ba.offset)
    chk = Array(T, ba.chunkSize)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        fill!(chk, convert(T, 0))
        @inbounds chk[cartesian_range2unit_range(rangeInChunk)...] = 
                                        buf[cartesian_range2unit_range(rangeInBuffer)...]
        ba.kvStore[ cartesian_range2string(chunkGlobalRange) ] = encode( chk, C)
    end
end 
"""
sequential version for debug
"""
function Base.getindex_V2( ba::BigArray{D, T, N, C}, idxes::Union{UnitRange, Int}...) where {D,T,N,C}
    t1 = time()
    sz = map(length, idxes)
    ret = OffsetArray(zeros(eltype(ba), sz), idxes...)
    baIter = Iterator(idxes, ba.chunkSize; offset=ba.offset)

    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter 
        #chk = zeros(T, ba.chunkSize)
        # explicit error handling to deal with EOFError
        println("global range of chunk: $(cartesian_range2string(chunkGlobalRange))") 
        v = ba.kvStore[cartesian_range2string(chunkGlobalRange)]
        chk = Codings.decode(v, C)
        chk = reshape(reinterpret(T, chk), ba.chunkSize)
        buf[cartesian_range2unit_range(rangeInBuffer)...] = 
            chk[cartesian_range2unit_range(rangeInChunk)...]
    end
end

@everywhere function do_work_getindex( ba::BigArray{D,T,N,C}, jobs::RemoteChannel, 
                                                results::RemoteChannel) where {D,T,N,C}
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in jobs 
        # explicit error handling to deal with EOFError
        delay = 0.05
        for t in 1:3
            try 
                data = ba.kvStore[cartesian_range2string(chunkGlobalRange)]
                data = Codings.decode(data, C)
                data = reshape(reinterpret(T, data), ba.chunkSize)
                data = data[cartesian_range2unit_range(rangeInChunk)...]
                block = OffsetArray(data, cartesian_range2unit_range(globalRange))
                put!(results, block)
                break  
            catch err 
                if isa(err, KeyError)
                    println("no suck key in kvstore: $(err), will fill this block as zeros")
                    break
                else
                    println("catch an error while getindex in BigArray: $err with type of $(typeof(err))")
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

"""
sequential version for debug
"""
function do_work_getindex_V1!(chan::Channel{Tuple}, buf::Array{T,N}, ba::BigArray{D,T,N,C}) where {D,T,N,C}
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in chan 
        #chk = zeros(T, ba.chunkSize)
        # explicit error handling to deal with EOFError
        println("global range of chunk: $(cartesian_range2string(chunkGlobalRange))") 
        v = ba.kvStore[cartesian_range2string(chunkGlobalRange)]
        chk = Codings.decode(v, C)
        chk = reshape(reinterpret(T, chk), ba.chunkSize)
        buf[cartesian_range2unit_range(rangeInBuffer)...] = 
            chk[cartesian_range2unit_range(rangeInChunk)...]
    end
end

function getindex_V2( ba::BigArray{D, T, N, C}, idxes::Union{UnitRange, Int}...) where {D,T,N,C}
    taskNum = get_task_num(ba)
    t1 = time()
    sz = map(length, idxes)
    ret = OffsetArray(zeros(eltype(ba), sz), idxes...)
    baIter = Iterator(idxes, ba.chunkSize; offset=ba.offset)
    workerPool = WorkerPool(workers())
    const jobs = RemoteChannel(()->Channel{Tuple}( nworkers() ))
    const resutls = RemoteChannel(()->Channel{OffsetArray}( nworkers() ))

    @sync begin
            @async begin
            for iter in baIter
                put!(jobs, iter)
            end
            close(jobs)
        end
        # start remote workers
        for pid in workers()
            remote_do(do_work_getindex, pid, ba, jobs, results) 
        end
        
        # collecting results
        for block in resutls 
            offsets = block.offsets 
            blockSize = size(block.parent)
            range = map((o,s)->o+1:o+s, offsets, blockSize)
            ret[range...] = block.parent
        end 
    end
    totalSize = prod(sz) * sizeof(eltype(ba)) / 1024/1024 # mega bytes
    elapsed = time() - t1 # seconds 
    println("cutout speed: $(totalSize/elapsed) MB/s")
    return ret
end

function remote_getindex_worker(ba::BigArray{D,T,N,C}, jobs::RemoteChannel, results::RemoteChannel) where {D,T,N,C}
    blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = take!(jobs) 
    #println("processing block in global range: $(cartesian_range2string(globalRange))")
    data = ba.kvStore[ cartesian_range2string(chunkGlobalRange) ]
    chk = Codings.decode(data, C)
    chk = reshape(reinterpret(T, chk), ba.chunkSize)
    chk = chk[cartesian_range2unit_range(rangeInChunk)...]
    arr = OffsetArray(chk, cartesian_range2unit_range(globalRange)...) 
    put!(results, arr)
end 

function Base.getindex( ba::BigArray{D, T, N, C}, idxes::Union{UnitRange, Int}...) where {D,T,N,C}
    t1 = time()
    sz = map(length, idxes)
    ret = OffsetArray(zeros(eltype(ba), sz), idxes...)
    const jobs    = RemoteChannel(()->Channel{Tuple}(nworkers()));
    const results = RemoteChannel(()->Channel{OffsetArray}(nworkers()));
    baIter = Iterator(idxes, ba.chunkSize; offset=ba.offset)
    
    @sync begin
        @async begin
            for iter in baIter
                put!(jobs, iter)
            end
            #close(jobs)
        end
        # control the number of concurrent requests here
        for iter in baIter
            @async remote_do(remote_getindex_worker, WORKER_POOL, ba, jobs, results)
        end

        @async begin 
            for iter in baIter
                block = take!(results)
                ret[indices(block)...] = parent(block)
            end
            #close(results)
        end
    end
    close(jobs)
    close(results)
    totalSize = length(parent(ret)) * sizeof(eltype(parent(ret))) / 1024/1024 # mega bytes
    elapsed = time() - t1 # seconds 
    println("cutout speed: $(totalSize/elapsed) MB/s")
    # handle single element indexing, return the single value
    ret 
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
    baIter = Iterator(idxes, ba.chunkSize; offset=ba.offset)                          
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
    missingChunkList = Vector{CartesianRange}()
    baIter = Iterator(idxes, ba.chunkSize; offset=ba.offset)
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

function list_missing_chunks(ba::BigArray, keySet::Set{String}, idxes::Union{UnitRange, Int}...)
    t1 = time()
    sz = map(length, idxes)
    missingChunkList = Vector{CartesianRange}()
    baIter = Iterator(idxes, ba.chunkSize; offset=ba.offset)
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        if !(cartesian_range2string(chunkGlobalRange) in keySet)
            push!(missingChunkList, chunkGlobalRange)
        end 
    end
    missingChunkList
end 

end # module

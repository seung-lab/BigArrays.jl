#mutex = Threads.ReentrantLock()
#iomutex = Threads.ReentrantLock()

function setindex_taskthreads_encode_worker( buf::Array{T,N}, 
                                        ba::BigArray{D,T}, iter::Tuple) where {D,T,N}
    #println("encode worker at ", Threads.threadid())
    C = get_encoding(ba)
    (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) = iter 
    
    chk = buf[rangeInBuffer]
    mipLevelName = get_mip_level_name(ba)
    key = joinpath(mipLevelName, cartesian_range2string( chunkGlobalRange ) )
    data = encode( chk, C)
    #println("encode worker at ", Threads.threadid(), " finished work.")
    return data
end 

function upload_worker(futureData, iter::Tuple, ba::BigArray)
    (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) = iter 
    data = fetch(futureData)
    # println("save data: ", data)
    ba.kvStore[ cartesian_range2string(chunkGlobalRange) ] = data
end

"""
    put array in RAM to a BigArray backend
"""
function setindex_taskthreads!( ba::BigArray{D,T}, buf::Array{T,N},
                       idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N}
    idxes = colon2unit_range(buf, idxes)
    offset = get_offset(ba)
    chunkSize = BigArrays.get_chunk_size(ba)
    # check alignment
    @assert all(map((x,y,z)->mod(first(x) - 1 - y, z), 
                    idxes, offset.I, chunkSize).==0) 
                    "the start of index should align with BigArray chunk size"
    t1 = time()
    baIter = ChunkIterator(idxes, chunkSize; offset=offset)

    encodedBlocks = []
    for iter in baIter
        adjustedIter = adjust_iter(ba, iter)
        futureData = Threads.@spawn setindex_taskthreads_encode_worker(
            buf, ba, adjustedIter)
        push!(encodedBlocks, (futureData, adjustedIter))
    end

    @sync begin
        for (futureData, iter) in encodedBlocks
            @async upload_worker(futureData, iter, ba)
        end
    end 
    elapsed = time() - t1 # sec
    println("saving speed: $(sizeof(buf)/1024/1024/elapsed) MB/s")
end 

function getindex_taskthreads_download_worker( ba::BigArray{D, T}, 
                                            iter::Tuple ) where {D, T}
    baRange = CartesianIndices(ba)

    blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = iter
    
    # handle the volume boundary  
    chunkSize = (last(chunkGlobalRange) - first(chunkGlobalRange) 
                                    + one(last(chunkGlobalRange))).I

    if any(map((x,y)->x>y, first(globalRange).I, last(baRange).I)) || 
                any(map((x,y)->x<y, last(globalRange).I, first(baRange).I))
        #@warn("out of volume range, keep it as zeros")
        return
    end
    
    key = joinpath(get_mip_level_name(ba), cartesian_range2string(chunkGlobalRange))
    # @show ba.kvStore.bucketName, ba.kvStore.keyPrefix, key
    data = ba.kvStore[ key ]
    if data === nothing && !ba.fillMissing
        throw(KeyError("no such key: $chunkGlobalRange"))
    end
    return data
end

function getindex_taskthreads_decode_worker!(
            ba::BigArray{D, T}, buf::Array{T, N},      
            futureData, iter::Tuple) where {D, T,N}

    data = fetch(futureData)
    if data === nothing
        return nothing
    end
    # println("data: ", data)
    
    blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = iter

    chk = Codings.decode(data, get_encoding(ba))
    chunkSize = get_chunk_size(ba)

    # handle the cross boundary case
    chunkSize = (last(chunkGlobalRange) - first(chunkGlobalRange) + 
                    one(last(chunkGlobalRange))).I
    chk = reshape(reinterpret(T, chk), chunkSize)
    @inbounds buf[rangeInBuffer] = chk[rangeInChunk]
    
    return nothing
end 

function getindex_taskthreads( ba::BigArray{D, T}, 
                                idxes::Union{UnitRange, Int}...) where {D,T}
    t1 = time()
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)

    chunkSize = get_chunk_size(ba)
    baIter = ChunkIterator(idxes, chunkSize; offset=get_offset(ba))
    
    dataList = []
    @sync begin
        for iter in baIter
            adjustedIter = adjust_iter(ba, iter)
            futureData = @async getindex_taskthreads_download_worker(ba, adjustedIter) 
            push!(dataList, (futureData, adjustedIter))
        end

        futures = []
        # control the number of concurrent requests here
        for (futureData, iter) in dataList
            future = Threads.@spawn getindex_taskthreads_decode_worker!(ba, buf, futureData, iter)
            push!(futures, future)
        end
        # synchronize the futures to wait for all the decoder tasks 
        fetch(futures)
        
        # this serial version is used for debug
        #for (futureData, iter) in dataList
        #    getindex_taskthreads_decode_worker!(ba, buf, futureData, iter)
        #end
    end
        
    elapsed = time() - t1 # seconds 
    println("cutout speed: $(sizeof(buf)/1024/1024/elapsed) MB/s")
    # handle single element indexing, return the single value
    OffsetArray(buf, idxes...) 
end 
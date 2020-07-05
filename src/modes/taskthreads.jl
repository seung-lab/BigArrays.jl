mutex = Threads.ReentrantLock()
#iomutex = Threads.ReentrantLock()

function setindex_taskthreads_encode_worker( channel::Channel{Tuple}, buf::Array{T,N}, 
                                        ba::BigArray{D,T}, iter::Tuple) where {D,T,N}
    println("encode worker at ", Threads.threadid())
    C = get_encoding(ba)
    (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) = iter 
    chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = adjust_volume_boundary(ba, 
                                    chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer)
    chk = buf[rangeInBuffer]
    mipLevelName = get_mip_level_name(ba)
    key = joinpath(mipLevelName, cartesian_range2string( chunkGlobalRange ) )
    data = encode( chk, C)
    
    println("encode worker at ", Threads.threadid(), " start ingesting data to channel")
    Threads.lock(mutex)
    put!(channel, (data, iter))
    Threads.unlock(mutex)
    println("encode worker at ", Threads.threadid(), " finished work.")
end 

function upload_worker(channel::Channel{Tuple}, ba::BigArray)
    println("upload worker at ", Threads.threadid())
    #Threads.lock(mutex)
    (data, iter) = take!(channel)
    #Threads.unlock(mutex)
    println("upload worker at ", Threads.threadid(), " have get the data from channel.")
    (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) = iter 
    ba.kvStore[ cartesian_range2string(chunkGlobalRange) ] = data
    println("upload worker at ", Threads.threadid(), " finished work.")
end

"""
    put array in RAM to a BigArray backend
this version uses channel to control the number of asynchronized request
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
    channel = Channel{Tuple}( CHUNK_CHANNEL_SIZE )
    @sync begin
        @async begin
            for iter in baIter
                Threads.@spawn setindex_taskthreads_encode_worker(channel, buf, ba, iter)
            end
        end
        for iter in baIter
            @async upload_worker(channel, ba)
        end
        close(channel)
    end 
    elapsed = time() - t1 # sec
    println("saving speed: $(sizeof(buf)/1024/1024/elapsed) MB/s")
end 

function getindex_taskthreads_download_worker(channel::Channel{Tuple}, 
                                        ba::BigArray, iter::Tuple) 
    baRange = CartesianIndices(ba)
    blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = iter
    chunkSize = (last(chunkGlobalRange) - first(chunkGlobalRange) + one(CartesianIndex{N})).I
    if any(map((x,y)->x>y, first(globalRange).I, last(baRange).I)) || 
                        any(map((x,y)->x<y, last(globalRange).I, first(baRange).I))
        #@warn("out of volume range, keep it as zeros")
        return
    end
    chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = adjust_volume_boundary(ba, 
                                    chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer)
    data = ba.kvStore[ cartesian_range2string(chunkGlobalRange) ]

    Threads.lock(mutex)
    put!(channel, (data, iter))
    Threads.unlock(mutex)
end

function getindex_taskthreads_decode_worker!(dataChannel::Channel{Tuple}, ret::OffsetArray{T,N}) where {T,N}
    Threads.lock(mutex)
    data, iter = take!(dataChannel)
    Threads.unlock(mutex)

    blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = iter
    chk = Codings.decode(data, get_encoding(ba))
    chk = reshape(reinterpret(T, chk), chunkSize)
    chk = chk[rangeInChunk]
    block = OffsetArray(chk, cartesian_range2unit_range(globalRange)...) 
    ret[axes(block)...] = parent(block)
end 

function getindex_taskthreads( ba::BigArray, idxes::Union{UnitRange, Int}...)
    t1 = time()
    sz = map(length, idxes)
    ret = OffsetArray(zeros(T, sz), idxes...)

    channelSize = cld( nworkers(), 2 )
    channel = Channel{OffsetArray}( channelSize );
    
    baIter = ChunkIterator(idxes, ba.chunkSize; offset=get_offset(ba))
    
    @sync begin
        @async begin
            for iter in baIter
                @async getindex_taskthreads_download_worker(channel, ba) 
            end
            close(channel)
        end
        # control the number of concurrent requests here
        for iter in baIter
            Threads.@spawn getindex_taskthreads_decode_worker(ret, channel)
        end

        @async begin 
            for iter in baIter
                block = take!(results)
                ret[axes(block)...] = parent(block)
            end
        end
    end
    elapsed = time() - t1 # seconds 
    println("cutout speed: $(sizeof(ret)/1024/1024/elapsed) MB/s")
    # handle single element indexing, return the single value
    ret 
end 
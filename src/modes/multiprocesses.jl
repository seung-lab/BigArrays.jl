using Distributed

WORKER_POOL = default_worker_pool()
@show WORKER_POOL

function setindex_multiprocesses_worker(block::Array{T,N}, ba::BigArray{D,T}, 
                                        chunkGlobalRange::CartesianIndices{N}) where {D,T,N}
    C = get_encoding(ba)
    ba.kvStore[ cartesian_range2string(chunkGlobalRange) ] = encode( block, C)
end

"""
    put array in RAM to a BigArray
this version uses channel to control the number of asynchronized request
"""
function setindex_multiprocesses!( ba::BigArray{D,T}, buf::Array{T,N},
                       idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N}
    idxes = colon2unit_range(buf, idxes)
    # check alignment
    info = ba.info
    offset = get_offset(ba)
    chunkSize = get_chunk_size(ba)

    @assert all(map((x,y,z)->mod(first(x) - 1 - y, z), idxes, offset.I, chunkSize).==0) "the start of index should align with BigArray chunk size" 
    t1 = time() 
    baIter = ChunkIterator(idxes, chunkSize; offset=offset)
    futures = []
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = 
            adjust_volume_boundary(ba, chunkGlobalRange, globalRange, 
                                    rangeInChunk, rangeInBuffer)
        block = buf[rangeInBuffer]
        ft = remotecall_wait(setindex_multiprocesses_worker, WORKER_POOL, 
                                    block, ba, chunkGlobalRange)
        push!(futures, ft)
    end 
    fetch(futures)

    elapsed = time() - t1 # sec
    println("saving speed: $(sizeof(buf)/1024/1024/elapsed) MB/s")
end 

function getindex_multiprocesses_worker(ba::BigArray{D,T}, jobs::RemoteChannel, 
                                results::RemoteChannel) where {D,T,N}
    baRange = CartesianIndices(ba)
    blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = take!(jobs)
    if any(map((x,y)->x>y, first(globalRange).I, last(baRange).I)) || any(map((x,y)->x<y, last(globalRange).I, first(baRange).I))
        # @warn("out of volume range, keep it as zeros")
        return
    end
    chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = adjust_volume_boundary(ba, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer)
    # finalize to avoid memory leak, see
    # https://discourse.julialang.org/t/understanding-distributed-memory-garbage-collection/8726/2
    #finalize(jobs)
    chunkSize = (last(chunkGlobalRange) - first(chunkGlobalRange) + one(CartesianIndex{N})).I
    #println("processing block in global range: $(cartesian_range2string(globalRange))")
    try 
        data = ba.kvStore[ cartesian_range2string(chunkGlobalRange) ]
        chk = Codings.decode(data, get_encoding(ba))
        chk = reshape(reinterpret(T, chk), chunkSize)
        chk = chk[rangeInChunk]
        arr = OffsetArray(chk, cartesian_range2unit_range(globalRange)...) 
        put!(results, arr)
    catch err
        if isa(err, KeyError) && ba.fillMissing
            println("no such key: $(err), will fill with zeros.")
        else  
            println("catch an error while get index in remote worker: $err")
            @show typeof(err)
            @show stacktrace()
            rethrow()
        end 
    end 
end 

function getindex_multiprocesses( ba::BigArray, idxes::Union{UnitRange, Int}...)
    t1 = time()
    sz = map(length, idxes)
    ret = OffsetArray(zeros(T, sz), idxes...)

    channelSize = cld( nworkers(), 2 )
    jobs    = RemoteChannel(()->Channel{Tuple}( channelSize ));
    results = RemoteChannel(()->Channel{OffsetArray}( channelSize ));
    
    chunkSize = get_chunk_size(ba)
    baIter = ChunkIterator(idxes, chunkSize; offset=get_offset(ba))
    
    @sync begin
        @async begin
            for iter in baIter
                put!(jobs, iter)
            end
            close(jobs)
        end
        # control the number of concurrent requests here
        for iter in baIter
            @async remote_do(getindex_multiprocesses_worker, WORKER_POOL, ba, jobs, results)
        end

        @async begin 
            for iter in baIter
                block = take!(results)
                ret[axes(block)...] = parent(block)
            end
            close(results)
        end
    end
    elapsed = time() - t1 # seconds 
    println("cutout speed: $(sizeof(ret)/1024/1024/elapsed) MB/s")
    # handle single element indexing, return the single value
    ret 
end 



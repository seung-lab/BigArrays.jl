#"""
#For now, we have to define our own indexing functions for performance.
#because of this issue: 
#https://github.com/JuliaLang/julia/issues/21957
#"""
#@inline function Base.getindex(S::SharedArray, i::Real) 
#	getindex(S.s, i)
#end
#@inline function Base.setindex!(S::SharedArray, x, i::Real) 
#	setindex!(S.s, x, i)
#end
#@inline function Base.getindex(S::SharedArray, i::Real...) 
#	getindex(S.s, i...)
#end
#@inline function Base.setindex!(S::SharedArray, x, i::Real...) 
#	setindex!(S.s, x, i...)
#end

function setindex_sharedarray_worker(ba::BigArray{D,T}, 
                                sharedBuffer::OffsetArray{T,N,SharedArray{T,N}},
                                chunkGlobalRange::CartesianIndices{N},
                                rangeInBuffer::CartesianIndices{N}) where {D,T,N}
    C = get_encoding(ba)
    try
        @inbounds block = sharedBuffer[rangeInBuffer]
        @inbounds ba.kvStore[ cartesian_range2string(chunkGlobalRange) ] = encode( block, C)
    catch err 
        println("catch an error while saving in BigArray: $err with type $(typeof(err))")
        rethrow()
    end
    nothing
end

"""
    put array in RAM to a BigArray
this version uses channel to control the number of asynchronized request
"""
function setindex_sharedarray!( ba::BigArray{D,T}, buf::Array{T,N},
                       idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N}
    idxes = colon2unit_range(buf, idxes)
    sharedBuffer = OffsetArray(SharedArray(buf), idxes...)
    # check alignment
    @assert all(map((x,y,z)->mod(x.start - 1 - y, z), 
                    idxes, ba.offset.I, ba.chunkSize).==0) 
                    "the start of index should align with BigArray chunk size" 
    t1 = time() 
    baIter = ChunkIterator(idxes, ba.chunkSize; offset=ba.offset)
    @sync @distributed for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = 
            adjust_volume_boundary(ba, chunkGlobalRange, globalRange, 
                                   rangeInChunk, rangeInBuffer)
        setindex_sharedarray_worker(ba, sharedBuffer, chunkGlobalRange, rangeInBuffer)
        #@async remote_do(setindex_remote_worker, WORKER_POOL, 
        #                           ba, sharedBuffer, chunkGlobalRange, rangeInBuffer)
    end 
    elapsed = time() - t1 # sec
    println("saving speed: $(sizeof(sharedBuffer)/1024/1024/elapsed) MB/s")
end 

function getindex_sharedarray_worker!(ba::BigArray{D,T}, jobs::RemoteChannel, 
                                sharedBuffer::OffsetArray{T,N,SharedArray{T,N}}) where {D,T,N}
    baRange = CartesianIndices(ba)
    blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = take!(jobs)
    if any(map((x,y)->x>y, first(globalRange).I, last(baRange).I)) || 
            any(map((x,y)->x<y, last(globalRange).I, first(baRange).I))
        @warn("out of volume range, keep it as zeros")
        return
    end
    chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = 
            adjust_volume_boundary(ba, chunkGlobalRange, globalRange, 
                                   rangeInChunk, rangeInBuffer)
    # finalize to avoid memory leak, see
    # https://discourse.julialang.org/t/understanding-distributed-memory-garbage-collection/8726/2
    #finalize(jobs)
    chunkSize = (last(chunkGlobalRange) - first(chunkGlobalRange) + one(CartesianIndex{N})).I
    #println("processing block in global range: $(cartesian_range2string(globalRange))")
    try 
        data = ba.kvStore[ cartesian_range2string(chunkGlobalRange) ]
        chk = Codings.decode(data, get_encoding(ba))
        chk = reshape(reinterpret(T, chk), chunkSize)
        @inbounds sharedBuffer[globalRange] = chk[rangeInChunk]
    catch err 
        if isa(err, KeyError) && ba.fillMissing 
            println("no such key in file system: $(err), will fill this block as zeros")
            return 
        else 
            println("catch an error while getindex: $err with type of $(typeof(err))")
            rethrow()
        end
    end
    nothing 
end 

function getindex_sharedarray(ba::BigArray, idxes::Union{UnitRange, Int}...)
    t1 = time()
    sz = map(length, idxes)
    # it seems that the default value is automatically set to zero
    sharedBuffer = OffsetArray(SharedArray{T}(sz), idxes...)

    channelSize = cld( nworkers(), 2 )
    jobs    = RemoteChannel(()->Channel{Tuple}( channelSize ));
    
    baIter = ChunkIterator(idxes, ba.chunkSize; offset=ba.offset)
    
    @sync begin
        @async begin
            for iter in baIter
                put!(jobs, iter)
            end
            close(jobs)
        end
        # control the number of concurrent requests here
        for iter in baIter
            @async remote_do(getindex_sharedarray_worker!, WORKER_POOL, ba, jobs, sharedBuffer)
        end
    end
    ret = OffsetArray(sdata(sharedBuffer |> parent), axes(sharedBuffer))
    elapsed = time() - t1 # seconds 
    println("cutout speed: $(sizeof(ret)/1024/1024/elapsed) MB/s")
    ret 
end 


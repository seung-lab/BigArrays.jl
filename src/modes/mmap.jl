"""
    setindex_mmap!( ba::BigArray{D,T,N,C}, buf::Array{T,N},
                             idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N,C}

assign values to bigarray using shared memory map with multiple processes
"""
function setindex_mmap!( ba::BigArray{D,T,N,C}, buf::Array{T,N},
                             idxes::Union{UnitRange, Int, Colon} ... ) where {D,T,N,C}
    idxes = colon2unit_range(buf, idxes)
    baIter = ChunkIterator(idxes, ba.chunkSize; offset=ba.offset)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
        chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = 
            adjust_volume_boundary(ba, chunkGlobalRange, globalRange, 
                                   rangeInChunk, rangeInBuffer)
        chk = buf[rangeInBuffer]
        ba.kvStore[ cartesian_range2string(chunkGlobalRange) ] = encode( chk, C)
    end
end 

"""
    get_index_sequential(ba::BigArray, idxes::Union{UnitRange, Int}...) 
sequential implementation for debuging 
"""
function getindex_mmap(ba::BigArray{D, T, N, C}, 
                             idxes::Union{UnitRange, Int}...) where {D,T,N,C}
    t1 = time()
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)
    baRange = CartesianIndices(ba)
    baIter = ChunkIterator(idxes, ba.chunkSize; offset=ba.offset)
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter  
        if any(map((x,y)->x>y, first(globalRange).I, last(baRange).I)) ||
            any(map((x,y)->x<y, last(globalRange).I, first(baRange).I))
            @warn("out of volume range, keep it as zeros")
            continue
        end
        chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = 
            adjust_volume_boundary(ba, chunkGlobalRange, globalRange, 
                                   rangeInChunk, rangeInBuffer)
            chunkSize = (last(chunkGlobalRange) - first(chunkGlobalRange) + 
                         one(last(chunkGlobalRange))).I
        try 
            #println("global range of chunk: $(cartesian_range2string(chunkGlobalRange))")
            key = cartesian_range2string(chunkGlobalRange)
            v = ba.kvStore[cartesian_range2string(chunkGlobalRange)]
            v = Codings.decode(v, C)
            chk = reinterpret(T, v) |> Vector
            chk = reshape(chk, chunkSize)
            @inbounds buf[rangeInBuffer] = chk[rangeInChunk]
        catch err 
            if isa(err, KeyError) && ba.fillMissing
                println("no suck key in kvstore: $(err), will fill this block as zeros")
                break
            else
                println("catch an error while getindex in BigArray: $err with type of $(typeof(err))")
                rethrow()
            end
        end 
    end

    elapsed = time() - t1 # seconds 
    println("cutout speed: $(sizeof(buf)/1024/1024/elapsed) MB/s")
    OffsetArray(buf, idxes...)
end



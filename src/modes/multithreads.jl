
function setindex_multithreads_worker( channel::Channel{Tuple}, buf::Array{T,N}, ba::BigArray{D,T} ) where {D,T,N}
    C = get_encoding(ba)
    mipLevelName = get_mip_level_name(ba)
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in channel
        # println("global range of chunk: $(cartesian_range2string(chunkGlobalRange))")
        chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = adjust_volume_boundary(ba, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer)
        delay = 0.05
        for t in 1:4
            try
                chk = buf[rangeInBuffer]
                key = joinpath(mipLevelName, cartesian_range2string( chunkGlobalRange ) )
                ba.kvStore[ key ] = encode( chk, C)
                # for the GSDicts backend, the haskey function will really download the object!
                #@assert haskey(ba.kvStore, key)
                break
            catch err 
                println("catch an error while saving in BigArray: $err")
                @show typeof(err)
                @show stacktrace()
                if t==4
                    println("rethrow the error: $err")
                    rethrow()
                else 
                    @warn("retry for the $(t)'s time.")
                end 
                sleep(delay*(0.8+(0.4*rand())))
                delay *= 10
                println("retry for the $(t)'s time: $(string(chunkGlobalRange))")
            end
        end
    end 
end 

"""
    put array in RAM to a BigArray backend
this version uses channel to control the number of asynchronized request
"""
function setindex_multithreads!( ba::BigArray{D,T}, buf::Array{T,N},
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
    @sync begin 
        channel = Channel{Tuple}( CHUNK_CHANNEL_SIZE )
        @async begin 
            for iter in baIter
                put!(channel, iter)
            end
            close(channel)
        end
        for i in 1:TASK_NUM  
            @async setindex_multithreads_worker(channel, buf, ba)
        end
    end 
    elapsed = time() - t1 # sec
    println("saving speed: $(sizeof(buf)/1024/1024/elapsed) MB/s")
end 

function getindex_multithreads_worker!(chan::Channel{Tuple}, buf::Array{T,N}, ba::BigArray{D,T}) where {D,T,N}
    baRange = CartesianIndices(ba)
    C = get_encoding(ba)
    mipLevelName = get_mip_level_name(ba)
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in chan
        if any(map((x,y)->x>y, first(globalRange).I, last(baRange).I)) ||
            any(map((x,y)->x<y, last(globalRange).I, first(baRange).I))
            @warn("out of volume: ", first(globalRange), ", ", last(globalRange))
            continue
        end
        chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer = 
            adjust_volume_boundary(ba, chunkGlobalRange, globalRange, 
                                   rangeInChunk, rangeInBuffer)
        chunkSize = (last(chunkGlobalRange) - first(chunkGlobalRange) + 
                     one(CartesianIndex{N})).I
        try 
            #println("global range of chunk: $(cartesian_range2string(chunkGlobalRange))")
            key = cartesian_range2string(chunkGlobalRange)
            v = ba.kvStore[joinpath(mipLevelName, cartesian_range2string(chunkGlobalRange))]
            v = Codings.decode(v, C)
            chk = reinterpret(T, v)
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
end

function getindex_multithreads( ba::BigArray, idxes::Union{UnitRange, Int}...)
    t1 = time()
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)
    baIter = ChunkIterator(idxes, get_chunk_size(ba); offset=get_offset(ba))

    @sync begin
        channel = Channel{Tuple}( CHUNK_CHANNEL_SIZE )
        @async begin
            for iter in baIter
                put!(channel, iter)
            end
            close(channel)
        end
        # control the number of concurrent requests here
        for i in 1:TASK_NUM  
            @async getindex_multithreads_worker!(channel, buf, ba)
        end
    end
    elapsed = time() - t1 # seconds 
    println("cutout speed: $(sizeof(buf)/1024/1024/elapsed) MB/s")
    OffsetArray(buf, idxes...)
end



function setindex_taskthreads_worker!(buf::Array{T,N}, ba::BigArray{D,T,N}, iter::Tuple ) where {D,T,N}
    C = get_encoding(ba)
    mipLevelName = get_mip_level_name(ba)
    (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) = iter
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
            return 
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

"""
    put array in RAM to a BigArray backend
this version uses channel to control the number of asynchronized request
"""
function setindex_taskthreads!( ba::BigArray{D,T,N}, buf::Array{T,N},
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

    tasks = Threads.Task[]
    for iter in baIter
        task = Threads.@spawn setindex_taskthreads_worker!(buf, ba, iter)
        push!(tasks, task)
    end
    for task in tasks
        wait(task)
    end
    elapsed = time() - t1 # sec
    println("saving speed: $(sizeof(buf)/1024/1024/elapsed) MB/s")
end 

function getindex_taskthreads_worker!(buf::Array{T,N}, ba::BigArray{D,T,N}, iter::Tuple) where {D,T,N}
    baRange = CartesianIndices(ba)
    C = get_encoding(ba)
    mipLevelName = get_mip_level_name(ba)
    (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) = iter
    if any(map((x,y)->x>y, first(globalRange).I, last(baRange).I)) ||
        any(map((x,y)->x<y, last(globalRange).I, first(baRange).I))
        @warn("out of volume range, keep it as zeros")
        return  
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
            return
        else
            println("catch an error while getindex in BigArray: $err with type of $(typeof(err))")
            rethrow()
        end
    end 
end

function getindex_taskthreads( ba::BigArray, idxes::Union{UnitRange, Int}...)
    t1 = time()
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)
    baIter = ChunkIterator(idxes, get_chunk_size(ba); offset=get_offset(ba))
    
    tasks = Threads.Task[]
    for iter in baIter
        task = Threads.@spawn getindex_taskthreads_worker!(buf, ba, iter)
        push!(tasks, task)
    end
    for task in tasks
        wait(task)
    end

    for iter in baIter
    end
    elapsed = time() - t1 # seconds 
    println("cutout speed: $(sizeof(buf)/1024/1024/elapsed) MB/s")
    OffsetArray(buf, idxes...)
end
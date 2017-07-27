using .BigArrayIterators

function Base.ndims{D,T,N}(ba::BigArray{D,T,N})
    N
end

function Base.eltype{D, T, N}( ba::BigArray{D,T,N} )
    # @show T
    return T
end

function Base.size{D,T,N}( ba::BigArray{D,T,N} )
    # get size according to the keys
    ret = size( CartesianRange(ba) )
    return ret
end

function Base.size(ba::BigArray, i::Int)
    size(ba)[i]
end

function Base.show(ba::BigArray)
    display(ba)
end

function Base.display(ba::BigArray)
    for field in fieldnames(ba)
        println("$field: $(getfield(ba,field))")
    end
end

function Base.reshape{D,T,N}(ba::BigArray{D,T,N}, newShape)
    warn("reshape failed, the shape of bigarray is immutable!")
end

function Base.CartesianRange{D,T,N}( ba::BigArray{D,T,N} )
    warn("the size was computed according to the keys, which is a number of chunk sizes and is not accurate")
    ret = CartesianRange(
            CartesianIndex([typemax(Int) for i=1:N]...),
            CartesianIndex([0            for i=1:N]...))
    warn("boundingbox function abanduned due to the malfunction of keys in S3Dicts")
    return ret
end

"""
	put data in bigarray
if the buffer data is not aligned with the block size of bigarray, the empty regions will be filled with zeros and the regions will be overwritten. Otherwise, if we would like to keep the original data in the empty regions, the original data should be readout first which is pretty costly in the case of cloud IO. It's the user's duty to make sure that the buffer is aligned with blocks if they don't want zero filling. 

the asynchronized requests could be overwhelming to the cloud and get a lot of errors.
"""
function Base.setindex!{D,T,N,C}( ba::BigArray{D,T,N,C}, buf::Array{T,N},
                                idxes::Union{UnitRange, Int, Colon} ... )
    # @show idxes
    idxes = colon2unitRange(buf, idxes)
    baIter = BigArrayIterator(idxes, ba.chunkSize)
    chk = Array(T, ba.chunkSize)
    #@sync begin 
        for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in baIter
            #@async begin
                println("global range of chunk: $(string(chunkGlobalRange))")
                fill!(chk, convert(T, 0))
                delay = 0.05
                for t in 1:4
                    try 
                        chk[rangeInChunk] = buf[rangeInBuffer]
                        ba.kvStore[ string(chunkGlobalRange) ] = encoding( chk, C)
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
            #end 
        end
    #end 
end 

function do_work_setindex{D,T,N,C}( channel::Channel{Tuple}, buf::Array{T,N}, ba::BigArray{D,T,N,C} )
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in channel 
        println("global range of chunk: $(string(chunkGlobalRange))")
        chk = Array(T, ba.chunkSize)
        fill!(chk, convert(T, 0))
        delay = 0.05
        for t in 1:4
            try 
                chk[rangeInChunk] = buf[rangeInBuffer]
                ba.kvStore[ string(chunkGlobalRange) ] = encoding( chk, C)
                chk = nothing
                gc()
                break
            catch e
                println("catch an error while saving in BigArray: $e")
                @show typeof(e)
                @show stacktrace()
                if t==4
                    println("rethrow the error: $e")
                    gc()
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
this version uses channel to control the number of asynchronized request, but it has a memory leak issue!
"""
function setindex_v2!{D,T,N,C}( ba::BigArray{D,T,N,C}, buf::Array{T,N},
                                idxes::Union{UnitRange, Int, Colon} ... )
    idxes = colon2unitRange(buf, idxes)
    baIter = BigArrayIterator(idxes, ba.chunkSize)
    @sync begin 
        channel = Channel{Tuple}(1)
        @async begin 
            for iter in baIter
                put!(channel, iter)
            end
            close(channel)
        end
        for i in 1:4
            @schedule do_work_setindex(channel, buf, ba)
        end
    end 
end 


function do_work_getindex!{D,T,N,C}(chan::Channel{Tuple}, buf::Array, ba::BigArray{D,T,N,C})
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in chan 
        # explicit error handling to deal with EOFError
        delay = 0.05
        for t in 1:4
            try 
                println("global range of chunk: $(string(chunkGlobalRange))") 
                v = ba.kvStore[string(chunkGlobalRange)]
                @assert isa(v, Array)
                chk = decoding(v, C)
                chk = reshape(reinterpret(T, chk), ba.chunkSize)
                buf[rangeInBuffer] = chk[rangeInChunk]
                break 
            catch e
                println("catch an error while getindex in BigArray: $e")
                if isa(e, KeyError)
                    println("no suck key in kvstore: $(e), will fill this block as zeros")
                    break
                else
                    if isa(e, EOFError)
                        println("get EOFError in bigarray getindex: $e")
                    end
                    if t==4
                        rethrow()
                    end
                    sleep(delay*(0.8+(0.4*rand())))
                    delay *= 10
                end
            end 
        end
    end
end

function Base.getindex{D,T,N,C}( ba::BigArray{D, T, N, C}, idxes::Union{UnitRange, Int}...)
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)

    baIter = BigArrayIterator(idxes, ba.chunkSize, ba.offset)
    @sync begin
        chan = Channel{Tuple}(4)
        @async begin
            for iter in baIter
                put!(chan, iter)
            end
            close(chan)
        end
        # control the number of concurrent requests here
        for i in 1:4
            @schedule do_work_getindex!(chan, buf, ba)
        end
    end 
    # handle single element indexing, return the single value
    if length(buf) == 1
        return buf[1]
    else 
        # otherwise return array
        return buf
    end 
end

function get_chunk_size(ba::AbstractBigArray)
    ba.chunkSize
end

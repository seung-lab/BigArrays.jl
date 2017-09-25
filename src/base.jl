using .Indexes

using .Iterators

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

function do_work_setindex{D,T,N,C}( channel::Channel{Tuple}, buf::Array{T,N}, ba::BigArray{D,T,N,C} )
    for (blockID, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in channel 
        println("global range of chunk: $(cartesianrange2string(chunkGlobalRange))")
        chk = zeros(T, ba.chunkSize)
		# only accept aligned writting
		@assert all(x->x==1, rangeInChunk.start.I) "the writting buffer should be aligned with bigarray blocks"
        delay = 0.05
        for t in 1:4
            try
                chk = buf[cartesianrange2unitrange(rangeInBuffer)...]
                ba.kvStore[ cartesianrange2string(chunkGlobalRange) ] = encoding( chk, C)
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
this version uses channel to control the number of asynchronized request
"""
function Base.setindex!{D,T,N,C}( ba::BigArray{D,T,N,C}, buf::Array{T,N},
                                idxes::Union{UnitRange, Int, Colon} ... )
    idxes = colon2unitRange(buf, idxes)
    baIter = Iterator(idxes, ba.chunkSize)
    @sync begin 
        channel = Channel{Tuple}(1)
        @async begin 
            for iter in baIter
                put!(channel, iter)
            end
            close(channel)
        end
        for i in 1:4
            @async do_work_setindex(channel, buf, ba)
        end
    end 
end 


function do_work_getindex!{D,T,N,C}(chan::Channel{Tuple}, buf::Array{T,N}, ba::BigArray{D,T,N,C})
    for (blockId, chunkGlobalRange, globalRange, rangeInChunk, rangeInBuffer) in chan 
        # explicit error handling to deal with EOFError
        delay = 0.05
        for t in 1:4
            try 
                println("global range of chunk: $(cartesianrange2string(chunkGlobalRange))") 
                v = ba.kvStore[cartesianrange2string(chunkGlobalRange)]
                @assert isa(v, Array)
                chk = decoding(v, C)
                chk = reshape(reinterpret(T, chk), ba.chunkSize)
                buf[cartesianrange2unitrange(rangeInBuffer)...] = 
                    chk[cartesianrange2unitrange(rangeInChunk)...]
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
    baIter = Iterator(idxes, ba.chunkSize, ba.offset)
    @sync begin
        channel = Channel{Tuple}(4)
        @async begin
            for iter in baIter
                put!(channel, iter)
            end
            close(channel)
        end
        # control the number of concurrent requests here
        for i in 1:4
            @async do_work_getindex!(channel, buf, ba)
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

# Note that DenseArray only works for memory stored Array
# http://docs.julialang.org/en/release-0.4/manual/arrays/#implementation
export AbstractBigArray, BigArray, NoSuckKeyException

# include("coding.jl")
# using .Coding

abstract AbstractBigArray <: AbstractArray

# abstract AbstractBigArrayBackend    <: Any

type NoSuchKeyException <: Exception end

"""
    BigArray
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
immutable BigArray{D<:Associative, T<:Real, N, C<:AbstractBigArrayCoding} <: AbstractBigArray
    kvStore     :: D
    chunkSize   :: NTuple{N}
    offset      :: CartesianIndex{N}
    function (::Type{BigArray}){D,T,N,C}(
                            kvStore     ::D,
                            foo         ::Type{T},
                            chunkSize   ::NTuple{N},
                            coding      ::Type{C} )
        new{D, T, N, C}(kvStore, chunkSize, CartesianIndex{N}() - 1)
    end

    function (::Type{BigArray}){D,T,N,C}(
                            kvStore     ::D,
                            foo         ::Type{T},
                            chunkSize   ::NTuple{N},
                            coding      ::Type{C},
                            offset      ::CartesianIndex{N} )
        # force the offset to be 0s to shutdown the functionality of offset for now
        # because it corrupted all the other bigarrays in aws s3
        offset = CartesianIndex{N}() - 1 
        new{D, T, N, C}(kvStore, chunkSize, offset)
    end
end

function BigArray( d::Associative )
    return BigArray(d, d.configDict)
end

function BigArray( d::Associative, configDict::Dict{Symbol, Any} )
    T = eval(parse(configDict[:dataType]))
    # @show T
    chunkSize = (configDict[:chunkSize]...)
    if haskey( configDict, :coding )
        if contains( configDict[:coding], "raw" )
            coding = RawCoding
        elseif contains(  configDict[:coding], "jpeg")
            coding = JPEGCoding
        elseif contains( configDict[:coding], "blosclz")
            coding = BlosclzCoding
        elseif contains( configDict[:coding], "gzip" )
            coding = GZipCoding
        else
            error("unknown coding")
        end
    else
        coding = DEFAULT_CODING
    end

    if haskey(configDict, :offset)
      offset = CartesianIndex(configDict[:offset]...)

      if length(offset) < length(chunkSize)
        N = length(chunkSize)
        offset = CartesianIndex{N}(Base.fill_to_length((offset...), 0, Val{N}))
      end

      return BigArray( d, T, chunkSize, coding, offset )
    else
      return BigArray( d, T, chunkSize, coding )
    end
end



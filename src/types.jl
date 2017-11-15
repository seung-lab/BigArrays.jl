# Note that DenseArray only works for memory stored Array
# http://docs.julialang.org/en/release-0.4/manual/arrays/#implementation
export AbstractBigArray, BigArray 

abstract type AbstractBigArray <: AbstractArray{Any,Any} end
# map datatype of python to Julia 
const DATATYPE_MAP = Dict{String, String}( 
    "uint8"     => "UInt8", 
    "uint16"    => "UInt16", 
    "uint32"    => "UInt32", 
    "uint64"    => "UInt64", 
    "float32"   => "Float32", 
    "float64"   => "Float64" 
)  

const CODING_MAP = Dict{String,Any}(
    # note that the raw encoding in cloud storage will be automatically encoded using gzip!
    "raw"       => RawCoding,
    "jpeg"      => JPEGCoding,
    "blosclz"   => BlosclzCoding,
    "gzip"      => GZipCoding 
)


"""
    BigArray
currently, assume that the array dimension (x,y,z,...) is >= 3
all the manipulation effects in the x,y,z dimension
"""
struct BigArray{D<:Associative, T<:Real, N, C<:AbstractBigArrayCoding} <: AbstractBigArray
    kvStore     :: D
    chunkSize   :: NTuple{N}
    offset      :: CartesianIndex{N}
    function BigArray(
                   kvStore     ::D,
                   foo         ::Type{T},
                   chunkSize   ::NTuple{N},
                   coding      ::Type{C};
                   offset      ::CartesianIndex{N} = CartesianIndex{N}() - 1 ) where {D,T,N,C}
        # force the offset to be 0s to shutdown the functionality of offset for now
        # because it corrupted all the other bigarrays in aws s3
        new{D, T, N, C}(kvStore, chunkSize, offset)
    end
end

function BigArray( d::Associative )
    return BigArray(d, d.configDict)
end

function BigArray( d::Associative, configDict::Dict{Symbol, Any} )
    T = eval(parse(configDict[:dataType]))
    chunkSize = (configDict[:chunkSize]...)
    local coding #<:AbstractBigArrayCoding
    try
        coding = CODING_MAP[ configDict[:coding] ]
    catch err
        warn("unknown coding: $(configDict[:coding]), will use default coding: $(Codings.DEFAULT_CODING)")
        coding = Codings.DEFAULT_CODING
    end

    if haskey(configDict, :offset)
      offset = CartesianIndex(configDict[:offset]...)

      if length(offset) < length(chunkSize)
        N = length(chunkSize)
        offset = CartesianIndex{N}(Base.fill_to_length((offset.I...), 0, Val{N}))
      end
      return BigArray( d, T, chunkSize, coding; offset=offset )
    else
      return BigArray( d, T, chunkSize, coding )
    end
end



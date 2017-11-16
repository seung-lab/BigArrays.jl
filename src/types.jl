using JSON
#import .BackendBase: AbstractBigArrayBackend  
# Note that DenseArray only works for memory stored Array
# http://docs.julialang.org/en/release-0.4/manual/arrays/#implementation
export AbstractBigArray, BigArray 

abstract type AbstractBigArray <: AbstractArray{Any,Any} end
# map datatype of python to Julia 
const DATATYPE_MAP = Dict{String, DataType}( 
    "uint8"     => UInt8, 
    "uint16"    => UInt16, 
    "uint32"    => UInt32, 
    "uint64"    => UInt64, 
    "float32"   => Float32, 
    "float64"   => Float64 
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
struct BigArray{D<:AbstractBigArrayBackend, T<:Real, N, C<:AbstractBigArrayCoding} <: AbstractBigArray
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

function BigArray( d::AbstractBigArrayBackend) 
    info = get_info(d)
    return BigArray(d, info)
end

function BigArray( d::AbstractBigArrayBackend, info::Vector{UInt8} )
    if ismatch(r"^{", String(info) )
        info = String(info)
    else
        # gzip compressed
        info = String(Libz.decompress(info))
    end 
   BigArray(d, info)
end 

function BigArray( d::AbstractBigArrayBackend, info::AbstractString )
    BigArray(d, JSON.parse( info, dicttype=Dict{Symbol, Any} ))
end 

function BigArray( d::AbstractBigArrayBackend, infoConfig::Dict{Symbol, Any} )
    # chunkSize
    scale_name = get_scale_name(d)
    T = DATATYPE_MAP[infoConfig[:data_type]]
    local offset::Tuple, encoding, chunkSize::Tuple 
    for scale in infoConfig[:scales]
        if scale[:key] == scale_name 
            chunkSize = (scale[:chunk_sizes][1]...)
            offset = (scale[:voxel_offset]...)
            encoding = CODING_MAP[ scale[:encoding] ]
            if infoConfig[:num_channels] > 1
                chunkSize = (chunkSize..., infoConfig[:num_channels])
                offset = (offset..., 0)
            end
            break 
        end 
    end 
    BigArray(d, T, chunkSize, encoding; offset=CartesianIndex(offset)) 
end



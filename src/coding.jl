module Coding

using ImageMagick
using Blosc
using Libz

export AbstractBigArrayCoding, JPEGCoding, RawCoding, BlosclzCoding
export encoding, decoding

abstract AbstractBigArrayCoding

function __init__()
    # use the same number of threads with Julia
    if haskey(ENV, "BLOSC_NUM_THREADS")
        Blosc.set_num_threads( parse(ENV["BLOSC_NUM_THREADS"]) )
    elseif haskey(ENV, "JULIA_NUM_THREADS")
        Blosc.set_num_threads( parse(ENV["JULIA_NUM_THREADS"]) )
    else
        Blosc.set_num_threads(4)
    end
    # use the default compression method
    # Blosc.set_compressor("blosclz")
end

immutable JPEGCoding    <: AbstractBigArrayCoding end
immutable RawCoding     <: AbstractBigArrayCoding end
immutable BlosclzCoding <: AbstractBigArrayCoding end

function encoding(data::Array, coding::Type{RawCoding})
    reinterpret(UInt8, data[:])
end

function decoding(data::Vector{UInt8}, coding::Type{RawCoding})
    return data
end

function encoding( data::Array, coding::Type{JPEGCoding} )
    error("unimplemented!")
end

function decoding( data::Vector{UInt8}, coding::Type{JPEGCoding} )
    return ImageMagick.load_(data)
end

end # end of module

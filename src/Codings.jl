module Codings

#using ImageMagick
using Blosc
using Libz

abstract type AbstractBigArrayCoding end

export AbstractBigArrayCoding, JPEGCoding, RawCoding, BlosclzCoding, GZipCoding
export encode, decode 

function __init__()
    # use the same number of threads with Julia
    if haskey(ENV, "BLOSC_NUM_THREADS")
        Blosc.set_num_threads( parse(ENV["BLOSC_NUM_THREADS"]) )
    elseif haskey(ENV, "JULIA_NUM_THREADS")
        Blosc.set_num_threads( parse(ENV["JULIA_NUM_THREADS"]) )
    else
        Blosc.set_num_threads( cld(Sys.CPU_CORES, 2) )
    end
    # use the default compression method, 
    # the default compressor is blosclz.
    # Blosc.set_compressor("blosclz")
end

struct JPEGCoding    <: AbstractBigArrayCoding end
struct RawCoding     <: AbstractBigArrayCoding end
struct BlosclzCoding <: AbstractBigArrayCoding end
struct GZipCoding    <: AbstractBigArrayCoding end

const DEFAULT_CODING = RawCoding

function encode(data::Array, coding::Type{RawCoding})
    reinterpret(UInt8, data[:])
end

function decode(data::Vector{UInt8}, coding::Type{RawCoding})
    return data
end

function encode(data::Array, coding::Type{GZipCoding})
    Libz.deflate(reinterpret(UInt8, data[:]))
end

function decode(data::Vector{UInt8}, coding::Type{GZipCoding})
    Libz.inflate(data)
end

function encode( data::Array, coding::Type{BlosclzCoding} )
    Blosc.compress( data )
end
function decode( data::Vector{UInt8}, coding::Type{BlosclzCoding} )
    Blosc.decompress(UInt8, data)
end

function encode( data::Array, coding::Type{JPEGCoding} )
    error("unimplemented!")
end

function decode( data::Vector{UInt8}, coding::Type{JPEGCoding} )
    error("not working correctly with neuroglancer")
#    return ImageMagick.load_(data)
end

end # end of module

module Codings

#using ImageMagick
using Blosc
using Libz
using ImageMagick

abstract type AbstractBigArrayCoding end

export AbstractBigArrayCoding, JPEGCoding, RawCoding, BlosclzCoding, GZipCoding
export encode, decode 

const GZIP_MAGIC_NUMBER = UInt8[0x1f, 0x8b, 0x08]

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
    if all(data[1:3] .== GZIP_MAGIC_NUMBER)
        return Libz.inflate(data)
    else 
        return data 
    end
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
    image = ImageMagick.load_(data)
    @assert size(image,2) * size(image,2) == size(image,1)
    blockSize = (size(image,2), size(image,2), size(image,2))
    image = reshape(image, blockSize)
    image = permutedims(image, [3,1,2])
    image = reinterpret(UInt8, image[:])
    return image
end

end # end of module

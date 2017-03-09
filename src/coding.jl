module Coding

using ImageMagick
using Blosc


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

abstract AbstractBigArrayCoding

immutable JPEGCoding    ::AbstractBigArrayCoding end
immutable RawCoding     ::AbstractBigArrayCoding end
immutable BlosclzCoding ::AbstractBigArrayCoding end

function encoding(data::Array, coding::RawCoding)
    reinterpret(UInt8, data[:])
end

function decoding(data::Vector{UInt8}, coding::RawCoding)
    return data
end

function encoding( data::Array, coding::JPEGCoding )
    error("unimplemented!")
end

function decoding( data::Vector{UInt8}, coding::JPEGCoding )
    ImageMagick.load_(data)
end

end # end of module

# store small cuboids in AWS S3, download all the cuboids and cutout locally.
# The larger the cuboids size, the more redundency the download will be.
# The smaller the cuboids, the more impact on speed from the network latency.

"""
BigArray with backend of Google Cloud Storage
"""
module GCSBigArray

using ..BigArrays
using GoogleCloud
using GoogleCloud.Utils.Storage
using JSON

# include("../types.jl")
# include("../index.jl")
const DEFAULT_CONFIG_FILE = "config.json"
const DEFAULT_PREFIX = "cuboid_"
const DEFAULT_CUBOID_SIZE = (516, 516, 64)
const DEFAULT_GLOBAL_OFFSET = (0,0,0)

"""
definition of big array
"""
type GCSBigArray <: AbstractBigArray
    prefix          ::AbstractString
    globalOffset    ::NTuple{3, Int}
    cuboidSize      ::NTuple{3, Int}
    eltype          ::Type
    cartesianRange  ::CartesianRange
    compression     ::Symbol              # deflate || blosc
end

"""
handle vector type
"""
function GCSBigArray(   prefix      ::AbstractString,
                        globalOffset::Vector,
                        blockSize   ::Vector,
                        chunkSize   ::Vector,
                        eltype      ::Type,
                        cartesianRange  ::CartesianRange
                        compression     ::AbstractString)
    BigArray(   prefix,
                NTuple{3, Int}((globalOffset ...)),
                NTuple{3, Int}((cuboidSize ...)),
                eltype,
                cartesianRange,
                Symbol(compression))
end

"""
construct a BigArray from a dict
"""
function GCSBigArray( configDict::Dict{Symbol, Any} )
    if isa(configDict[:eltype], AbstractString)
        configDict[:eltype] = eval(parse( configDict[:eltype] ))
    end
    if isa(configDict[:cartesianRange], AbstractString)
        configDict[:cartesianRange] = eval(parse(configDict[:cartesianRange]))
    end
    BigArray(   configDict[:prefix],
                configDict[:globalOffset],
                configDict[:cuboidSize],
                configDict[:eltype],
                configDict[:cartesianRange],
                configDict[:compression] )
end

"""
construct from a register file, which defines file architecture
"""
function GCSBigArray(  dir::AbstractString;
                    prefix::AbstractString          = DEFAULT_PREFIX,
                    globalOffset::NTuple{3, Int}    = DEFAULT_GLOBAL_OFFSET,
                    cuboidSize::NTuple{3, Int}      = DEFAULT_CUBOID_SIZE,
                    eltype::Type                    = UInt8,
                    compression::Symbol             = :deflate)
    @assert ismatch(r"^gs://*", dir)
    configFile = joinpath(dir, DEFAULT_CONFIG_FILE)
    bkt, key = gspath2bkt_key(configFile)
    configString = storage(:Object, :get, bkt, key)
    if isa(configString, Dict) && haskey(config, :error)
        # the file is not exist, create one
        ba = BigArray(prefix, globalOffset, cuboidSize, eltype, compression)
        updateconfigfile(ba)
    else
        configDict = JSON.parse(configString, dicttype=Dict{Symbol, Any})
        ba = GCSBigArray( configDict )
    end
    ba
end


"""
transform bigarray to string
"""
function bigArray2dict(ba::GCSBigArray)
    d = Dict{Symbol, Any}()
    d[:prefix]          = ba.prefix
    d[:globalOffset]    = ba.globalOffset
    d[:cuboidSize]      = ba.cuboidSize
    d[:eltype]          = ba.eltype
    d[:cartesianRange]  = ba.cartesianRange
    d[:compression]     = ba.compression
    return d
end

function bigArray2string(ba::GCSBigArray)
    d = bigArray2dict(ba)
    @show d
    JSON.json(d)
end

"""
update the config.json file
"""
function updateconfigfile(ba::GCSBigArray)
    str = bigArray2string(ba)
    @show str
    configFile = joinpath(ba.dir, CONFIG_FILE)
    gsupload(configFile, str; content_type="text/html")
end

"""
element type of big array
"""
function Base.eltype(ba::BigArray)
    ba.eltype
end

"""
number of dimension
"""
function Base.ndims(ba::BigArray)
    length(ba.cuboidSize)
end

"""
bounding box of the whole volume
"""
function boundingbox(ba::GCSBigArray)
    ba.cartesianRange
end

"""
compute size from bounding box
"""
function Base.size(ba::GCSBigArray)
    bb = BoundingBox(ba)
    size(bb)
end

function Base.size(ba::GCSBigArray, i::Int)
  size(ba)[i]
end

function Base.show(ba::GCSBigArray)
  println("element type: $(eltype(ba))")
  println("the data is in Google Cloud Storage: $(ba.dir)")
end

"""
extract chunk from a bigarray
only works for 3D now.
"""
function Base.getindex(ba::GCSBigArray, idxes::Union{UnitRange, Int, Colon}...)
    @show idxes
    # clarify the Colon
    idxes = colon2unitRange(buf, idxes)
    # transform to originate from (0,0,0)
    idxes = [idxes...] .- [ba.globalOffset...]

    @show ba.globalOffset
    @show idxes
    # only support 3D image now, could support arbitrary dimensions in the future
    # allocate memory
    sz = map(length, idxes)
    buf = zeros(eltype(ba), sz)

    Threads.@threads for gidxes in collect(GlobalIndex(zip(idxes, ba.blockSize)))
        # get block id
        bids = map(blockid, zip(gidxes, ba.blockSize))
        # global coordinate
        globalOrigin = [ba.globalOffset...] .+ ([bids...]-1).* ba.blockSize .+ 1
        # get file name
        fileName = get_filename(ba, globalOrigin)
        # if have data fill with data,
        # if not, no need to change, keep as zero
            # compute index in hdf5
            blkidxes = globalIndexes2blockIndexes(gidxes, ba.blockSize)
            # compute index in buffer
            bufidxes = globalIndexes2bufferIndexes(gidxes, idxes)
            # assign data value, preserve existing value
            while true
                try
                    block = gsread(fileName)
                    if isa(block, Dict)
                        @assert haskey(block, :error)
                        warn("filled with zeros because file do not exist: $(fileName)")
                    else
                        buf[(bufidxes...)] = block[(blkidxes...)]
                    end
                    break
                catch
                    rethrow()
                    warn("open and read $fileName failed, will try 5 seconds later...")
                    sleep(5)
                end
            end
        else

        end
    end
    buf
end


"""
put small array to big array
"""
function Base.setindex!(ba::GCSBigArray, buf::Array, idxes::Union{UnitRange, Int, Colon}...)
    @assert ndims(buf) == length(idxes)
    # clarify the Colon
    idxes = colon2unitRange(buf, idxes)
    # set bounding box
    # blendchunk!(ba.boundingBox, buf, idxes)
    # transform to originate from (0,0,0)
    idxes = [idxes...] .- [ba.globalOffset...]

    Threads.@threads for gidxes in collect(GlobalIndex(zip(idxes, ba.blockSize)))
        # get block id
        bids = map(blockid, zip(gidxes, ba.blockSize))
        # global coordinate
        globalOrigin = [ba.globalOffset...] .+ ([bids...].-1) .* [ba.blockSize...] .+ 1
        # get hdf5 file name
        fileName = get_filename(ba, globalOrigin)
        @show fileName
        # compute index in hdf5
        blkidxes = globalIndexes2blockIndexes(gidxes, ba.blockSize)
        # compute index in buffer
        bufidxes = globalIndexes2bufferIndexes(gidxes, idxes)
        # put buffer subarray to hdf5, reserve existing values
        while true
            try
                save_buffer(buf, fileName, blkidxes, bufidxes; compression = ba.compression)
                info("save $(gidxes) from buffer $(bufidxes) to $(blkidxes) of $(fileName)")
                break
            catch
                rethrow()
                warn("open and write $fileName failed, will try 5 seconds later...")
                sleep(5)
            end
        end
    end
end

"""
save part of or whole buffer to one hdf5 file
only support **aligned** saving now!
"""
function save_buffer{T,D}(  buf::Array{T, D}, fileName::AbstractString,
                            blkidxes::Union{Tuple, Vector},
                            bufidxes::Union{Tuple, Vector};
                            compression::Symbol = :none)
    @assert D==length(blkidxes)
    @assert D==length(bufidxes)

    if ba.compression == :deflate
        error("not support yet")
    elseif ba.compression == :blosc
        error("not support yet")
    else
        gssave(fileName, buf[(bufidxes...)])
    end
end

end # end of module: Cuboids

# store small cuboids in AWS S3, download all the cuboids and cutout locally.
# The larger the cuboids size, the more redundency the download will be.
# The smaller the cuboids, the more impact on speed from the network latency.
module GCSBigArrays

using ..BigArrays
# using ..BigArrays.BoundingBox
using GoogleCloud
using GoogleCloud.Utils.Storage
using JSON

# include("../types.jl")
# include("../index.jl")
const DEFAULT_CONFIG_FILE   = "config.json"
const DEFAULT_PREFIX        = "cuboid_"
const DEFAULT_CUBOID_SIZE   = (516, 516, 64)
const DEFAULT_GLOBAL_OFFSET = (0,0,0)
const DEFAULT_ELTYPE        = UInt8
const DEFAULT_RANGE         = CartesianRange(
        CartesianIndex((typemax(Int), typemax(Int), typemax(Int))),
        CartesianIndex((0,0,0)))
const DEFAULT_COMPRESSION   = :none

export GCSBigArray

"""
definition of big array
"""
type GCSBigArray <: AbstractBigArray
    dir             ::AbstractString
    prefix          ::AbstractString
    globalOffset    ::Tuple
    cuboidSize      ::Tuple
    eltype          ::Type
    cartesianRange  ::CartesianRange
    compression     ::Symbol              # deflate || blosc
end

"""
construct a BigArray from a dict
"""
function GCSBigArray( configDict::Dict{Symbol, Any} )
    if isa(configDict[:eltype], AbstractString)
        configDict[:eltype] = eval(parse( configDict[:eltype] ))
    end
    @show configDict[:eltype]
    if isa(configDict[:cartesianRange], AbstractString)
        configDict[:cartesianRange] = CartesianRange( eval(
                                        parse(configDict[:cartesianRange])))
    end
    if isa(configDict[:globalOffset], Vector)
        configDict[:globalOffset] = (configDict[:globalOffset]...)
    end
    if isa(configDict[:cuboidSize], Vector)
        configDict[:cuboidSize] = (configDict[:cuboidSize]...)
    end
    configDict[:compression] = Symbol(configDict[:compression])

    GCSBigArray(configDict[:dir],
                configDict[:prefix],
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
                    globalOffset::Tuple             = DEFAULT_GLOBAL_OFFSET,
                    cuboidSize::Tuple               = DEFAULT_CUBOID_SIZE,
                    eltype::Type                    = DEFAULT_ELTYPE,
                    cartesianRange::CartesianRange  = DEFAULT_RANGE,
                    compression::Symbol             = DEFAULT_COMPRESSION)
    @assert ismatch(r"^gs://*", dir)
    configFile = joinpath(dir, DEFAULT_CONFIG_FILE)
    bkt, key = gspath2bkt_key(configFile)
    configString = storage(:Object, :get, bkt, key)
    if isa(configString, Dict) && haskey(configString, :error)
        # the file is not exist, create one
        ba = GCSBigArray(  dir, prefix, globalOffset, cuboidSize,
                        eltype, cartesianRange, compression)
        updateconfigfile(ba)
    else
        configDict = JSON.parse(configString, dicttype=Dict{Symbol, Any})
        @show configDict
        ba = GCSBigArray( configDict )
    end
    ba
end

"""
transform bigarray to string
"""
function bigArray2dict(ba::GCSBigArray)
    d = Dict{Symbol, Any}()
    d[:dir]             = ba.dir
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
    d[:cartesianRange] = string(d[:cartesianRange])
    @show d
    JSON.json(d)
end

"""
update the config.json file
"""
function updateconfigfile(ba::GCSBigArray)
    str = bigArray2string(ba)
    @show str
    configFile = joinpath(ba.dir, DEFAULT_CONFIG_FILE)
    gssave(configFile, str; content_type="text/html")
end

"""
element type of big array
"""
function Base.eltype(ba::GCSBigArray)
    return ba.eltype
end

"""
number of dimension
"""
function Base.ndims(ba::GCSBigArray)
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
    size(ba.cartesianRange)
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
    idxes = colon2unitRange(ba, idxes)
    # transform to originate from (0,0,0)
    idxes = [idxes...] .- [ba.globalOffset...]

    @show ba.globalOffset
    @show idxes
    # only support 3D image now, could support arbitrary dimensions in the future
    # allocate memory
    sz = map(length, idxes)
    buf = zeros(ba.eltype, (sz...))

    for gidxes in collect(GlobalIndex(zip(idxes, ba.cuboidSize)))
        # get block id
        bids = map(blockid, zip(gidxes, ba.cuboidSize))
        # global coordinate
        globalOrigin = [ba.globalOffset...] .+ ([bids...]-1).* ba.cuboidSize .+ 1
        # get file name
        fileName = get_filename(ba, globalOrigin)
        # if have data fill with data,
        # if not, no need to change, keep as zero
        # compute index in hdf5
        blkidxes = globalIndexes2blockIndexes(gidxes, ba.cuboidSize)
        # compute index in buffer
        bufidxes = globalIndexes2bufferIndexes(gidxes, idxes)
        # assign data value, preserve existing value
        block = gsread(fileName)

        if isa(block, Dict)
            @assert haskey(block, :error)
            warn("filled with zeros because file do not exist: $(fileName)")
        else
            block = reshape(    reinterprete(ba.eltype,
                                            Vector{UInt8}(block)),
                                ba.cuboidSize)
            buf[(bufidxes...)] = block[(blkidxes...)]
        end
    end
    buf
end

"""
get h5 file name
"""
function get_filename(ba::GCSBigArray, globalOrigin::Union{Tuple, Vector})
    fileName = ba.prefix
    for i in 1:length(globalOrigin)
        fileName *= "$(globalOrigin[i])-$(globalOrigin[i]+ba.cuboidSize[i]-1)_"
    end
    return joinpath(ba.dir, "$(fileName[1:end-1]).h5")
end

"""
put small array to big array
"""
function Base.setindex!(ba::GCSBigArray, buf::Array, idxes::Union{UnitRange, Int, Colon}...)
    @assert ndims(buf) == length(idxes)
    # clarify the Colon
    idxes = colon2unitRange(buf, idxes)
    # set bounding box
    adjust_range!(ba, idxes)
    # transform to originate from (0,0,0)
    idxes = map((x,y)-> x-y, idxes, ba.globalOffset)

    @show idxes
    @show ba.cuboidSize
    for gidxes in map((x,y) -> GlobalIndex(x,y), idxes, ba.cuboidSize)
        @show gidxes
        # get block id
        bids = map(blockid, zip(gidxes, ba.cuboidSize))
        # global coordinate
        globalOrigin = [ba.globalOffset...] .+ ([bids...].-1) .* [ba.cuboidSize...] .+ 1
        # get hdf5 file name
        fileName = get_filename(ba, globalOrigin)
        @show fileName
        # compute index in hdf5
        blkidxes = globalIndexes2blockIndexes(gidxes, ba.cuboidSize)
        # compute index in buffer
        bufidxes = globalIndexes2bufferIndexes(gidxes, idxes)
        # put buffer subarray to hdf5, reserve existing values
        save_buffer(buf, fileName, blkidxes, bufidxes; compression = ba.compression)
        info("save $(gidxes) from buffer $(bufidxes) to $(blkidxes) of $(fileName)")
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

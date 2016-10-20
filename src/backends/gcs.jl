# store small blocks in AWS S3, download all the blocks and cutout locally.
# The larger the blocks size, the more redundency the download will be.
# The smaller the blocks, the more impact on speed from the network latency.
module GCSBigArrays

using ..BigArrays
# using ..BigArrays.BoundingBox
using ..BigArrays.BigArrayIterators

using GoogleCloud
using GoogleCloud.Utils.Storage
using JSON

# include("../types.jl")
# include("../index.jl")
const DEFAULT_CONFIG_FILE   = "config.json"
const DEFAULT_PREFIX        = "block_"
const DEFAULT_CUBOID_SIZE   = (516, 516, 64)
const DEFAULT_GLOBAL_OFFSET = (0,0,0)
const DEFAULT_ELTYPE        = UInt8
const DEFAULT_RANGE         = CartesianRange(
        CartesianIndex((typemax(Int), typemax(Int), typemax(Int))),
        CartesianIndex((0,0,0)))
const DEFAULT_COMPRESSION   = :blosc
const DEFAULT_EXT           = "blk"

export GCSBigArray

"""
definition of big array
"""
type GCSBigArray <: AbstractBigArray
    dir             ::AbstractString
    prefix          ::AbstractString
    globalOffset    ::Tuple
    blockSize       ::Tuple
    eltype          ::Type
    globalRange     ::CartesianRange
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
    if isa(configDict[:globalRange], AbstractString)
        configDict[:globalRange] = CartesianRange( eval(
                                        parse(configDict[:globalRange])))
    end
    if isa(configDict[:globalOffset], Vector)
        configDict[:globalOffset] = (configDict[:globalOffset]...)
    end
    if isa(configDict[:blockSize], Vector)
        configDict[:blockSize] = (configDict[:blockSize]...)
    end
    configDict[:compression] = Symbol(configDict[:compression])

    GCSBigArray(configDict[:dir],
                configDict[:prefix],
                configDict[:globalOffset],
                configDict[:blockSize],
                configDict[:eltype],
                configDict[:globalRange],
                configDict[:compression] )
end

"""
construct from a register file, which defines file architecture
"""
function GCSBigArray(  dir::AbstractString;
                    prefix::AbstractString          = DEFAULT_PREFIX,
                    globalOffset::Tuple             = DEFAULT_GLOBAL_OFFSET,
                    blockSize::Tuple               = DEFAULT_CUBOID_SIZE,
                    eltype::Type                    = DEFAULT_ELTYPE,
                    globalRange::CartesianRange  = DEFAULT_RANGE,
                    compression::Symbol             = DEFAULT_COMPRESSION)
    @assert ismatch(r"^gs://*", dir)
    configFile = joinpath(dir, DEFAULT_CONFIG_FILE)
    bkt, key = gspath2bkt_key(configFile)
    configString = storage(:Object, :get, bkt, key)
    if isa(configString, Dict) && haskey(configString, :error)
        # the file is not exist, create one
        ba = GCSBigArray(  dir, prefix, globalOffset, blockSize,
                        eltype, globalRange, compression)
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
    d[:blockSize]      = ba.blockSize
    d[:eltype]          = ba.eltype
    d[:globalRange]  = ba.globalRange
    d[:compression]     = ba.compression
    return d
end

function bigArray2string(ba::GCSBigArray)
    d = bigArray2dict(ba)
    d[:globalRange] = string(d[:globalRange])
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
    length(ba.blockSize)
end

"""
bounding box of the whole volume
"""
function boundingbox(ba::GCSBigArray)
    ba.globalRange
end

"""
compute size from bounding box
"""
function Base.size(ba::GCSBigArray)
    size(ba.globalRange)
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
    idxes = map((x,y)->x-y, idxes, ba.globalOffset)

    # only support 3D image now, could support arbitrary dimensions in the future
    # allocate memory
    sz = map(length, idxes)
    buf = zeros(ba.eltype, (sz...))

    # transform to originate from (0,0,0)
    idxes = map((x,y)-> x-y, idxes, ba.globalOffset)
    bufferGlobalRange = CartesianRange(idxes)

    baIter = BigArrayIterator(ba.globalRange, ba.blockSize)

    # temporal block as a buffer to reduce memory allocation
    tempBlock = Array(ba.eltype, ba.blockSize)
    for (blockID, globalRange, blockRange, bufferRange) in baIter
        blockFileName = get_block_file_name(ba, blockID)
        info("read $(globalRange) from $(blockRange) of $(blockFileName) to buffer $(bufferRange) ...")
        tempBlock = gsread(blockFileName; eltype=ba.eltype,
                            shape = ba.blockSize,
                            compression = ba.compression)
        # map((x,y)->buf[x]=tempBlock[y], BufferRange, blockRange)
        buf[bufferRange] = tempBlock[blockRange]
    end
    buf
end

"""
get block file name from a
"""
function get_block_file_name{N}( ba::GCSBigArray, blockID::NTuple{N})
    blockGlobalRange = blockid2global_range( blockID, ba.blockSize )

    fileName = ba.prefix
    for i in 1:N
        fileName *= "$(blockGlobalRange.start[i])-$(blockGlobalRange.stop[i])_"
    end
    return joinpath(ba.dir, "$(fileName[1:end-1]).$(DEFAULT_EXT)")
end
function get_block_file_name(ba::GCSBigArray, idx::CartesianIndex)
    blockID = index2blockid( idx, ba.blockSize )
    get_block_file_name(ba, blockID)
end

"""
put small array to big array
"""
function Base.setindex!{T,N}(ba::GCSBigArray, buf::Array{T,N}, idxes::Union{UnitRange, Int, Colon}...)
    @assert ndims(buf) == length(idxes)
    # clarify the Colon
    idxes = colon2unitRange(buf, idxes)
    # set bounding box
    adjust_range!(ba, idxes)
    updateconfigfile(ba)
    # transform to originate from (0,0,0)
    idxes = map((x,y)-> x-y, idxes, ba.globalOffset)
    bufferGlobalRange = CartesianRange(idxes)

    @show idxes
    @show bufferGlobalRange
    baIter = BigArrayIterator(ba.globalRange, ba.blockSize)

    # temporal block as a buffer to reduce memory allocation
    tempBlock = Array(T, ba.blockSize)
    for (blockID, globalRange, blockRange, bufferRange) in baIter
        # refresh the temporal block
        fill!(tempBlock, ba.eltype(0))
        # map((x,y)->tempBlock[x]=buf[y], blockRange, bufferRange)
        @show bufferRange
        tempBlock[blockRange] = buf[bufferRange]
        blockFileName = get_block_file_name(ba, blockID)
        info("save $(globalRange) from buffer $(bufferRange) to $(blockRange) of $(blockFileName) ...")
        gssave(blockFileName, tempBlock)
    end
end

end # end of module: Cuboids
